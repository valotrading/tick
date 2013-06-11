#include "tick/builtins.h"

#include "tick/nyse/taq-proto.h"
#include "tick/format.h"
#include "tick/stream.h"
#include "tick/error.h"
#include "tick/taq.h"

#include <getopt.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

extern const char *program;

static void usage(void)
{
#define FMT								\
"\n usage: %s taq [<options>] <input> <output>\n"			\
"\n"									\
"    -s, --symbol <symbol> symbol\n"					\
"    -f, --format <format> input file format\n"				\
"    -d, --date <date>     date\n"					\
"\n Supported file formats are:\n"					\
"\n"									\
"   %s\n"								\
"\n"
	fprintf(stderr, FMT,
			program,
			format_names[FORMAT_NYSE_TAQ_17]);

#undef FMT

	exit(EXIT_FAILURE);
}

static const struct option options[] = {
	{ "date",	required_argument,	NULL, 'd' },
	{ "format",	required_argument, 	NULL, 'f' },
	{ "symbol",	required_argument,	NULL, 's' },
	{ NULL,		0,			NULL,  0  },
};

static const char	*output_filename;
static const char	*input_filename;
static const char	*date;
static const char	*format;
static const char	*symbol;

static void parse_args(int argc, char *argv[])
{
	int opt;

	while ((opt = getopt_long(argc, argv, "f:s:d:", options, NULL)) != -1) {
		switch (opt) {
		case 's':
			symbol		= optarg;
			break;
		case 'f':
			format		= optarg;
			break;
		case 'd':
			date		= optarg;
			break;
		default:
			usage();
			break;
		}
	}

	argc -= optind;
	argv += optind;

	if (argc < 2)
		usage();

	input_filename = argv[0];
	output_filename = argv[1];
}

static void init_stream(z_stream *stream)
{
	memset(stream, 0, sizeof(*stream));

	if (inflateInit2(stream, 15 + 32) != Z_OK)
		error("unable to initialize zlib");
}

int cmd_taq(int argc, char *argv[])
{
	int in_fd, out_fd;
	enum format fmt;
	z_stream stream;

	setlocale(LC_ALL, "");

	parse_args(argc - 1, argv + 1);

	if (!format)
		error("%s: unsupported file format", input_filename);

	if (!symbol)
		error("symbol not specified");

	init_stream(&stream);

	in_fd = open(input_filename, O_RDONLY);
	if (in_fd < 0)
		error("%s: %s", input_filename, strerror(errno));

	out_fd = open(output_filename, O_RDWR|O_CREAT|O_EXCL, 0644);
	if (out_fd < 0)
		error("%s: %s", output_filename, strerror(errno));

	taq_write_header(out_fd);

	fmt = parse_format(format);

	switch (fmt) {
	case FORMAT_NYSE_TAQ_17: {
		struct nyse_taq_session	session;

		session = (struct nyse_taq_session) {
			.zstream	= &stream,
			.in_fd		= in_fd,
			.out_fd		= out_fd,
			.input_filename	= input_filename,
			.date           = date,
			.time_zone	= "America/New_York",
			.time_zone_len	= strlen("America/New_York"),
		};

		nyse_taq_filter_init(&session.filter, symbol);

		nyse_taq_taq(&session);
		break;
	}
	case FORMAT_BATS_PITCH_112:
	case FORMAT_NASDAQ_ITCH_41:
	default:
		error("%s is not a supported file format", format);

		break;
	}

	printf("\n");

	if (close(in_fd) < 0)
		error("%s: %s", input_filename, strerror(errno));

	if (close(out_fd) < 0)
		error("%s: %s", output_filename, strerror(errno));

	return 0;
}
