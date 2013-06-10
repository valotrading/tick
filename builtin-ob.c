#include "tick/builtins.h"

#include "tick/bats/pitch-proto.h"
#include "tick/format.h"
#include "tick/error.h"
#include "tick/ob.h"

#include <sys/types.h>
#include <sys/stat.h>
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
"\n usage: %s ob [<options>] <input> <output>\n"			\
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
			FORMAT_BATS_PITCH_112);

#undef FMT

	exit(EXIT_FAILURE);
}

static const struct option options[] = {
	{ "date",	required_argument,	NULL, 'd' },
	{ "format",	required_argument, 	NULL, 'f' },
	{ "symbol",	required_argument, 	NULL, 's' },
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

	while ((opt = getopt_long(argc, argv, "s:f:d:", options, NULL)) != -1) {
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

	input_filename	= argv[0];
	output_filename = argv[1];
}

static void init_stream(z_stream *stream)
{
	memset(stream, 0, sizeof(*stream));

	if (inflateInit2(stream, 15 + 32) != Z_OK)
		error("unable to initialize zlib");
}

static void release_stream(z_stream *stream)
{
	inflateEnd(stream);
}


int cmd_ob(int argc, char *argv[])
{
	int in_fd, out_fd;
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

	ob_write_header(out_fd);

	if (!strcmp(format, FORMAT_BATS_PITCH_112)) {
		struct pitch_session session;
		struct ob_event event;
		char date_buf[11];

		if (!date) {
			if (pitch_file_parse_date(input_filename, date_buf, sizeof(date_buf)) < 0)
				error("%s: unable to parse date from filename", input_filename);

			date = date_buf;
		}

		session = (struct pitch_session) {
			.zstream	= &stream,
			.in_fd		= in_fd,
			.out_fd		= out_fd,
			.input_filename	= input_filename,
			.time_zone	= "America/New_York",
			.time_zone_len	= strlen("America/New_York"),
			.exchange	= "BATS",
			.exchange_len	= strlen("BATS"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
		};

		pitch_filter_init(&session.filter, symbol);

		event = (struct ob_event) {
			.type		= OB_EVENT_DATE,
			.date		= date,
			.date_len	= strlen(date),
			.time_zone	= session.time_zone,
			.time_zone_len	= session.time_zone_len,
			.exchange	= session.exchange,
			.exchange_len	= session.exchange_len,
		};

		ob_write_event(session.out_fd, &event);

		bats_pitch112_process(&session);
	} else
		error("%s is not a supported file format", format);

	printf("\n");

	if (close(in_fd) < 0)
		error("%s: %s", input_filename, strerror(errno));

	if (close(out_fd) < 0)
		error("%s: %s", output_filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
