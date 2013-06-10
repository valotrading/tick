#include "tick/builtins.h"

#include "tick/nasdaq/stat.h"
#include "tick/bats/stat.h"
#include "tick/format.h"
#include "tick/error.h"
#include "tick/stats.h"

#include <getopt.h>
#include <libgen.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <zlib.h>

extern const char *program;

static void usage(void)
{
#define FMT								\
"\n usage: %s stat [<options>] <filename>\n"				\
"\n"									\
"    -f, --format <format> input file format\n"				\
"\n Supported file formats are:\n"					\
"\n"									\
"   %s\n"								\
"   %s\n"								\
"\n"
	fprintf(stderr, FMT,
			program,
			FORMAT_BATS_PITCH_112,
			FORMAT_NASDAQ_ITCH_41);

#undef FMT

	exit(EXIT_FAILURE);
}

static const struct option options[] = {
	{ "format",	required_argument, 	NULL, 'f' },
	{ NULL,		0,			NULL,  0  },
};

static const char	*filename;
static const char	*format;

static void parse_args(int argc, char *argv[])
{
	int opt;

	while ((opt = getopt_long(argc, argv, "f:v", options, NULL)) != -1) {
		switch (opt) {
		case 'f':
			format		= optarg;
			break;
		default:
			usage();
			break;
		}
	}

	argc -= optind;
	argv += optind;

	if (argc != 1)
		usage();

	filename = argv[0];
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

int cmd_stat(int argc, char *argv[])
{
	z_stream stream;
	int fd;

	setlocale(LC_ALL, "");

	parse_args(argc - 1, argv + 1);

	if (!format)
		error("%s: unsupported file format", filename);

	init_stream(&stream);

	fd = open(filename, O_RDONLY);
	if (fd < 0)
		error("%s: %s", filename, strerror(errno));

	if (!strcmp(format, FORMAT_NASDAQ_ITCH_41)) {
		struct stats stats;

		stats = (struct stats) {
			.filename	= filename,
		};

		nasdaq_itch41_stat(&stats, fd, &stream);
		nasdaq_itch41_print_stats(&stats);
	} else if (!strcmp(format, FORMAT_BATS_PITCH_112)) {
		struct stats stats;

		stats = (struct stats) {
			.filename	= filename,
		};

		bats_pitch112_stat(&stats, fd, &stream);
		bats_pitch112_print_stats(&stats);
	} else
		error("%s is not a supported file format", format);

	printf("\n");

	if (close(fd) < 0)
		error("%s: %s: %s", filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
