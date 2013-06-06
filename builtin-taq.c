#include "builtins.h"

#include <getopt.h>
#include <locale.h>
#include <stdlib.h>
#include <stdio.h>

#define FORMAT_OB	"ob"

extern const char *program;

static void usage(void)
{
#define FMT								\
"\n usage: %s taq [<options>] <input> <output>\n"			\
"\n"									\
"    -f, --format <format> input file format\n"				\
"\n Supported file formats are:\n"					\
"\n"									\
"   %s\n"								\
"\n"
	fprintf(stderr, FMT,
			program,
			FORMAT_OB);

#undef FMT

	exit(EXIT_FAILURE);
}

static const struct option options[] = {
	{ "format",	required_argument, 	NULL, 'f' },
};

static const char	*output_filename;
static const char	*input_filename;
static const char	*format;

static void parse_args(int argc, char *argv[])
{
	int opt;

	while ((opt = getopt_long(argc, argv, "f:", options, NULL)) != -1) {
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

	if (argc < 2)
		usage();

	input_filename	= argv[0];
	output_filename = argv[1];
}

int cmd_taq(int argc, char *argv[])
{
	setlocale(LC_ALL, "");

	parse_args(argc - 1, argv + 1);

	return 0;
}
