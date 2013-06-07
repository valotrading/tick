#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/read-write.h"
#include "libtrading/buffer.h"

#include "bats-pitch112.h"
#include "nasdaq-itch41.h"
#include "builtins.h"
#include "format.h"
#include "stream.h"
#include "error.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <libgen.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <zlib.h>

#include <getopt.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

static uint64_t stats[256];

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

static void print_stat(u8 msg_type, const char *name)
{
	fprintf(stdout, "%'14.0f  %s\n", (double) stats[msg_type], name);
}

static void print_progress(struct buffer *buf)
{
	fprintf(stderr, "Processing messages: %3u%%\r", (unsigned int)(buf->start * 100 / buf->capacity));

	fflush(stderr);
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

/*
 * BATS PITCH 1.12
 */

static void bats_pitch112_print_stats(void)
{
	printf(" Message type stats for '%s':\n\n", filename);

	print_stat(PITCH_MSG_SYMBOL_CLEAR,	"Symbol Clear");
	print_stat(PITCH_MSG_ADD_ORDER_SHORT,	"Add Order (short)");
	print_stat(PITCH_MSG_ADD_ORDER_LONG,	"Add Order (long)");
	print_stat(PITCH_MSG_ORDER_EXECUTED,	"Order Executed");
	print_stat(PITCH_MSG_ORDER_CANCEL,	"Order Cancel");
	print_stat(PITCH_MSG_TRADE_SHORT,	"Trade (short)");
	print_stat(PITCH_MSG_TRADE_LONG,	"Trade (long)");
	print_stat(PITCH_MSG_TRADE_BREAK,	"Trade Break");
	print_stat(PITCH_MSG_TRADING_STATUS,	"Trading Status");
	print_stat(PITCH_MSG_AUCTION_UPDATE,	"Auction Update");
	print_stat(PITCH_MSG_AUCTION_SUMMARY,	"Auction Summary");

	printf("\n");
}

static void bats_pitch112_stat(int fd, z_stream *zstream)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s", filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s", filename, strerror(errno));

	zstream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s", strerror(errno));

	stream = (struct stream) {
		.zstream	= zstream,
		.uncomp_buf	= uncomp_buf,
		.comp_buf	= comp_buf,
		.progress	= print_progress,
	};

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch112_read(&stream, &msg);
		if (err)
			error("%s: %s", filename, strerror(err));

		if (!msg)
			break;

		stats[msg->MessageType]++;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}


/*
 * NASDAQ TotalView-ITCH 4.1
 */

static void nasdaq_itch41_print_stats(void)
{
	printf(" Message type stats for '%s':\n\n", filename);

	print_stat(ITCH41_MSG_TIMESTAMP_SECONDS,	"Timestamp - Seconds");
	print_stat(ITCH41_MSG_SYSTEM_EVENT,		"System Event");
	print_stat(ITCH41_MSG_STOCK_DIRECTORY,		"Stock Directory");
	print_stat(ITCH41_MSG_STOCK_TRADING_ACTION,	"Stock Trading Action");
	print_stat(ITCH41_MSG_REG_SHO_RESTRICTION,	"REG SHO Restriction");
	print_stat(ITCH41_MSG_MARKET_PARTICIPANT_POS,	"Market Participant Position");
	print_stat(ITCH41_MSG_ADD_ORDER,		"Add Order");
	print_stat(ITCH41_MSG_ADD_ORDER_MPID,		"Add Order - MPID Attribution");
	print_stat(ITCH41_MSG_ORDER_EXECUTED,		"Order Executed");
	print_stat(ITCH41_MSG_ORDER_EXECUTED_WITH_PRICE,"Order Executed With Price");
	print_stat(ITCH41_MSG_ORDER_CANCEL,		"Order Cancel");
	print_stat(ITCH41_MSG_ORDER_DELETE,		"Order Delete");
	print_stat(ITCH41_MSG_ORDER_REPLACE,		"Order Replace");
	print_stat(ITCH41_MSG_TRADE,			"Trade (non-cross)");
	print_stat(ITCH41_MSG_CROSS_TRADE,		"Cross Trade");
	print_stat(ITCH41_MSG_BROKEN_TRADE,		"Broken Trade");
	print_stat(ITCH41_MSG_NOII,			"NOII");
	print_stat(ITCH41_MSG_RPII,			"RPII");

	printf("\n");
}

static void nasdaq_itch41_stat(int fd, z_stream *zstream)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s", filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s", filename, strerror(errno));

	zstream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s", strerror(errno));

	stream = (struct stream) {
		.zstream	= zstream,
		.uncomp_buf	= uncomp_buf,
		.comp_buf	= comp_buf,
		.progress	= print_progress,
	};


	for (;;) {
		struct itch41_message *msg;
		int err;

		err = nasdaq_itch41_read(&stream, &msg);
		if (err)
			error("%s: %s", filename, strerror(err));

		if (!msg)
			break;

		stats[msg->MessageType]++;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
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
		nasdaq_itch41_stat(fd, &stream);
		nasdaq_itch41_print_stats();
	} else if (!strcmp(format, FORMAT_BATS_PITCH_112)) {
		bats_pitch112_stat(fd, &stream);
		bats_pitch112_print_stats();
	} else
		error("%s is not a supported file format", format);

	printf("\n");

	if (close(fd) < 0)
		error("%s: %s: %s", filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
