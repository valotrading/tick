#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/read-write.h"
#include "libtrading/buffer.h"

#include "builtins.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <libgen.h>
#include <locale.h>
#include <stdarg.h>
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

#define FORMAT_BATS_PITCH_112	"bats-pitch-1.12"
#define FORMAT_NASDAQ_ITCH_41	"nasdaq-itch-4.1"

extern const char *program;

static void error(const char *fmt, ...)
{
	va_list ap;

	fprintf(stderr, "%s: error: ", program);

	va_start(ap, fmt);

	vfprintf(stderr, fmt, ap);

	va_end(ap);

	fprintf(stderr, "\n");

	exit(EXIT_FAILURE);
}

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
		error("%s: unable to initialize zlib\n", program);
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

static void bats_pitch112_process(int fd, z_stream *stream)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	stream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s: %s\n", program, strerror(errno));

	for (;;) {
		struct pitch_message *msg;
		unsigned char ch;

retry_size:
		if (buffer_size(uncomp_buf) < sizeof(u8) + sizeof(struct pitch_message)) {
			ssize_t nr;

			buffer_compact(uncomp_buf);

			nr = buffer_inflate(comp_buf, uncomp_buf, stream);
			if (nr < 0)
				error("%s: zlib error\n", program);

			if (!nr)
				break;

			print_progress(comp_buf);

			goto retry_size;
		}

		ch = buffer_peek_8(uncomp_buf);
		if (ch != 0x53)
			error("%s: %s: expected sequence message marker 0x53, got 0x%x ('%c')",
					program, filename, ch, ch);

		buffer_advance(uncomp_buf, sizeof(u8));

retry_message:
		msg = pitch_message_decode(uncomp_buf);
		if (!msg) {
			ssize_t nr;

			buffer_compact(uncomp_buf);

			nr = buffer_inflate(comp_buf, uncomp_buf, stream);
			if (nr < 0)
				error("%s: zlib error\n", program);

			if (!nr)
				break;

			print_progress(comp_buf);

			goto retry_message;
		}

retry_LF:
		if (buffer_size(uncomp_buf) < sizeof(u8)) {
			ssize_t nr;

			buffer_compact(uncomp_buf);

			nr = buffer_inflate(comp_buf, uncomp_buf, stream);
			if (nr < 0)
				error("%s: zlib error\n", program);

			if (!nr)
				break;

			print_progress(comp_buf);

			goto retry_LF;
		}

		ch = buffer_peek_8(uncomp_buf);
		if (ch != 0x0A)
			error("%s: %s: expected LF marker 0x0A, got 0x%x ('%c')",
					program, filename, ch, ch);

		buffer_advance(uncomp_buf, sizeof(u8));

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

static void nasdaq_itch41_process(int fd, z_stream *stream)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	stream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s: %s\n", program, strerror(errno));

	for (;;) {
		struct itch41_message *msg;

retry_size:
		if (buffer_size(uncomp_buf) < sizeof(u16)) {
			ssize_t nr;

			buffer_compact(uncomp_buf);

			nr = buffer_inflate(comp_buf, uncomp_buf, stream);
			if (nr < 0)
				error("%s: zlib error\n", program);

			if (!nr)
				break;

			print_progress(comp_buf);

			goto retry_size;
		}

		buffer_advance(uncomp_buf, sizeof(u16));

retry_message:
		msg = itch41_message_decode(uncomp_buf);
		if (!msg) {
			ssize_t nr;

			buffer_compact(uncomp_buf);

			nr = buffer_inflate(comp_buf, uncomp_buf, stream);
			if (nr < 0)
				error("%s: zlib error\n", program);

			if (!nr)
				break;

			print_progress(comp_buf);

			goto retry_message;
		}

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

	if (argc < 3)
		usage();

	parse_args(argc - 1, argv + 1);

	if (!format)
		error("%s: unsupported file format", filename);

	init_stream(&stream);

	fd = open(filename, O_RDONLY);
	if (fd < 0)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	if (!strcmp(format, FORMAT_NASDAQ_ITCH_41)) {
		nasdaq_itch41_process(fd, &stream);
		nasdaq_itch41_print_stats();
	} else if (!strcmp(format, FORMAT_BATS_PITCH_112)) {
		bats_pitch112_process(fd, &stream);
		bats_pitch112_print_stats();
	} else
		error("%s is not a supported file format", format);

	printf("\n");

	if (close(fd) < 0)
		error("%s: %s: %s\n", program, filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
