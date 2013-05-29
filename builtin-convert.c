#include "builtins.h"

#include "bats-pitch112.h"
#include "stream.h"
#include "types.h"
#include "ob.h"

#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/buffer.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <assert.h>
#include <getopt.h>
#include <libgen.h>
#include <locale.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <glib.h>

#define FORMAT_BATS_PITCH_112	"bats-pitch-1.12"

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
"\n usage: %s stat [<options>] <input> <output>\n"			\
"\n"									\
"    -s, --symbol <symbol> symbol\n"					\
"    -f, --format <format> input file format\n"				\
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
	{ "format",	required_argument, 	NULL, 'f' },
	{ "symbol",	required_argument, 	NULL, 's' },
};

static const char	*output_filename;
static const char	*input_filename;
static const char	*format;
static const char	*symbol;

static void parse_args(int argc, char *argv[])
{
	int opt;

	while ((opt = getopt_long(argc, argv, "s:f:", options, NULL)) != -1) {
		switch (opt) {
		case 's':
			symbol		= optarg;
			break;
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

	input_filename	= argv[0];
	output_filename = argv[1];
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

struct order_info {
	uint64_t		order_id;
	uint32_t		remaining;
};

/*
 * BATS PITCH 1.12
 */

static GHashTable *order_hash;

#define BASE36_DIGIT(ch) ((ch) - '0')

static unsigned char base36_table[64] = {
	[BASE36_DIGIT('0')] = 0,
	[BASE36_DIGIT('1')] = 1,
	[BASE36_DIGIT('2')] = 2,
	[BASE36_DIGIT('3')] = 3,
	[BASE36_DIGIT('4')] = 4,
	[BASE36_DIGIT('5')] = 5,
	[BASE36_DIGIT('6')] = 6,
	[BASE36_DIGIT('7')] = 7,
	[BASE36_DIGIT('8')] = 8,
	[BASE36_DIGIT('9')] = 9,
	[BASE36_DIGIT('A')] = 10,
	[BASE36_DIGIT('B')] = 11,
	[BASE36_DIGIT('C')] = 12,
	[BASE36_DIGIT('D')] = 13,
	[BASE36_DIGIT('E')] = 14,
	[BASE36_DIGIT('F')] = 15,
	[BASE36_DIGIT('G')] = 16,
	[BASE36_DIGIT('H')] = 17,
	[BASE36_DIGIT('I')] = 18,
	[BASE36_DIGIT('J')] = 19,
	[BASE36_DIGIT('K')] = 20,
	[BASE36_DIGIT('L')] = 21,
	[BASE36_DIGIT('M')] = 22,
	[BASE36_DIGIT('N')] = 23,
	[BASE36_DIGIT('O')] = 24,
	[BASE36_DIGIT('P')] = 25,
	[BASE36_DIGIT('Q')] = 26,
	[BASE36_DIGIT('R')] = 27,
	[BASE36_DIGIT('S')] = 28,
	[BASE36_DIGIT('T')] = 29,
	[BASE36_DIGIT('U')] = 30,
	[BASE36_DIGIT('V')] = 31,
	[BASE36_DIGIT('W')] = 32,
	[BASE36_DIGIT('X')] = 33,
	[BASE36_DIGIT('Y')] = 34,
	[BASE36_DIGIT('Z')] = 35,
};

static unsigned long base36_pos(unsigned char ch)
{
	return base36_table[BASE36_DIGIT(ch)];
}

static uint64_t base36_decode(const char *s, size_t len)
{
	uint64_t ret = 0;
	unsigned int i;

	for (i = 0; i < len; i++) {
		char ch = s[i];

		ret *= 36;

		ret += base36_pos(ch);
	}

	return ret;
}

static uint64_t base10_decode(const char *s, size_t len)
{
	uint64_t ret = 0;
	unsigned int i;

	for (i = 0; i < len; i++) {
		char ch = s[i];

		ret *= 10;

		ret += (ch - '0');
	}

	return ret;
}

static struct order_info *lookup_order(struct pitch_message *msg)
{
	switch (msg->MessageType) {
	case PITCH_MSG_ORDER_EXECUTED: {
		struct pitch_msg_order_executed *m = (void *) msg;
		unsigned long order_id;

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		return g_hash_table_lookup(order_hash, &order_id);
	}
	case PITCH_MSG_ORDER_CANCEL: {
		struct pitch_msg_order_cancel *m = (void *) msg;
		unsigned long order_id;

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		return g_hash_table_lookup(order_hash, &order_id);
	}
	default:
		break;
	}

	return NULL;
}

static bool pitch_symbol_filter(struct pitch_message *msg, const char *s)
{
	switch (msg->MessageType) {
	case PITCH_MSG_SYMBOL_CLEAR: {
		struct pitch_msg_symbol_clear *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_ADD_ORDER_SHORT: {
		struct pitch_msg_add_order_short *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_ADD_ORDER_LONG: {
		struct pitch_msg_add_order_long *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADE_SHORT: {
		struct pitch_msg_trade_short *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADE_LONG: {
		struct pitch_msg_trade_long *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADING_STATUS: {
		struct pitch_msg_trading_status *m = (void *) msg;

		return !memcmp(m->StockSymbol, s, sizeof(m->StockSymbol));
	}
	default:
		break;
	}

	return false;
}

static gboolean remove_entry(gpointer __maybe_unused key, gpointer __maybe_unused  val, gpointer __maybe_unused data)
{
	free(val);

	return TRUE;
}

#define PITCH_PRICE_INT_LEN		6
#define PITCH_PRICE_FRACTION_LEN	4

static void bats_pitch112_write(int out_fd, struct pitch_message *msg)
{
	struct order_info *info = NULL;
	struct ob_event event;

	info = lookup_order(msg);
	if (info)
		goto found;

	if (!pitch_symbol_filter(msg, symbol))
		return;

found:
	switch (msg->MessageType) {
	case PITCH_MSG_SYMBOL_CLEAR: {
		struct pitch_msg_symbol_clear *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_CLEAR,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
		};

		ob_write_event(out_fd, &event);

		g_hash_table_foreach_remove(order_hash, remove_entry, NULL);

		break;
	}
	case PITCH_MSG_ADD_ORDER_SHORT: {
		struct pitch_msg_add_order_short *m = (void *) msg;
		unsigned long order_id;

		assert(info == NULL);

		info = malloc(sizeof(*info));

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		info->order_id	= order_id;
		info->remaining	= base10_decode(m->Shares, sizeof(m->Shares));

		g_hash_table_insert(order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.side		= &m->SideIndicator,
			.side_len	= sizeof(m->SideIndicator),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct ob_price) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(out_fd, &event);

		break;
	}
	case PITCH_MSG_ADD_ORDER_LONG: {
		struct pitch_msg_add_order_long *m = (void *) msg;
		unsigned long order_id;

		assert(info == NULL);

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		info = malloc(sizeof(*info));
		info->order_id	= order_id;
		info->remaining	= base10_decode(m->Shares, sizeof(m->Shares));

		g_hash_table_insert(order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.side		= &m->SideIndicator,
			.side_len	= sizeof(m->SideIndicator),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct ob_price) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(out_fd, &event);

		break;
	}
	case PITCH_MSG_ORDER_EXECUTED: {
		struct pitch_msg_order_executed *m = (void *) msg;
		unsigned int nr_executed;

		assert(info != NULL);

		nr_executed = base10_decode(m->ExecutedShares, sizeof(m->ExecutedShares));

		assert(info->remaining >= nr_executed);

		info->remaining	-= nr_executed;

		event = (struct ob_event) {
			.type		= OB_EVENT_EXECUTE_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->ExecutedShares,
			.quantity_len	= sizeof(m->ExecutedShares),
		};

		ob_write_event(out_fd, &event);

		if (!info->remaining) {
			if (!g_hash_table_remove(order_hash, &info->order_id))
				assert(0);

			free(info);
		}

		break;
	}
	case PITCH_MSG_ORDER_CANCEL: {
		struct pitch_msg_order_cancel *m = (void *) msg;
		unsigned int nr_canceled;

		assert(info != NULL);

		nr_canceled = base10_decode(m->CanceledShares, sizeof(m->CanceledShares));

		assert(info->remaining >= nr_canceled);

		info->remaining	-= nr_canceled;

		event = (struct ob_event) {
			.type		= OB_EVENT_CANCEL_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.quantity	= m->CanceledShares,
			.quantity_len	= sizeof(m->CanceledShares),
		};

		ob_write_event(out_fd, &event);

		if (!info->remaining) {
			if (!g_hash_table_remove(order_hash, &info->order_id))
				assert(0);

			free(info);
		}

		break;
	}
	case PITCH_MSG_TRADE_SHORT: {
		struct pitch_msg_trade_short *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct ob_price) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADE_LONG: {
		struct pitch_msg_trade_long *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct ob_price) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADING_STATUS: {
		struct pitch_msg_trading_status *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_STATUS,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.status		= &m->HaltStatus,
			.status_len	= sizeof(m->HaltStatus),
		};

		ob_write_event(out_fd, &event);

		break;
	}
	default:
		/* Ignore message */
		break;
	}
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

static void bats_pitch112_process(int fd, z_stream *zstream, int out_fd)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s: %s", program, input_filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s: %s", program, input_filename, strerror(errno));

	zstream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s: %s", program, strerror(errno));

	stream = (struct stream) {
		.zstream	= zstream,
		.uncomp_buf	= uncomp_buf,
		.comp_buf	= comp_buf,
		.progress	= print_progress,
	};

	order_hash = g_hash_table_new(g_int_hash, g_int_equal);
	if (!order_hash)
		error("no mem");

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch112_read(&stream, &msg);
		if (err)
			error("%s: %s: %s", program, input_filename, strerror(err));

		if (!msg)
			break;

		bats_pitch112_write(out_fd, msg);
	}

	g_hash_table_destroy(order_hash);

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}

int cmd_convert(int argc, char *argv[])
{
	int in_fd, out_fd;
	z_stream stream;

	setlocale(LC_ALL, "");

	if (argc < 4)
		usage();

	parse_args(argc - 1, argv + 1);

	if (!format)
		error("%s: unsupported file format", input_filename);

	if (!symbol)
		error("symbol not specified");

	init_stream(&stream);

	in_fd = open(input_filename, O_RDONLY);
	if (in_fd < 0)
		error("%s: %s: %s", program, input_filename, strerror(errno));

	out_fd = open(output_filename, O_RDWR|O_CREAT|O_EXCL, 0644);
	if (out_fd < 0)
		error("%s: %s: %s", program, output_filename, strerror(errno));

	ob_write_header(out_fd);

	if (!strcmp(format, FORMAT_BATS_PITCH_112)) {
		struct ob_event event;

		event = (struct ob_event) {
			.type		= OB_EVENT_DATE,
			.time_zone	= "America/New_York",
			.time_zone_len	= strlen("America/New_York"),
			.exchange	= "BZX",
			.exchange_len	= strlen("BZX"),
		};

		ob_write_event(out_fd, &event);

		bats_pitch112_process(in_fd, &stream, out_fd);
	} else
		error("%s is not a supported file format", format);

	printf("\n");

	if (close(in_fd) < 0)
		error("%s: %s\n", program, input_filename, strerror(errno));

	if (close(out_fd) < 0)
		error("%s: %s\n", program, output_filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
