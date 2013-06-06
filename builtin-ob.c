#include "builtins.h"

#include "bats-pitch112.h"
#include "decimal.h"
#include "base10.h"
#include "base36.h"
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
#include <locale.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <glib.h>

#define FORMAT_BATS_PITCH_112	"bats-pitch-1.12"

struct pitch_filter {
	char			symbol[6];
};

struct pitch_session {
	struct pitch_filter	filter;
	z_stream		*zstream;
	int			in_fd;
	int			out_fd;
	const char		*exchange;
	unsigned long		exchange_len;
	const char		*time_zone;
	unsigned long		time_zone_len;
};

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
	char			price[10];
};

/*
 * BATS PITCH 1.12
 */

static GHashTable *order_hash;

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

#define PITCH_FILENAME_DATE_LEN		8
#define PITCH_FILENAME_EXT		".dat.gz"
#define PITCH_FILENAME_SUFFIX_LEN	(PITCH_FILENAME_DATE_LEN + strlen(PITCH_FILENAME_EXT))

static int pitch_file_parse_date(const char *filename, char *buf, size_t buf_len)
{
	const char *suffix;
	size_t len;

	len = strlen(filename);
	if (len < PITCH_FILENAME_SUFFIX_LEN)
		return -EINVAL;

	suffix = filename + len - PITCH_FILENAME_SUFFIX_LEN;

	if (memcmp(suffix + PITCH_FILENAME_DATE_LEN, PITCH_FILENAME_EXT, strlen(PITCH_FILENAME_EXT)))
		return -EINVAL;

	snprintf(buf, buf_len, "%c%c%c%c-%c%c-%c%c",
		suffix[0], suffix[1], suffix[2], suffix[3],
		suffix[4], suffix[5], suffix[6], suffix[7]);

	return 0;
}

static void pitch_filter_init(struct pitch_filter *filter)
{
	memset(filter->symbol, ' ', sizeof(filter->symbol));
	memcpy(filter->symbol, symbol, strlen(symbol));
}

static bool pitch_filter_msg(struct pitch_filter *filter, struct pitch_message *msg)
{
	switch (msg->MessageType) {
	case PITCH_MSG_SYMBOL_CLEAR: {
		struct pitch_msg_symbol_clear *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_ADD_ORDER_SHORT: {
		struct pitch_msg_add_order_short *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_ADD_ORDER_LONG: {
		struct pitch_msg_add_order_long *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADE_SHORT: {
		struct pitch_msg_trade_short *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADE_LONG: {
		struct pitch_msg_trade_long *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
	}
	case PITCH_MSG_TRADING_STATUS: {
		struct pitch_msg_trading_status *m = (void *) msg;

		return !memcmp(m->StockSymbol, filter->symbol, sizeof(m->StockSymbol));
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

static void bats_pitch112_write(struct pitch_session *session, struct pitch_message *msg)
{
	struct order_info *info = NULL;
	struct ob_event event;

	info = lookup_order(msg);
	if (info)
		goto found;

	if (!pitch_filter_msg(&session->filter, msg))
		return;

found:
	switch (msg->MessageType) {
	case PITCH_MSG_SYMBOL_CLEAR: {
		struct pitch_msg_symbol_clear *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_CLEAR,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
		};

		ob_write_event(session->out_fd, &event);

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
		memcpy(info->price, m->Price, sizeof(m->Price));

		g_hash_table_insert(order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.side		= &m->SideIndicator,
			.side_len	= sizeof(m->SideIndicator),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct decimal) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(session->out_fd, &event);

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
		memcpy(info->price, m->Price, sizeof(m->Price));

		g_hash_table_insert(order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.side		= &m->SideIndicator,
			.side_len	= sizeof(m->SideIndicator),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct decimal) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(session->out_fd, &event);

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
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->ExecutedShares,
			.quantity_len	= sizeof(m->ExecutedShares),
			.price          = (struct decimal) {
				.integer        = info->price,
				.integer_len    = PITCH_PRICE_INT_LEN,
				.fraction       = info->price + PITCH_PRICE_INT_LEN,
				.fraction_len   = PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(session->out_fd, &event);

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
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.quantity	= m->CanceledShares,
			.quantity_len	= sizeof(m->CanceledShares),
		};

		ob_write_event(session->out_fd, &event);

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
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct decimal) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADE_LONG: {
		struct pitch_msg_trade_long *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
			.quantity	= m->Shares,
			.quantity_len	= sizeof(m->Shares),
			.price		= (struct decimal) {
				.integer	= m->Price,
				.integer_len	= PITCH_PRICE_INT_LEN,
				.fraction	= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len	= PITCH_PRICE_FRACTION_LEN,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADING_STATUS: {
		struct pitch_msg_trading_status *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_STATUS,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= symbol,
			.symbol_len	= strlen(symbol),
			.status		= &m->HaltStatus,
			.status_len	= sizeof(m->HaltStatus),
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	default:
		/* Ignore message */
		break;
	}
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

static void bats_pitch112_process(struct pitch_session *session)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(session->in_fd, &st) < 0)
		error("%s: %s", input_filename, strerror(errno));

	comp_buf = buffer_mmap(session->in_fd, st.st_size);
	if (!comp_buf)
		error("%s: %s", input_filename, strerror(errno));

	session->zstream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s", strerror(errno));

	stream = (struct stream) {
		.zstream	= session->zstream,
		.uncomp_buf	= uncomp_buf,
		.comp_buf	= comp_buf,
		.progress	= print_progress,
	};

	order_hash = g_hash_table_new(g_int_hash, g_int_equal);
	if (!order_hash)
		error("out of memory");

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch112_read(&stream, &msg);
		if (err)
			error("%s: %s", input_filename, strerror(err));

		if (!msg)
			break;

		bats_pitch112_write(session, msg);
	}

	g_hash_table_destroy(order_hash);

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
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
			.time_zone	= "America/New_York",
			.time_zone_len	= strlen("America/New_York"),
			.exchange	= "BATS",
			.exchange_len	= strlen("BATS"),
		};

		pitch_filter_init(&session.filter);

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
		error("%s\n", input_filename, strerror(errno));

	if (close(out_fd) < 0)
		error("%s\n", output_filename, strerror(errno));

	release_stream(&stream);

	return 0;
}
