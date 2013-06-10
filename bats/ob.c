#include "tick/bats/pitch-proto.h"

#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/buffer.h"

#include "tick/progress.h"
#include "tick/decimal.h"
#include "tick/base10.h"
#include "tick/base36.h"
#include "tick/format.h"
#include "tick/stream.h"
#include "tick/error.h"
#include "tick/types.h"
#include "tick/ob.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <assert.h>
#include <getopt.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <glib.h>

struct exec_info {
	uint64_t		exec_id;
	const char		*symbol;
};

struct order_info {
	uint64_t		order_id;
	uint32_t		remaining;
	char			price[10];
};

#define PITCH_PRICE_INT_LEN		6
#define PITCH_PRICE_FRACTION_LEN	4

#define PITCH_FILENAME_DATE_LEN		8
#define PITCH_FILENAME_EXT		".dat.gz"
#define PITCH_FILENAME_SUFFIX_LEN	(PITCH_FILENAME_DATE_LEN + strlen(PITCH_FILENAME_EXT))

static struct order_info *
pitch_session_lookup_order(struct pitch_session *session, struct pitch_message *msg)
{
	switch (msg->MessageType) {
	case PITCH_MSG_ORDER_EXECUTED: {
		struct pitch_msg_order_executed *m = (void *) msg;
		unsigned long order_id;

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		return g_hash_table_lookup(session->order_hash, &order_id);
	}
	case PITCH_MSG_ORDER_CANCEL: {
		struct pitch_msg_order_cancel *m = (void *) msg;
		unsigned long order_id;

		order_id = base36_decode(m->OrderID, sizeof(m->OrderID));

		return g_hash_table_lookup(session->order_hash, &order_id);
	}
	default:
		break;
	}

	return NULL;
}

int pitch_file_parse_date(const char *filename, char *buf, size_t buf_len)
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

void pitch_filter_init(struct pitch_filter *filter, const char *symbol)
{
	memset(filter->symbol, ' ', sizeof(filter->symbol));
	memcpy(filter->symbol, symbol, strlen(symbol));
}

static bool pitch_session_filter_msg(struct pitch_session *session, struct pitch_message *msg)
{
	struct pitch_filter *filter = &session->filter;

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
	case PITCH_MSG_TRADE_BREAK: {
		struct pitch_msg_trade_break *m = (void *) msg;
		struct exec_info *e_info;
		unsigned long exec_id;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = g_hash_table_lookup(session->exec_hash, &exec_id);
		if (!e_info)
			return false;

		return !memcmp(e_info->symbol, filter->symbol, 6);
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

static gboolean free_entry(gpointer __maybe_unused key, gpointer __maybe_unused  val, gpointer __maybe_unused data)
{
	free(val);

	return TRUE;
}

static void bats_pitch112_write(struct pitch_session *session, struct pitch_message *msg)
{
	struct order_info *info = NULL;
	struct ob_event event;

	info = pitch_session_lookup_order(session, msg);
	if (info)
		goto found;

	if (!pitch_session_filter_msg(session, msg))
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
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
		};

		ob_write_event(session->out_fd, &event);

		g_hash_table_foreach_remove(session->order_hash, free_entry, NULL);

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

		g_hash_table_insert(session->order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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

		g_hash_table_insert(session->order_hash, &info->order_id, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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
		struct exec_info *e_info;
		unsigned long exec_id;

		assert(info != NULL);

		nr_executed = base10_decode(m->ExecutedShares, sizeof(m->ExecutedShares));

		assert(info->remaining >= nr_executed);

		info->remaining	-= nr_executed;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = malloc(sizeof(*e_info));
		e_info->exec_id	= exec_id;
		e_info->symbol	= session->filter.symbol;

		g_hash_table_insert(session->exec_hash, &e_info->exec_id, e_info);

		event = (struct ob_event) {
			.type		= OB_EVENT_EXECUTE_ORDER,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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
			if (!g_hash_table_remove(session->order_hash, &info->order_id))
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
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= m->OrderID,
			.order_id_len	= sizeof(m->OrderID),
			.quantity	= m->CanceledShares,
			.quantity_len	= sizeof(m->CanceledShares),
		};

		ob_write_event(session->out_fd, &event);

		if (!info->remaining) {
			if (!g_hash_table_remove(session->order_hash, &info->order_id))
				assert(0);

			free(info);
		}

		break;
	}
	case PITCH_MSG_TRADE_SHORT: {
		struct pitch_msg_trade_short *m = (void *) msg;
		struct exec_info *e_info;
		unsigned long exec_id;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = malloc(sizeof(*e_info));
		e_info->exec_id	= exec_id;
		e_info->symbol	= session->filter.symbol;

		g_hash_table_insert(session->exec_hash, &e_info->exec_id, e_info);

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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
		struct exec_info *e_info;
		unsigned long exec_id;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = malloc(sizeof(*e_info));
		e_info->exec_id	= exec_id;
		e_info->symbol	= session->filter.symbol;

		g_hash_table_insert(session->exec_hash, &e_info->exec_id, e_info);

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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
	case PITCH_MSG_TRADE_BREAK: {
		struct pitch_msg_trade_break *m = (void *) msg;

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE_BREAK,
			.time		= m->Timestamp,
			.time_len	= sizeof(m->Timestamp),
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
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
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
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

void bats_pitch112_process(struct pitch_session *session)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(session->in_fd, &st) < 0)
		error("%s: %s", session->input_filename, strerror(errno));

	comp_buf = buffer_mmap(session->in_fd, st.st_size);
	if (!comp_buf)
		error("%s: %s", session->input_filename, strerror(errno));

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

	session->exec_hash = g_hash_table_new(g_int_hash, g_int_equal);
	if (!session->exec_hash)
		error("out of memory");

	session->order_hash = g_hash_table_new(g_int_hash, g_int_equal);
	if (!session->order_hash)
		error("out of memory");

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch112_read(&stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		bats_pitch112_write(session, msg);
	}

	g_hash_table_destroy(session->order_hash);

	g_hash_table_foreach_remove(session->exec_hash, free_entry, NULL);

	g_hash_table_destroy(session->exec_hash);

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
