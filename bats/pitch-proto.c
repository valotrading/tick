#include "tick/bats/pitch-proto.h"

#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/buffer.h"

#include "tick/base36.h"
#include "tick/stream.h"

#include <string.h>
#include <stdio.h>
#include <errno.h>

int bats_pitch_read(struct stream *stream, struct pitch_message **msg_p)
{
	struct pitch_message *msg;
	unsigned char ch;

	*msg_p = NULL;

retry_size:
	if (buffer_size(stream->uncomp_buf) < sizeof(u8) + sizeof(struct pitch_message)) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr < 0)
			return nr;

		if (!nr)
			return 0;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_size;
	}

	ch = buffer_peek_8(stream->uncomp_buf);
	if (ch != 0x53)
		return -EINVAL;

	buffer_advance(stream->uncomp_buf, sizeof(u8));

retry_message:
	msg = pitch_message_decode(stream->uncomp_buf, 1);
	if (!msg) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr < 0)
			return nr;

		if (!nr)
			return 0;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_message;
	}

	ch = buffer_peek_8(stream->uncomp_buf);
	if (ch != 0x0A)
		return -EINVAL;

	buffer_advance(stream->uncomp_buf, sizeof(u8));

	*msg_p = msg;

	return 0;
}

#define PITCH_FILENAME_DATE_LEN		8
#define PITCH_FILENAME_EXT		".dat.gz"
#define PITCH_FILENAME_SUFFIX_LEN	(PITCH_FILENAME_DATE_LEN + strlen(PITCH_FILENAME_EXT))

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

struct pitch_order_info *
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

bool pitch_session_filter_msg(struct pitch_session *session, struct pitch_message *msg)
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
		struct pitch_exec_info *e_info;
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
