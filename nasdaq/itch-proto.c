#include "tick/nasdaq/itch-proto.h"

#include "tick/stream.h"

#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/byte-order.h"
#include "libtrading/buffer.h"

#include <string.h>
#include <stdio.h>
#include <errno.h>

int nasdaq_itch_read(struct stream *stream, struct itch41_message **msg_p)
{
	struct itch41_message *msg;

	*msg_p = NULL;

retry_size:
	if (buffer_size(stream->uncomp_buf) < sizeof(u16)) {
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

	buffer_advance(stream->uncomp_buf, sizeof(u16));

retry_message:
	msg = itch41_message_decode(stream->uncomp_buf);
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

	*msg_p = msg;

	return 0;
}

#define ITCH_FILENAME_DATE_LEN		6
#define ITCH_FILENAME_EXT		"-v41.txt.gz"
#define ITCH_FILENAME_LEN		(1 + ITCH_FILENAME_DATE_LEN + strlen(ITCH_FILENAME_EXT))

int nasdaq_itch_file_parse_date(const char *filename, char *buf, size_t buf_len)
{
	size_t len;

	len = strlen(filename);
	if (len < strlen(ITCH_FILENAME_EXT) + ITCH_FILENAME_DATE_LEN)
		return -EINVAL;

	filename = filename + len - ITCH_FILENAME_LEN;

	if (filename[0] != 'S')
		return -EINVAL;
		
	if (memcmp(filename + 1 + ITCH_FILENAME_DATE_LEN, ITCH_FILENAME_EXT, strlen(ITCH_FILENAME_EXT)))
		return -EINVAL;

        snprintf(buf, buf_len, "20%c%c-%c%c-%c%c",
                filename[5], filename[6], filename[1], filename[2], filename[3], filename[4]);

	return 0;
}

void nasdaq_itch_filter_init(struct nasdaq_itch_filter *filter, const char *symbol)
{
	memset(filter->symbol, ' ', sizeof(filter->symbol));
	memcpy(filter->symbol, symbol, strlen(symbol));
}

struct nasdaq_itch_order_info *
nasdaq_itch_session_lookup_order(struct nasdaq_itch_session *session, struct itch41_message *msg)
{
	switch (msg->MessageType) {
	case ITCH41_MSG_ORDER_EXECUTED: {
		struct itch41_msg_order_executed *m = (void *) msg;
		uint64_t order_ref_num;

		order_ref_num = be64_to_cpu(m->OrderReferenceNumber);

		return g_hash_table_lookup(session->order_hash, &order_ref_num);
	}
	case ITCH41_MSG_ORDER_EXECUTED_WITH_PRICE: {
		struct itch41_msg_order_executed_with_price *m = (void *) msg;
		uint64_t order_ref_num;

		order_ref_num = be64_to_cpu(m->OrderReferenceNumber);

		return g_hash_table_lookup(session->order_hash, &order_ref_num);
	}
	case ITCH41_MSG_ORDER_CANCEL: {
		struct itch41_msg_order_cancel *m = (void *) msg;
		uint64_t order_ref_num;

		order_ref_num = be64_to_cpu(m->OrderReferenceNumber);

		return g_hash_table_lookup(session->order_hash, &order_ref_num);
	}
	case ITCH41_MSG_ORDER_DELETE: {
		struct itch41_msg_order_delete *m = (void *) msg;
		uint64_t order_ref_num;

		order_ref_num = be64_to_cpu(m->OrderReferenceNumber);

		return g_hash_table_lookup(session->order_hash, &order_ref_num);
	}
	case ITCH41_MSG_ORDER_REPLACE: {
		struct itch41_msg_order_replace *m = (void *) msg;
		uint64_t order_ref_num;

		order_ref_num = be64_to_cpu(m->OriginalOrderReferenceNumber);

		return g_hash_table_lookup(session->order_hash, &order_ref_num);
	}
	default:
		break;
	}

	return NULL;
}

bool nasdaq_itch_session_filter_msg(struct nasdaq_itch_session *session, struct itch41_message *msg)
{
	struct nasdaq_itch_filter *filter = &session->filter;

	switch (msg->MessageType) {
	case ITCH41_MSG_TIMESTAMP_SECONDS: {
		return true;
	}
	case ITCH41_MSG_STOCK_DIRECTORY: {
		struct itch41_msg_stock_directory *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_STOCK_TRADING_ACTION: {
		struct itch41_msg_stock_trading_action *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_ADD_ORDER: {
		struct itch41_msg_add_order *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_ADD_ORDER_MPID: {
		struct itch41_msg_add_order_mpid *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_TRADE: {
		struct itch41_msg_trade *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_CROSS_TRADE: {
		struct itch41_msg_cross_trade *m = (void *) msg;

		return !memcmp(m->Stock, filter->symbol, sizeof(m->Stock));
	}
	case ITCH41_MSG_BROKEN_TRADE: {
		struct itch41_msg_broken_trade *m = (void *) msg;
		struct nasdaq_itch_exec_info *e_info;
		uint64_t match_num;

		match_num = be64_to_cpu(m->MatchNumber);

		e_info = g_hash_table_lookup(session->exec_hash, &match_num);
		if (!e_info)
			return false;

		return !memcmp(e_info->symbol, filter->symbol, 6);
	}
	default:
		break;
	}

	return false;
}
