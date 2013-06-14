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
#include "tick/taq.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <glib.h>

static gboolean free_entry(gpointer __maybe_unused key, gpointer __maybe_unused  val, gpointer __maybe_unused data)
{
	free(val);

	return TRUE;
}

static void bats_pitch_write(struct pitch_session *session, struct pitch_message *msg)
{
	struct pitch_order_info *info = NULL;
	struct taq_event event;

	info = pitch_session_lookup_order(session, msg);
	if (info)
		goto found;

	if (!pitch_session_filter_msg(session, msg))
		return;

found:
	switch (msg->MessageType) {
	case PITCH_MSG_SYMBOL_CLEAR: {
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

		break;
	}
	case PITCH_MSG_ORDER_EXECUTED: {
		struct pitch_msg_order_executed *m = (void *) msg;
		unsigned int nr_executed;
		struct pitch_exec_info *e_info;
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

		event = (struct taq_event) {
			.type			= TAQ_EVENT_TRADE,
			.time			= (struct time) {
				.value			= m->Timestamp,
				.value_len		= sizeof(m->Timestamp),
				.unit			= TIME_UNIT_MILLISECONDS,
			},
			.exchange		= session->exchange,
			.exchange_len		= session->exchange_len,
			.symbol			= session->symbol,
			.symbol_len		= session->symbol_len,
			.exec_id		= m->ExecutionID,
			.exec_id_len		= sizeof(m->ExecutionID),
			.trade_quantity		= m->ExecutedShares,
			.trade_quantity_len	= sizeof(m->ExecutedShares),
			.trade_price		= (struct decimal) {
				.integer		= info->price,
				.integer_len		= PITCH_PRICE_INT_LEN,
				.fraction		= info->price + PITCH_PRICE_INT_LEN,
				.fraction_len		= PITCH_PRICE_FRACTION_LEN,
			},
			.trade_type		= TAQ_TRADE_TYPE_REGULAR,
			.trade_type_len		= TAQ_TRADE_TYPE_LEN,
		};

		taq_write_event(session->out_fd, &event);

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

		if (!info->remaining) {
			if (!g_hash_table_remove(session->order_hash, &info->order_id))
				assert(0);

			free(info);
		}

		break;
	}
	case PITCH_MSG_TRADE_SHORT: {
		struct pitch_msg_trade_short *m = (void *) msg;
		struct pitch_exec_info *e_info;
		unsigned long exec_id;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = malloc(sizeof(*e_info));
		e_info->exec_id	= exec_id;
		e_info->symbol	= session->filter.symbol;

		g_hash_table_insert(session->exec_hash, &e_info->exec_id, e_info);

		event = (struct taq_event) {
			.type			= TAQ_EVENT_TRADE,
			.time			= (struct time) {
				.value			= m->Timestamp,
				.value_len		= sizeof(m->Timestamp),
				.unit			= TIME_UNIT_MILLISECONDS,
			},
			.exchange		= session->exchange,
			.exchange_len		= session->exchange_len,
			.symbol			= session->symbol,
			.symbol_len		= session->symbol_len,
			.exec_id		= m->ExecutionID,
			.exec_id_len		= sizeof(m->ExecutionID),
			.trade_quantity		= m->Shares,
			.trade_quantity_len	= sizeof(m->Shares),
			.trade_price		= (struct decimal) {
				.integer		= m->Price,
				.integer_len		= PITCH_PRICE_INT_LEN,
				.fraction		= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len		= PITCH_PRICE_FRACTION_LEN,
			},
			.trade_type		= TAQ_TRADE_TYPE_NON_DISPLAYED,
			.trade_type_len		= TAQ_TRADE_TYPE_LEN,
		};
		taq_write_event(session->out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADE_LONG: {
		struct pitch_msg_trade_long *m = (void *) msg;
		struct pitch_exec_info *e_info;
		unsigned long exec_id;

		exec_id = base36_decode(m->ExecutionID, sizeof(m->ExecutionID));

		e_info = malloc(sizeof(*e_info));
		e_info->exec_id	= exec_id;
		e_info->symbol	= session->filter.symbol;

		g_hash_table_insert(session->exec_hash, &e_info->exec_id, e_info);

		event = (struct taq_event) {
			.type			= TAQ_EVENT_TRADE,
			.time			= (struct time) {
				.value			= m->Timestamp,
				.value_len		= sizeof(m->Timestamp),
				.unit			= TIME_UNIT_MILLISECONDS,
			},
			.exchange		= session->exchange,
			.exchange_len		= session->exchange_len,
			.symbol			= session->symbol,
			.symbol_len		= session->symbol_len,
			.exec_id		= m->ExecutionID,
			.exec_id_len		= sizeof(m->ExecutionID),
			.trade_quantity		= m->Shares,
			.trade_quantity_len	= sizeof(m->Shares),
			.trade_price		= (struct decimal) {
				.integer		= m->Price,
				.integer_len		= PITCH_PRICE_INT_LEN,
				.fraction		= m->Price + PITCH_PRICE_INT_LEN,
				.fraction_len		= PITCH_PRICE_FRACTION_LEN,
			},
			.trade_type		= TAQ_TRADE_TYPE_NON_DISPLAYED,
			.trade_type_len		= TAQ_TRADE_TYPE_LEN,
		};
		taq_write_event(session->out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADE_BREAK: {
		struct pitch_msg_trade_break *m = (void *) msg;

		event = (struct taq_event) {
			.type		= TAQ_EVENT_TRADE_BREAK,
			.time		= (struct time) {
				.value		= m->Timestamp,
				.value_len	= sizeof(m->Timestamp),
				.unit		= TIME_UNIT_MILLISECONDS,
			},
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.exec_id	= m->ExecutionID,
			.exec_id_len	= sizeof(m->ExecutionID),
		};

		taq_write_event(session->out_fd, &event);

		break;
	}
	case PITCH_MSG_TRADING_STATUS: {
		struct pitch_msg_trading_status *m = (void *) msg;

		event = (struct taq_event) {
			.type		= TAQ_EVENT_STATUS,
			.time		= (struct time) {
				.value		= m->Timestamp,
				.value_len	= sizeof(m->Timestamp),
				.unit		= TIME_UNIT_MILLISECONDS,
			},
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.status		= &m->HaltStatus,
			.status_len	= sizeof(m->HaltStatus),
		};

		taq_write_event(session->out_fd, &event);

		break;
	}
	default:
		/* Ignore message */
		break;
	}
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void bats_pitch_taq(struct pitch_session *session)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct taq_event event;
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

	event = (struct taq_event) {
		.type		= TAQ_EVENT_DATE,
		.date		= session->date,
		.date_len	= session->date_len,
		.time_zone	= session->time_zone,
		.time_zone_len	= session->time_zone_len,
		.exchange	= session->exchange,
		.exchange_len	= session->exchange_len,
	};

	taq_write_event(session->out_fd, &event);

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch_read(&stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		bats_pitch_write(session, msg);
	}

	g_hash_table_destroy(session->order_hash);

	g_hash_table_foreach_remove(session->exec_hash, free_entry, NULL);

	g_hash_table_destroy(session->exec_hash);

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
