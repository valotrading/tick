#include "tick/nasdaq/itch-proto.h"

#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/byte-order.h"
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
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <glib.h>

struct nasdaq_ob_event {
	char			timestamp[16];
	unsigned int		timestamp_len;
	char			order_id[16];
	unsigned int		order_id_len;
	char			exec_id[16];
	unsigned int		exec_id_len;
	char			quantity[16];
	unsigned int		quantity_len;
	char			price[16];
	unsigned int		price_len;
};

static void fmt_timestamp(struct nasdaq_ob_event *ev, uint64_t timestamp)
{
	ev->timestamp_len = snprintf(ev->timestamp, sizeof(ev->timestamp), "%lu", timestamp);
}

static void fmt_order_id(struct nasdaq_ob_event *ev, uint64_t order_ref_num)
{
	ev->order_id_len = snprintf(ev->order_id, sizeof(ev->order_id), "%lu", order_ref_num);
}

static void fmt_exec_id(struct nasdaq_ob_event *ev, uint64_t match_num)
{
	ev->exec_id_len = snprintf(ev->exec_id, sizeof(ev->exec_id), "%lu", match_num);
}

static void fmt_quantity(struct nasdaq_ob_event *ev, uint32_t shares)
{
	ev->quantity_len = snprintf(ev->quantity, sizeof(ev->quantity), "%u", shares);
}

static void fmt_price(struct nasdaq_ob_event *ev, uint32_t price)
{
	ev->price_len = snprintf(ev->price, sizeof(ev->price), "%.10u", price);
}

static gboolean free_entry(gpointer __maybe_unused key, gpointer __maybe_unused  val, gpointer __maybe_unused data)
{
	free(val);

	return TRUE;
}

static uint64_t nasdaq_itch_timestamp(struct nasdaq_itch_session *session, unsigned long nsec)
{
	return session->second * 1000000000UL + nsec;
}

static void nasdaq_itch_write(struct nasdaq_itch_session *session, struct itch41_message *msg)
{
	struct nasdaq_itch_order_info *info;
	struct nasdaq_ob_event n_event;
	struct ob_event event;

	info = nasdaq_itch_session_lookup_order(session, msg);
	if (info)
		goto found;

	if (!nasdaq_itch_session_filter_msg(session, msg))
		return;

found:
	switch (msg->MessageType) {
	case ITCH41_MSG_TIMESTAMP_SECONDS: {
		struct itch41_msg_timestamp_seconds *m = (void *) msg;

		session->second = be32_to_cpu(m->Second);

		break;
	}
	case ITCH41_MSG_STOCK_TRADING_ACTION: {
		struct itch41_msg_stock_trading_action *m = (void *) msg;
		uint64_t timestamp_nsec;
		char status;

		timestamp_nsec = nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));

		fmt_timestamp(&n_event, timestamp_nsec);

		switch (m->TradingState) {
		case 'H': status = 'H'; break;
		case 'P': status = 'P'; break;
		case 'Q': status = 'Q'; break;
		case 'T': status = 'T'; break;
		default:
			assert(0);
		}

		event = (struct ob_event) {
			.type		= OB_EVENT_STATUS,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.status		= &status,
			.status_len	= sizeof(status),
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case ITCH41_MSG_ADD_ORDER: {
		struct itch41_msg_add_order *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;
		uint32_t shares;
		uint32_t price;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);
		shares		= be32_to_cpu(m->Shares);
		price		= be32_to_cpu(m->Price);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, shares);
		fmt_price    (&n_event, price);

		info = malloc(sizeof(*info));
		info->order_ref_num	= order_ref_num;
		info->remaining		= shares;
		info->price		= price;
		info->side		= m->BuySellIndicator;

		g_hash_table_insert(session->order_hash, &info->order_ref_num, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.side		= &info->side,
			.side_len	= sizeof(info->side),
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price + 6,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case ITCH41_MSG_ADD_ORDER_MPID: {
		struct itch41_msg_add_order_mpid *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;
		uint32_t shares;
		uint32_t price;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);
		shares		= be32_to_cpu(m->Shares);
		price		= be32_to_cpu(m->Price);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, shares);
		fmt_price    (&n_event, price);

		info = malloc(sizeof(*info));
		info->order_ref_num	= order_ref_num;
		info->remaining		= shares;
		info->price		= price;
		info->side		= m->BuySellIndicator;

		g_hash_table_insert(session->order_hash, &info->order_ref_num, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.side		= &m->BuySellIndicator,
			.side_len	= sizeof(m->BuySellIndicator),
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price + 6,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case ITCH41_MSG_ORDER_EXECUTED: {
		struct itch41_msg_order_executed *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;
		uint64_t match_num;
		uint32_t shares;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);
		shares		= be32_to_cpu(m->ExecutedShares);
		match_num	= be64_to_cpu(m->MatchNumber);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, shares);
		fmt_exec_id  (&n_event, match_num);
		fmt_price    (&n_event, info->price);

		event = (struct ob_event) {
			.type		= OB_EVENT_EXECUTE_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.exec_id	= n_event.exec_id,
			.exec_id_len	= n_event.exec_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price + 6,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		assert(info->remaining >= shares);

		info->remaining -= shares;

		if (!info->remaining) {
			if (!g_hash_table_remove(session->order_hash, &info->order_ref_num))
				assert(0);

			free(info);
		}

		break;
	}
	case ITCH41_MSG_ORDER_EXECUTED_WITH_PRICE: {
		struct itch41_msg_order_executed_with_price *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;
		uint64_t match_num;
		uint32_t shares;
		uint32_t price;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);
		shares		= be32_to_cpu(m->ExecutedShares);
		match_num	= be64_to_cpu(m->MatchNumber);
		price		= be32_to_cpu(m->ExecutionPrice);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, shares);
		fmt_exec_id  (&n_event, match_num);
		fmt_price    (&n_event, price);

		event = (struct ob_event) {
			.type		= OB_EVENT_EXECUTE_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.exec_id	= n_event.exec_id,
			.exec_id_len	= n_event.exec_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price + 6,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		assert(info->remaining >= shares);

		info->remaining -= shares;

		if (!info->remaining) {
			if (!g_hash_table_remove(session->order_hash, &info->order_ref_num))
				assert(0);

			free(info);
		}

		break;
	}
	case ITCH41_MSG_ORDER_CANCEL: {
		struct itch41_msg_order_cancel *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;
		uint32_t shares;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);
		shares		= be32_to_cpu(m->CanceledShares);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, shares);

		event = (struct ob_event) {
			.type		= OB_EVENT_CANCEL_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
		};

		ob_write_event(session->out_fd, &event);

		assert(info->remaining >= shares);

		info->remaining -= shares;

		break;
	}
	case ITCH41_MSG_ORDER_DELETE: {
		struct itch41_msg_order_delete *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t order_ref_num;

		assert(info->remaining > 0);

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		order_ref_num	= be64_to_cpu(m->OrderReferenceNumber);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, order_ref_num);
		fmt_quantity (&n_event, info->remaining);

		event = (struct ob_event) {
			.type		= OB_EVENT_CANCEL_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
		};

		ob_write_event(session->out_fd, &event);

		if (!g_hash_table_remove(session->order_hash, &info->order_ref_num))
			assert(0);

		free(info);

		break;
	}
	case ITCH41_MSG_ORDER_REPLACE: {
		struct itch41_msg_order_replace *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t orig_ref_num;
		uint64_t new_ref_num;
		uint32_t shares;
		uint32_t price;
		char side;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		orig_ref_num	= be64_to_cpu(m->OriginalOrderReferenceNumber);
		new_ref_num	= be64_to_cpu(m->NewOrderReferenceNumber);
		shares		= be32_to_cpu(m->Shares);
		price		= be32_to_cpu(m->Price);
		side		= info->side;

		/*
		 * Cancel order:
		 */

		assert(info->remaining > 0);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, orig_ref_num);
		fmt_quantity (&n_event, info->remaining);

		event = (struct ob_event) {
			.type		= OB_EVENT_CANCEL_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
		};

		ob_write_event(session->out_fd, &event);

		if (!g_hash_table_remove(session->order_hash, &info->order_ref_num))
			assert(0);

		free(info);

		/*
		 * Add order:
		 */

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_order_id (&n_event, new_ref_num);
		fmt_quantity (&n_event, shares);
		fmt_price    (&n_event, price);

		info = malloc(sizeof(*info));
		info->order_ref_num	= new_ref_num;
		info->remaining		= shares;
		info->price		= price;
		info->side		= side;

		g_hash_table_insert(session->order_hash, &info->order_ref_num, info);

		event = (struct ob_event) {
			.type		= OB_EVENT_ADD_ORDER,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.order_id	= n_event.order_id,
			.order_id_len	= n_event.order_id_len,
			.side		= &info->side,
			.side_len	= sizeof(info->side),
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price + 6,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case ITCH41_MSG_TRADE: {
		struct itch41_msg_trade *m = (void *) msg;
		uint64_t timestamp_nsec;
		uint64_t match_num;
		uint32_t shares;
		uint32_t price;

		timestamp_nsec	= nasdaq_itch_timestamp(session, be32_to_cpu(m->TimestampNanoseconds));
		shares		= be32_to_cpu(m->Shares);
		match_num	= be64_to_cpu(m->MatchNumber);
		price		= be32_to_cpu(m->Price);

		fmt_timestamp(&n_event, timestamp_nsec);
		fmt_quantity (&n_event, shares);
		fmt_exec_id  (&n_event, match_num);
		fmt_price    (&n_event, price);

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.exec_id	= n_event.exec_id,
			.exec_id_len	= n_event.exec_id_len,
			.quantity	= n_event.quantity,
			.quantity_len	= n_event.quantity_len,
			.price		= (struct decimal) {
				.integer	= n_event.price,
				.integer_len	= 6,
				.fraction	= n_event.price,
				.fraction_len	= 4,
			},
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	case ITCH41_MSG_BROKEN_TRADE: {
		struct itch41_msg_broken_trade *m = (void *) msg;
		uint64_t match_num;

		match_num = be64_to_cpu(m->MatchNumber);

		fmt_exec_id(&n_event, match_num);

		event = (struct ob_event) {
			.type		= OB_EVENT_TRADE_BREAK,
			.time		= n_event.timestamp,
			.time_len	= n_event.timestamp_len,
			.exchange	= session->exchange,
			.exchange_len	= session->exchange_len,
			.symbol		= session->symbol,
			.symbol_len	= session->symbol_len,
			.exec_id	= n_event.exec_id,
			.exec_id_len	= n_event.exec_id_len,
		};

		ob_write_event(session->out_fd, &event);

		break;
	}
	default:
		/* Ignore */
		break;
	}
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void nasdaq_itch_ob(struct nasdaq_itch_session *session)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct ob_event event;
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

	event = (struct ob_event) {
		.type		= OB_EVENT_DATE,
		.date		= session->date,
		.date_len	= session->date_len,
		.time_zone	= session->time_zone,
		.time_zone_len	= session->time_zone_len,
		.exchange	= session->exchange,
		.exchange_len	= session->exchange_len,
	};

	ob_write_event(session->out_fd, &event);

	for (;;) {
		struct itch41_message *msg;
		int err;

		err = nasdaq_itch_read(&stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		nasdaq_itch_write(session, msg);
	}

	g_hash_table_destroy(session->order_hash);

	g_hash_table_foreach_remove(session->exec_hash, free_entry, NULL);

	g_hash_table_destroy(session->exec_hash);

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
