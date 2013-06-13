#include "tick/nyse/taq-proto.h"

#include "tick/progress.h"
#include "tick/stream.h"
#include "tick/error.h"
#include "tick/taq.h"

#include "libtrading/proto/nyse_taq_message.h"
#include "libtrading/buffer.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

enum file_type {
	FILE_TYPE_UNKNOWN,
	FILE_TYPE_DAILY_QUOTE,
	FILE_TYPE_DAILY_TRADE,
};

#define MIC_LEN		4
#define MIC_ID(id)	((unsigned int)(id))

const char *mic[128] = {
	[MIC_ID('A')] = "XASE",	/* NYSE MKT Stock Exchange */
	[MIC_ID('B')] = "XBOS",	/* NASDAQ OMX BX Stock Exchange */
	[MIC_ID('C')] = "XCIS",	/* National Stock Exchange */
	[MIC_ID('D')] = "FINR",	/* FINRA */
	[MIC_ID('I')] = "XISX",	/* International Securities Exchange */
	[MIC_ID('J')] = "EDGA",	/* Direct Edge A Stock Exchange */
	[MIC_ID('K')] = "EDGX",	/* Direct Edge X Stock Exchange */
	[MIC_ID('M')] = "XCHI",	/* Chicago Stock Exchange */
	[MIC_ID('N')] = "XNYS",	/* New York Stock Exchange */
	[MIC_ID('T')] = "XPHL",	/* NASDAQ OMX Stock Exchange */
	[MIC_ID('P')] = "ARCX",	/* NYSE Arca SM */
	[MIC_ID('S')] = "XXXX",	/* Consolidated Tape System */
	[MIC_ID('Q')] = "XNAS",	/* NASDAQ Stock Exchange */
	[MIC_ID('W')] = "XCBO",	/* CBOE Stock Exchange */
	[MIC_ID('X')] = "XPSX",	/* NASDAQ OMX PSX Stock Exchange */
	[MIC_ID('Y')] = "BATY",	/* BATS Y-Exchange */
	[MIC_ID('Z')] = "BATS",	/* BATS Exhange */
};

const char mic_ids[] = "ABCDIJKMNTPSQWXYZ";

static const char *mic_by_index(unsigned int ndx)
{
	return mic[MIC_ID(mic_ids[ndx])];
}

static size_t nr_mic(void)
{
	return strlen(mic_ids);
}

static enum file_type parse_header(struct stream *stream, char *dst, size_t dst_len)
{
	unsigned int count;
	char buf[10];
	int nr;

	if (buffer_size(stream->uncomp_buf) < sizeof(buf)) {
		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr <= 0)
			return FILE_TYPE_UNKNOWN;
	}

	buffer_get_n(stream->uncomp_buf, sizeof(buf), buf);

	count = 0;

	snprintf(dst, dst_len, "%c%c%c%c-%c%c-%c%c",
		buf[6], buf[7], buf[8], buf[9],
		buf[2], buf[3], buf[4], buf[5]);

	while (true) {
		if (buffer_size(stream->uncomp_buf) == 0) {
			buffer_compact(stream->uncomp_buf);

			nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
			if (nr <= 0)
				return FILE_TYPE_UNKNOWN;
		}

		if (buffer_get_8(stream->uncomp_buf) == '\n')
			break;

		count++;
	}

	/* Drop CR and LF. */
	switch (count - 2) {
	case 83: /* Undocumented. */
		return FILE_TYPE_DAILY_QUOTE;
	case 82:
		return FILE_TYPE_DAILY_QUOTE;
	case 60:
		return FILE_TYPE_DAILY_TRADE;
	default:
		return FILE_TYPE_UNKNOWN;
	}
}

int nyse_taq_msg_daily_quote_read(struct stream *stream,
	struct nyse_taq_msg_daily_quote **msg_p)
{
	struct nyse_taq_msg_daily_quote *msg;

	*msg_p = NULL;

retry_message:
	msg = nyse_taq_msg_daily_quote_decode(stream->uncomp_buf);
	if (!msg) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr <= 0)
			return nr;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_message;
	}

	*msg_p = msg;

	return 0;
}

int nyse_taq_msg_daily_trade_read(struct stream *stream,
	struct nyse_taq_msg_daily_trade **msg_p)
{
	struct nyse_taq_msg_daily_trade *msg;

	*msg_p = NULL;

retry_message:
	msg = nyse_taq_msg_daily_trade_decode(stream->uncomp_buf);
	if (!msg) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr <= 0)
			return nr;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_message;
	}

	*msg_p = msg;

	return 0;
}

void nyse_taq_filter_init(struct nyse_taq_filter *filter, const char *symbol)
{
	memset(filter->symbol, ' ', sizeof(filter->symbol));
	memcpy(filter->symbol, symbol, strlen(symbol));
}

static bool nyse_taq_session_filter_msg_daily_quote(struct nyse_taq_session *session,
	struct nyse_taq_msg_daily_quote *msg)
{
	struct nyse_taq_filter *filter = &session->filter;

	return !memcmp(msg->Symbol, filter->symbol, sizeof(filter->symbol));
}

static bool filter_msg_daily_quote_quote_condition(struct nyse_taq_msg_daily_quote *msg)
{
	switch (msg->QuoteCondition) {
	case 'R':
		return true;
	case 'Y':
		return true;
	default:
		return false;
	}
}

static bool nyse_taq_session_filter_msg_daily_trade(struct nyse_taq_session *session,
	struct nyse_taq_msg_daily_trade *msg)
{
	struct nyse_taq_filter *filter = &session->filter;

	return !memcmp(msg->Symbol, filter->symbol, sizeof(filter->symbol));
}

static bool filter_msg_daily_trade_sale_condition(struct nyse_taq_msg_daily_trade *msg)
{
	unsigned int ndx;

	for (ndx = 0; ndx < sizeof(msg->SaleCondition); ndx++) {
		switch (msg->SaleCondition[ndx]) {
		case '@':
			break;
		case ' ':
			break;
		case 'F':
			break;
		default:
			return false;
		}
	}

	return true;
}

static bool filter_msg_daily_trade_trade_correction_indicator(struct nyse_taq_msg_daily_trade *msg)
{
	if (msg->TradeCorrectionIndicator[0] == '1')
		return false;

	if (msg->TradeCorrectionIndicator[1] == '0')
		return true;

	if (msg->TradeCorrectionIndicator[1] == '1')
		return true;

	return false;
}

static const char *trade_type(struct nyse_taq_msg_daily_trade *msg)
{
	unsigned int ndx;

	for (ndx = 0; ndx < sizeof(msg->SaleCondition); ndx++) {
		if (msg->SaleCondition[ndx] == 'F') {
			return TAQ_TRADE_TYPE_INTERMARKET_SWEEP;
		}
	}

	return TAQ_TRADE_TYPE_REGULAR;
}

#define PRICE_INT_LEN		7
#define PRICE_FRACTION_LEN	4

static void nyse_taq_msg_daily_quote_write(struct nyse_taq_session *session,
	struct nyse_taq_msg_daily_quote *msg)
{
	struct taq_event event;

	if (!nyse_taq_session_filter_msg_daily_quote(session, msg))
		return;

	if (!filter_msg_daily_quote_quote_condition(msg))
		return;

	event = (struct taq_event) {
		.type			= TAQ_EVENT_QUOTE,
		.time			= (struct time) {
			.value		= msg->Time,
			.value_len	= sizeof(msg->Time),
			.unit		= TIME_UNIT_MILLISECONDS,
		},
		.exchange		= mic[MIC_ID(msg->Exchange)],
		.exchange_len		= MIC_LEN,
		.symbol			= msg->Symbol,
		.symbol_len		= sizeof(msg->Symbol),
		.bid_quantity1		= msg->BidSize,
		.bid_quantity1_len	= sizeof(msg->BidSize),
		.bid_price1		= (struct decimal) {
			.integer	= msg->BidPrice,
			.integer_len	= PRICE_INT_LEN,
			.fraction	= msg->BidPrice + PRICE_INT_LEN,
			.fraction_len	= PRICE_FRACTION_LEN,
		},
		.ask_quantity1		= msg->AskSize,
		.ask_quantity1_len	= sizeof(msg->AskSize),
		.ask_price1		= (struct decimal) {
			.integer	= msg->AskPrice,
			.integer_len	= PRICE_INT_LEN,
			.fraction	= msg->AskPrice + PRICE_INT_LEN,
			.fraction_len	= PRICE_FRACTION_LEN,
		},
		.status_len		= 0,
	};

	taq_write_event(session->out_fd, &event);
}

static void nyse_taq_msg_daily_trade_write(struct nyse_taq_session *session,
	struct nyse_taq_msg_daily_trade *msg)
{
	struct taq_event event;

	if (!nyse_taq_session_filter_msg_daily_trade(session, msg))
		return;

	if (!filter_msg_daily_trade_sale_condition(msg))
		return;

	if (!filter_msg_daily_trade_trade_correction_indicator(msg))
		return;

	event = (struct taq_event) {
		.type 			= TAQ_EVENT_TRADE,
		.time			= (struct time) {
			.value		= msg->Time,
			.value_len	= sizeof(msg->Time),
			.unit		= TIME_UNIT_MILLISECONDS,
		},
		.exchange		= mic[MIC_ID(msg->Exchange)],
		.exchange_len		= MIC_LEN,
		.symbol			= msg->Symbol,
		.symbol_len		= sizeof(msg->Symbol),
		.trade_quantity		= msg->TradeVolume,
		.trade_quantity_len	= sizeof(msg->TradeVolume),
		.trade_price		= (struct decimal) {
			.integer	= msg->TradePrice,
			.integer_len	= PRICE_INT_LEN,
			.fraction	= msg->TradePrice + PRICE_INT_LEN,
			.fraction_len	= PRICE_FRACTION_LEN,
		},
		.trade_type		= trade_type(msg),
		.trade_type_len		= TAQ_TRADE_TYPE_LEN,
	};

	taq_write_event(session->out_fd, &event);
}

static void process_daily_quotes(struct nyse_taq_session *session,
	struct stream *stream)
{
	for (;;) {
		struct nyse_taq_msg_daily_quote *msg;
		int err;

		err = nyse_taq_msg_daily_quote_read(stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		nyse_taq_msg_daily_quote_write(session, msg);
	}
}

static void process_daily_trades(struct nyse_taq_session *session,
	struct stream *stream)
{
	for (;;) {
		struct nyse_taq_msg_daily_trade *msg;
		int err;

		err = nyse_taq_msg_daily_trade_read(stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		nyse_taq_msg_daily_trade_write(session, msg);
	}
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void nyse_taq_taq(struct nyse_taq_session *session)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;
	char date_buf[11];
	unsigned int ndx;
	struct taq_event event;
	enum file_type file_type;

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

	file_type = parse_header(&stream, date_buf, sizeof(date_buf));
	if (file_type == FILE_TYPE_UNKNOWN)
		error("%s: Unknown file type", session->input_filename);

	if (!session->date)
		session->date = date_buf;

	for (ndx = 0; ndx < nr_mic(); ndx++) {
		event = (struct taq_event) {
			.type		= TAQ_EVENT_DATE,
			.date		= session->date,
			.date_len	= strlen(session->date),
			.time_zone	= session->time_zone,
			.time_zone_len	= session->time_zone_len,
			.exchange	= mic_by_index(ndx),
			.exchange_len	= strlen(mic_by_index(ndx)),
		};

		taq_write_event(session->out_fd, &event);
	}

	switch (file_type) {
	case FILE_TYPE_DAILY_QUOTE:
		process_daily_quotes(session, &stream);
		break;
	case FILE_TYPE_DAILY_TRADE:
		process_daily_trades(session, &stream);
		break;
	case FILE_TYPE_UNKNOWN:
		break;
	default:
		break;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
