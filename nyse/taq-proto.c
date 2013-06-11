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
	[MIC_ID('P')] = "XARC",	/* NYSE Arca SM */
	[MIC_ID('S')] = "XXXX",	/* Consolidated Tape System */
	[MIC_ID('Q')] = "XNAS",	/* NASDAQ Stock Exchange */
	[MIC_ID('W')] = "XCBO",	/* CBOE Stock Exchange */
	[MIC_ID('X')] = "XPSX",	/* NASDAQ OMX PSX Stock Exchange */
	[MIC_ID('Y')] = "BATY",	/* BATS Y-Exchange */
	[MIC_ID('Z')] = "BATS",	/* BATS Exhange */
};

const char mic_ids[] = "ABCDIJKMNTPSQWXYZ";

const char *nyse_taq_mic(unsigned int ndx)
{
	return mic[MIC_ID(mic_ids[ndx])];
}

size_t nyse_taq_nr_mic(void)
{
	return strlen(mic_ids);
}

static void nyse_taq_record_count_skip(struct stream *stream)
{
	int nr;

	while (true) {
		if (buffer_size(stream->uncomp_buf) == 0) {
			buffer_compact(stream->uncomp_buf);

			nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
			if (nr <= 0)
				return;

			if (stream->progress)
				stream->progress(stream->comp_buf);
		}

		if (buffer_get_8(stream->uncomp_buf) == '\n')
			return;
	}
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

#define TRADE_PRICE_INT_LEN		7
#define TRADE_PRICE_FRACTION_LEN	4

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
		.time 			= msg->Time,
		.time_len		= sizeof(msg->Time),
		.exchange		= mic[MIC_ID(msg->Exchange)],
		.exchange_len		= MIC_LEN,
		.symbol			= msg->Symbol,
		.symbol_len		= sizeof(msg->Symbol),
		.trade_quantity		= msg->TradeVolume,
		.trade_quantity_len	= sizeof(msg->TradeVolume),
		.trade_price		= (struct decimal) {
			.integer	= msg->TradePrice,
			.integer_len	= TRADE_PRICE_INT_LEN,
			.fraction	= msg->TradePrice + TRADE_PRICE_INT_LEN,
			.fraction_len	= TRADE_PRICE_FRACTION_LEN,
		},
		.trade_type		= trade_type(msg),
		.trade_type_len		= TAQ_TRADE_TYPE_LEN,
	};

	taq_write_event(session->out_fd, &event);
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void nyse_taq_taq(struct nyse_taq_session *session)
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

	nyse_taq_record_count_skip(&stream);

	for (;;) {
		struct nyse_taq_msg_daily_trade *msg;
		int err;

		err = nyse_taq_msg_daily_trade_read(&stream, &msg);
		if (err)
			error("%s: %s", session->input_filename, strerror(err));

		if (!msg)
			break;

		nyse_taq_msg_daily_trade_write(session, msg);
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
