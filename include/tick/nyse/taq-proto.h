#ifndef TICK_NYSE_TAQ_PROTO_H
#define TICK_NYSE_TAQ_PROTO_H

#include <stddef.h>
#include <zlib.h>

struct nyse_taq_filter {
	char			symbol[6];
};

void nyse_taq_filter_init(struct nyse_taq_filter *filter, const char *symbol);

struct stream;
struct nyse_taq_msg_daily_trade;

int nyse_taq_msg_daily_trade_read(struct stream *stream,
	struct nyse_taq_msg_daily_trade **msg_p);

struct nyse_taq_session {
	struct nyse_taq_filter	filter;
	z_stream		*zstream;
	int			in_fd;
	int			out_fd;
	const char		*input_filename;
	const char		*date;
	const char		*time_zone;
	size_t			time_zone_len;
};

int nyse_taq_parse_date(struct stream *stream, char *buf, size_t len);

void nyse_taq_taq(struct nyse_taq_session *);

#endif
