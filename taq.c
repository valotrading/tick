#include "tick/taq.h"

#include "tick/types.h"
#include "tick/dsv.h"

#include <unistd.h>

static const char *column_names[] = {
	"Event",
	"Date",
	"Time",
	"TimeZone",
	"Exchange",
	"Symbol",
	"ExecID",
	"TradeQuantity",
	"TradePrice",
	"TradeSide",
	"TradeType",
	"BidQuantity1",
	"BidPrice1",
	"AskQuantity1",
	"AskPrice1",
};

void taq_write_header(int fd)
{
	dsv_write_header(fd, column_names, ARRAY_SIZE(column_names), '\t');
}

void taq_write_event(int fd, struct taq_event *event)
{
	size_t idx = 0;
	char buf[1024];

	idx += dsv_fmt_char   (buf + idx, event->type, '\t');
	idx += dsv_fmt_value  (buf + idx, event->date, event->date_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->time, event->time_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->time_zone, event->time_zone_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->exchange, event->exchange_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->symbol, event->symbol_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->exec_id, event->exec_id_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->trade_quantity, event->trade_quantity_len, '\t');
	idx += dsv_fmt_decimal(buf + idx, &event->trade_price, '\t');
	idx += dsv_fmt_value  (buf + idx, event->trade_side, event->trade_side_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->trade_type, event->trade_type_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->bid_quantity1, event->bid_quantity1_len, '\t');
	idx += dsv_fmt_decimal(buf + idx, &event->bid_price1, '\t');
	idx += dsv_fmt_value  (buf + idx, event->ask_quantity1, event->ask_quantity1_len, '\t');
	idx += dsv_fmt_decimal(buf + idx, &event->ask_price1, '\t');
	idx += dsv_fmt_value  (buf + idx, event->status, event->status_len, '\n');

	write(fd, buf, idx);
}
