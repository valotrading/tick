#include "taq.h"

#include "types.h"

#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

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
	unsigned long idx = 0;
	unsigned int i;
	char buf[1024];

	for (i = 0; i < ARRAY_SIZE(column_names); i++) {
		idx += snprintf(buf + idx, sizeof(buf) - idx, "%s", column_names[i]);

		if (i < ARRAY_SIZE(column_names)-1)
			idx += snprintf(buf + idx, sizeof(buf) - idx, "\t");
	}

	idx += snprintf(buf + idx, sizeof(buf) - idx, "\n");

	write(fd, buf, strlen(buf));
}

static size_t fmt_char(char *buf, unsigned char c, char sep)
{
	size_t ret = 0;

	buf[ret++] = c;
	buf[ret++] = sep;

	return ret;
}

static size_t fmt_price(char *buf, struct taq_price *price, char sep)
{
	size_t ret = 0;

	if (price->integer) {
		memcpy(buf, price->integer, price->integer_len);

		ret += price->integer_len;

		buf[ret++] = '.';

		memcpy(buf + ret, price->fraction, price->fraction_len);

		ret += price->fraction_len;
	}

	buf[ret++] = sep;

	return ret;
}

static size_t fmt_value(char *buf, const char *value, size_t len, char sep)
{
	size_t ret = 0;

	if (value) {
		memcpy(buf, value, len);

		ret += len;
	}

	buf[ret++] = sep;

	return ret;
}

void taq_write_event(int fd, struct taq_event *event)
{
	size_t idx = 0;
	char buf[1024];

	idx += fmt_char (buf + idx, event->type, '\t');
	idx += fmt_value(buf + idx, event->date, event->date_len, '\t');
	idx += fmt_value(buf + idx, event->time, event->time_len, '\t');
	idx += fmt_value(buf + idx, event->time_zone, event->time_zone_len, '\t');
	idx += fmt_value(buf + idx, event->exchange, event->exchange_len, '\t');
	idx += fmt_value(buf + idx, event->symbol, event->symbol_len, '\t');
	idx += fmt_value(buf + idx, event->exec_id, event->exec_id_len, '\t');
	idx += fmt_value(buf + idx, event->trade_quantity, event->trade_quantity_len, '\t');
	idx += fmt_price(buf + idx, &event->trade_price, '\t');
	idx += fmt_value(buf + idx, event->trade_side, event->trade_side_len, '\t');
	idx += fmt_value(buf + idx, event->trade_type, event->trade_type_len, '\t');
	idx += fmt_value(buf + idx, event->bid_quantity1, event->bid_quantity1_len, '\t');
	idx += fmt_price(buf + idx, &event->bid_price1, '\t');
	idx += fmt_value(buf + idx, event->ask_quantity1, event->ask_quantity1_len, '\t');
	idx += fmt_price(buf + idx, &event->ask_price1, '\t');
	idx += fmt_value(buf + idx, event->status, event->status_len, '\n');

	write(fd, buf, idx);
}
