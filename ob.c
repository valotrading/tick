#include "ob.h"

#include "types.h"
#include "dsv.h"

#include <stddef.h>
#include <string.h>
#include <unistd.h>

static const char *column_names[] = {
	"Event",
	"Date",
	"Time",
	"TimeZone",
	"Exchange",
	"Symbol",
	"OrderID",
	"ExecID",
	"Side",
	"Quantity",
	"Price",
	"Status",
};

void ob_write_header(int fd)
{
	dsv_write_header(fd, column_names, ARRAY_SIZE(column_names), '\t');
}

static size_t fmt_price(char *buf, struct ob_price *price, char sep)
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

void ob_write_event(int fd, struct ob_event *event)
{
	size_t idx = 0;
	char buf[1024];

	idx += dsv_fmt_char (buf + idx, event->type, '\t');
	idx += dsv_fmt_value(buf + idx, event->date, event->date_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->time, event->time_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->time_zone, event->time_zone_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->exchange, event->exchange_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->symbol, event->symbol_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->order_id, event->order_id_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->exec_id, event->exec_id_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->side, event->side_len, '\t');
	idx += dsv_fmt_value(buf + idx, event->quantity, event->quantity_len, '\t');
	idx += fmt_price(buf + idx, &event->price, '\t');
	idx += dsv_fmt_value(buf + idx, event->status, event->status_len, '\n');

	write(fd, buf, idx);
}
