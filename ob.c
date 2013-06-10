#include "tick/ob.h"

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

void ob_write_event(int fd, struct ob_event *event)
{
	size_t idx = 0;
	char buf[1024];

	idx += dsv_fmt_char   (buf + idx, event->type, '\t');
	idx += dsv_fmt_value  (buf + idx, event->date, event->date_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->time, event->time_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->time_zone, event->time_zone_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->exchange, event->exchange_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->symbol, event->symbol_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->order_id, event->order_id_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->exec_id, event->exec_id_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->side, event->side_len, '\t');
	idx += dsv_fmt_value  (buf + idx, event->quantity, event->quantity_len, '\t');
	idx += dsv_fmt_decimal(buf + idx, &event->price, '\t');
	idx += dsv_fmt_value  (buf + idx, event->status, event->status_len, '\n');

	write(fd, buf, idx);
}
