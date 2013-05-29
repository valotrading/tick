#ifndef TICK_OB_H
#define TICK_OB_H

#include "decimal.h"

/*
 * OB format
 */

enum ob_event_type {
	OB_EVENT_DATE		= 'D',
	OB_EVENT_ADD_ORDER	= 'A',
	OB_EVENT_CANCEL_ORDER	= 'X',
	OB_EVENT_EXECUTE_ORDER	= 'E',
	OB_EVENT_CLEAR		= 'C',
	OB_EVENT_TRADE		= 'T',
	OB_EVENT_STATUS		= 'S',
};

struct ob_event {
	enum ob_event_type	type;
	const char		*date;
	unsigned long		date_len;
	const char		*time;
	unsigned long		time_len;
	const char		*time_zone;
	unsigned long		time_zone_len;
	const char		*exchange;
	unsigned long		exchange_len;
	const char		*symbol;
	unsigned long		symbol_len;
	const char		*order_id;
	unsigned long		order_id_len;
	const char		*exec_id;
	unsigned long		exec_id_len;
	const char		*side;
	unsigned long		side_len;
	const char		*quantity;
	unsigned long		quantity_len;
	const char		*status;
	unsigned long		status_len;
	struct decimal		price;
};

void ob_write_header(int fd);
void ob_write_event(int fd, struct ob_event *event);

#endif
