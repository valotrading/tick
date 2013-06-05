#ifndef TICK_TAQ_H
#define TICK_TAQ_H

#include "decimal.h"

/*
 * TAQ format
 */

enum taq_event_type {
	TAQ_EVENT_DATE		= 'D',
	TAQ_EVENT_TRADE		= 'T',
	TAQ_EVENT_TRADE_BREAK	= 'B',
	TAQ_EVENT_QUOTE		= 'Q',
	TAQ_EVENT_STATUS	= 'S',
};

struct taq_event {
	enum taq_event_type	type;
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
	const char		*exec_id;
	unsigned long		exec_id_len;
	const char		*trade_quantity;
	unsigned long		trade_quantity_len;
	struct decimal		trade_price;
	const char		*trade_side;
	unsigned long		trade_side_len;
	const char		*trade_type;
	unsigned long		trade_type_len;
	const char		*bid_quantity1;
	unsigned long		bid_quantity1_len;
	struct decimal		bid_price1;
	const char		*ask_quantity1;
	unsigned long		ask_quantity1_len;
	struct decimal		ask_price1;
	const char		*status;
	unsigned long		status_len;
};

void taq_write_header(int fd);
void taq_write_event(int fd, struct taq_event *event);

#endif
