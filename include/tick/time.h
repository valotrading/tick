#ifndef TICK_TIME_H
#define TICK_TIME_H

enum time_unit {
	TIME_UNIT_NANOSECONDS,
	TIME_UNIT_MILLISECONDS,
};

struct time {
	const char		*value;
	unsigned long		value_len;
	enum time_unit		unit;
};

#endif
