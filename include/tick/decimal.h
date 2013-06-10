#ifndef TICK_DECIMAL_H
#define TICK_DECIMAL_H

struct decimal {
	const char		*integer;
	unsigned long		integer_len;
	const char		*fraction;
	unsigned long		fraction_len;
};

#endif
