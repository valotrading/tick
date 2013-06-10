#ifndef TICK_BASE36_H
#define TICK_BASE36_H

#include <stddef.h>
#include <stdint.h>

#define BASE36_DIGIT(ch) ((ch) - '0')

extern unsigned char base36_table[];

static inline unsigned long base36_pos(unsigned char ch)
{
	return base36_table[BASE36_DIGIT(ch)];
}

static inline uint64_t base36_decode(const char *s, size_t len)
{
	uint64_t ret = 0;
	unsigned int i;

	for (i = 0; i < len; i++) {
		char ch = s[i];

		ret *= 36;

		ret += base36_pos(ch);
	}

	return ret;
}

#endif
