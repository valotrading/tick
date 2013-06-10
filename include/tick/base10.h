#ifndef TICK_BASE10_H
#define TICK_BASE10_H

#include <stddef.h>
#include <stdint.h>

static inline uint64_t base10_decode(const char *s, size_t len)
{
	uint64_t ret = 0;
	unsigned int i;

	for (i = 0; i < len; i++) {
		char ch = s[i];

		ret *= 10;

		ret += (ch - '0');
	}

	return ret;
}

#endif
