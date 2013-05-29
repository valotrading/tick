#ifndef TICK_DSV_H
#define TICK_DSV_H

#include "decimal.h"

#include <stdlib.h>
#include <string.h>

static inline size_t dsv_fmt_char(char *buf, unsigned char c, char delim)
{
	size_t ret = 0;

	buf[ret++] = c;
	buf[ret++] = delim;

	return ret;
}

static inline size_t dsv_fmt_decimal(char *buf, struct decimal *decimal, char delim)
{
	size_t ret = 0;

	if (decimal->integer) {
		memcpy(buf, decimal->integer, decimal->integer_len);

		ret += decimal->integer_len;

		buf[ret++] = '.';

		memcpy(buf + ret, decimal->fraction, decimal->fraction_len);

		ret += decimal->fraction_len;
	}

	buf[ret++] = delim;

	return ret;
}

static inline size_t dsv_fmt_value(char *buf, const char *value, size_t len, char delim)
{
	size_t ret = 0;

	if (value) {
		memcpy(buf, value, len);

		ret += len;
	}

	buf[ret++] = delim;

	return ret;
}

void dsv_write_header(int fd, const char *columns[], size_t num_columns, char delim);

#endif
