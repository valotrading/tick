#ifndef TICK_DSV_H
#define TICK_DSV_H

#include <stdlib.h>

static inline size_t dsv_fmt_char(char *buf, unsigned char c, char delim)
{
	size_t ret = 0;

	buf[ret++] = c;
	buf[ret++] = delim;

	return ret;
}

void dsv_write_header(int fd, const char *columns[], size_t num_columns, char delim);

#endif
