#include "dsv.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

void dsv_write_header(int fd, const char *columns[], size_t nr_columns, char delim)
{
	unsigned long idx = 0;
	unsigned int i;
	char buf[1024];

	for (i = 0; i < nr_columns; i++) {
		idx += snprintf(buf + idx, sizeof(buf) - idx, "%s", columns[i]);

		if (i < nr_columns - 1)
			buf[idx++] = delim;
	}

	buf[idx++] = '\n';
	buf[idx++] = '\0';

	write(fd, buf, strlen(buf));
}
