#include "dsv.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

void dsv_write_header(int fd, const char *columns[], size_t num_columns, char delimiter)
{
	unsigned long idx = 0;
	unsigned int i;
	char buf[1024];

	for (i = 0; i < num_columns; i++) {
		idx += snprintf(buf + idx, sizeof(buf) - idx, "%s", columns[i]);

		if (i < num_columns - 1)
			buf[idx++] = delimiter;
	}

	buf[idx++] = '\n';
	buf[idx++] = '\0';

	printf("buf: \"%s\"\n", buf);

	write(fd, buf, strlen(buf));
}
