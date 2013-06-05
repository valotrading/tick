#ifndef TICK_DSV_H
#define TICK_DSV_H

#include <stdlib.h>

void dsv_write_header(int fd, const char *columns[], size_t num_columns, char delimiter);

#endif
