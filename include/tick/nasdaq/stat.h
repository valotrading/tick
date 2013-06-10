#ifndef TICK_NASDAQ_STAT_H
#define TICK_NASDAQ_STAT_H

#include <zlib.h>

struct stats;

void nasdaq_itch41_print_stats(struct stats *stats);
void nasdaq_itch41_stat(struct stats *stats, int fd, z_stream *zstream);

#endif
