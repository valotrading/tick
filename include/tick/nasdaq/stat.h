#ifndef TICK_NASDAQ_STAT_H
#define TICK_NASDAQ_STAT_H

#include <zlib.h>

struct stats;

void nasdaq_itch_print_stats(struct stats *stats);
void nasdaq_itch_stat(struct stats *stats, int fd, z_stream *zstream);

#endif
