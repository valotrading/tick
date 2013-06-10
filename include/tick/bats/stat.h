#ifndef TICK_BATS_STAT_H
#define TICK_BATS_STAT_H

#include <zlib.h>

struct stats;

void bats_pitch112_print_stats(struct stats *stats);
void bats_pitch112_stat(struct stats *stats, int fd, z_stream *zstream);

#endif
