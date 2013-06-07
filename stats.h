#ifndef TICK_STATS_H
#define TICK_STATS_H

#include <libtrading/types.h>

#include <stddef.h>
#include <stdint.h>

struct stats {
	const char	*filename;
	uint64_t	stats[256];
};

void print_stat(struct stats *stats, u8 msg_type, const char *name);
void print_stats(struct stats *stats, const char **stat_names, size_t stat_len);

#endif
