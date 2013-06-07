#include "stats.h"

#include <stdio.h>

void print_stat(struct stats *stats, u8 msg_type, const char *name)
{
	fprintf(stdout, "%'14.0f  %s\n", (double) stats->stats[msg_type], name);
}
