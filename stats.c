#include "tick/stats.h"

#include <ctype.h>
#include <stdio.h>

void print_stat(struct stats *stats, u8 msg_type, const char *name)
{
	if (isprint(msg_type))
		fprintf(stdout, "%'14.0f  %-30s # '%c'\n", (double) stats->stats[msg_type], name, msg_type);
	else
		fprintf(stdout, "%'14.0f  %-30s # 0x%02x\n", (double) stats->stats[msg_type], name, msg_type);
}

void print_stats(struct stats *stats, const char **stat_names, size_t stat_len)
{
	unsigned int i;

	printf(" Message type stats for '%s':\n\n", stats->filename);

	for (i = 0; i < stat_len; i++) {
		const char *name = stat_names[i];

		if (name) {
			print_stat(stats, i, name);
		} else if (stats->stats[i]) {
			print_stat(stats, i, "<unknown>");
		}
	}

	printf("\n");
}
