#include "tick/format.h"

#include "tick/types.h"

#include <string.h>

const char *format_names[] = {
	[FORMAT_BATS_PITCH_112]	= "bats-pitch-1.12",
	[FORMAT_NASDAQ_ITCH_41]	= "nasdaq-itch-4.1",
	[FORMAT_NYSE_TAQ_17]	= "nyse-taq-1.7",
};

enum format parse_format(const char *name)
{
	unsigned int i;

	for (i = 0; i < ARRAY_SIZE(format_names); i++) {
		if (!strcmp(name, format_names[i]))
			return i;
	}

	return -1;
}
