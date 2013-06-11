#ifndef TICK_FORMAT_H
#define TICK_FORMAT_H

enum format {
	FORMAT_BATS_PITCH_112,
	FORMAT_NASDAQ_ITCH_41,
	FORMAT_NYSE_TAQ_17,
};

extern const char *format_names[];

enum format parse_format(const char *name);

#endif
