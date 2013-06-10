#ifndef TICK_BATS_PITCH112_H
#define TICK_BATS_PITCH112_H

#include <glib.h>
#include <zlib.h>

struct pitch_message;
struct stream;

struct pitch_filter {
	char			symbol[6];
};

struct pitch_session {
	struct pitch_filter	filter;
	z_stream		*zstream;
	int			in_fd;
	int			out_fd;
	const char		*input_filename;
	const char		*exchange;
	unsigned long		exchange_len;
	const char		*time_zone;
	unsigned long		time_zone_len;
	const char		*symbol;
	unsigned long		symbol_len;
	GHashTable		*order_hash;
	GHashTable		*exec_hash;
};

int bats_pitch_read(struct stream *stream, struct pitch_message **msg_p);
int pitch_file_parse_date(const char *filename, char *buf, size_t buf_len);
void pitch_filter_init(struct pitch_filter *filter, const char *symbol);
void bats_pitch_ob(struct pitch_session *session);

#endif
