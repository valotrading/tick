#ifndef TICK_BATS_PITCH112_H
#define TICK_BATS_PITCH112_H

#include <stdbool.h>
#include <stdint.h>
#include <glib.h>
#include <zlib.h>

struct pitch_message;
struct stream;

#define PITCH_PRICE_INT_LEN		6
#define PITCH_PRICE_FRACTION_LEN	4

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

struct pitch_exec_info {
	uint64_t		exec_id;
	const char		*symbol;
};

struct pitch_order_info {
	uint64_t		order_id;
	uint32_t		remaining;
	char			price[10];
};

int bats_pitch_read(struct stream *stream, struct pitch_message **msg_p);
int pitch_file_parse_date(const char *filename, char *buf, size_t buf_len);
void pitch_filter_init(struct pitch_filter *filter, const char *symbol);
void bats_pitch_ob(struct pitch_session *session);
void bats_pitch_taq(struct pitch_session *session);
struct pitch_order_info *pitch_session_lookup_order(struct pitch_session *session, struct pitch_message *msg);
bool pitch_session_filter_msg(struct pitch_session *session, struct pitch_message *msg);

#endif
