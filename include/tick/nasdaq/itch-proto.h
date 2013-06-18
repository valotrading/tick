#ifndef TICK_NASDAQ_ITCH41_H
#define TICK_NASDAQ_ITCH41_H

#include <stdbool.h>
#include <stdint.h>
#include <glib.h>
#include <zlib.h>

struct itch41_message;
struct stream;

struct nasdaq_itch_filter {
	char			symbol[8];
};

struct nasdaq_itch_session {
	struct nasdaq_itch_filter	filter;
	z_stream			*zstream;
	int				in_fd;
	int				out_fd;
	const char			*input_filename;
	const char			*date;
	unsigned long			date_len;
	const char			*exchange;
	unsigned long			exchange_len;
	const char			*time_zone;
	unsigned long			time_zone_len;
	const char			*symbol;
	unsigned long			symbol_len;
	unsigned long			second;
	GHashTable			*order_hash;
	GHashTable			*exec_hash;
};

struct nasdaq_itch_exec_info {
	uint64_t		exec_id;
	const char		*symbol;
};

struct nasdaq_itch_order_info {
	uint64_t		order_ref_num;
	uint32_t		remaining;
	uint32_t		price;
	char			side;
};

int nasdaq_itch_read(struct stream *stream, struct itch41_message **msg_p);
int nasdaq_itch_file_parse_date(const char *filename, char *buf, size_t buf_len);
void nasdaq_itch_filter_init(struct nasdaq_itch_filter *filter, const char *symbol);
void nasdaq_itch_ob(struct nasdaq_itch_session *session);
void nasdaq_itch_taq(struct nasdaq_itch_session *session);
struct nasdaq_itch_order_info *nasdaq_itch_session_lookup_order(struct nasdaq_itch_session *session, struct itch41_message *msg);
bool nasdaq_itch_session_filter_msg(struct nasdaq_itch_session *session, struct itch41_message *msg);

#endif
