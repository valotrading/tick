#ifndef TICK_NASDAQ_ITCH41_H
#define TICK_NASDAQ_ITCH41_H

struct itch41_message;
struct stream;

int nasdaq_itch41_read(struct stream *stream, struct itch41_message **msg_p);

#endif
