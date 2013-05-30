#ifndef TICK_BATS_PITCH112_H
#define TICK_BATS_PITCH112_H

struct pitch_message;
struct stream;

int bats_pitch112_read(struct stream *stream, struct pitch_message **msg_p);

#endif
