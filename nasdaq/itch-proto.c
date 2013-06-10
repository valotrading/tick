#include "tick/nasdaq/itch-proto.h"

#include "tick/stream.h"

#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/buffer.h"

#include <errno.h>

int nasdaq_itch_read(struct stream *stream, struct itch41_message **msg_p)
{
	struct itch41_message *msg;

	*msg_p = NULL;

retry_size:
	if (buffer_size(stream->uncomp_buf) < sizeof(u16)) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr < 0)
			return nr;

		if (!nr)
			return 0;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_size;
	}

	buffer_advance(stream->uncomp_buf, sizeof(u16));

retry_message:
	msg = itch41_message_decode(stream->uncomp_buf);
	if (!msg) {
		ssize_t nr;

		buffer_compact(stream->uncomp_buf);

		nr = buffer_inflate(stream->comp_buf, stream->uncomp_buf, stream->zstream);
		if (nr < 0)
			return nr;

		if (!nr)
			return 0;

		if (stream->progress)
			stream->progress(stream->comp_buf);

		goto retry_message;
	}

	*msg_p = msg;

	return 0;
}
