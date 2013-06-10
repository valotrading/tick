#ifndef TICK_STREAM_H
#define TICK_STREAM_H

#include <zlib.h>

struct buffer;

struct stream {
	z_stream		*zstream;
	struct buffer		*uncomp_buf;
	struct buffer		*comp_buf;
	void			(*progress)(struct buffer *);
};

#endif
