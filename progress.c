#include "progress.h"

#include <libtrading/buffer.h>

#include <stdio.h>

void print_progress(struct buffer *buf)
{
	fprintf(stderr, "Processing messages: %3u%%\r", (unsigned int)(buf->start * 100 / buf->capacity));

	fflush(stderr);
}

