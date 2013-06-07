#include "bats-stat.h"

#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/buffer.h"

#include "bats-pitch112.h"
#include "format.h"
#include "stream.h"
#include "error.h"
#include "stats.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

void bats_pitch112_print_stats(struct stats *stats)
{
	printf(" Message type stats for '%s':\n\n", stats->filename);

	print_stat(stats, PITCH_MSG_SYMBOL_CLEAR,	"Symbol Clear");
	print_stat(stats, PITCH_MSG_ADD_ORDER_SHORT,	"Add Order (short)");
	print_stat(stats, PITCH_MSG_ADD_ORDER_LONG,	"Add Order (long)");
	print_stat(stats, PITCH_MSG_ORDER_EXECUTED,	"Order Executed");
	print_stat(stats, PITCH_MSG_ORDER_CANCEL,	"Order Cancel");
	print_stat(stats, PITCH_MSG_TRADE_SHORT,	"Trade (short)");
	print_stat(stats, PITCH_MSG_TRADE_LONG,		"Trade (long)");
	print_stat(stats, PITCH_MSG_TRADE_BREAK,	"Trade Break");
	print_stat(stats, PITCH_MSG_TRADING_STATUS,	"Trading Status");
	print_stat(stats, PITCH_MSG_AUCTION_UPDATE,	"Auction Update");
	print_stat(stats, PITCH_MSG_AUCTION_SUMMARY,	"Auction Summary");

	printf("\n");
}

static void print_progress(struct buffer *buf)
{
	fprintf(stderr, "Processing messages: %3u%%\r", (unsigned int)(buf->start * 100 / buf->capacity));

	fflush(stderr);
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void bats_pitch112_stat(struct stats *stats, int fd, z_stream *zstream)
{
	struct buffer *comp_buf, *uncomp_buf;
	struct stream stream;
	struct stat st;

	if (fstat(fd, &st) < 0)
		error("%s: %s", stats->filename, strerror(errno));

	comp_buf = buffer_mmap(fd, st.st_size);
	if (!comp_buf)
		error("%s: %s", stats->filename, strerror(errno));

	zstream->next_in = (void *) buffer_start(comp_buf);

	uncomp_buf = buffer_new(BUFFER_SIZE);
	if (!uncomp_buf)
		error("%s", strerror(errno));

	stream = (struct stream) {
		.zstream	= zstream,
		.uncomp_buf	= uncomp_buf,
		.comp_buf	= comp_buf,
		.progress	= print_progress,
	};

	for (;;) {
		struct pitch_message *msg;
		int err;

		err = bats_pitch112_read(&stream, &msg);
		if (err)
			error("%s: %s", stats->filename, strerror(err));

		if (!msg)
			break;

		stats->stats[msg->MessageType]++;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
