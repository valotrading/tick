#include "tick/bats/stat.h"

#include "libtrading/proto/bats_pitch_message.h"
#include "libtrading/buffer.h"

#include "tick/bats/pitch-proto.h"
#include "tick/progress.h"
#include "tick/format.h"
#include "tick/stream.h"
#include "tick/error.h"
#include "tick/stats.h"
#include "tick/types.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

static const char *bats_stat_names[256] = {
	[PITCH_MSG_SYMBOL_CLEAR]	= "Symbol Clear",
	[PITCH_MSG_ADD_ORDER_SHORT]	= "Add Order (short)",
	[PITCH_MSG_ADD_ORDER_LONG]	= "Add Order (long)",
	[PITCH_MSG_ORDER_EXECUTED]	= "Order Executed",
	[PITCH_MSG_ORDER_CANCEL]	= "Order Cancel",
	[PITCH_MSG_TRADE_SHORT]		= "Trade (short)",
	[PITCH_MSG_TRADE_LONG]		= "Trade (long)",
	[PITCH_MSG_TRADE_BREAK]		= "Trade Break",
	[PITCH_MSG_TRADING_STATUS]	= "Trading Status",
	[PITCH_MSG_AUCTION_UPDATE]	= "Auction Update",
	[PITCH_MSG_AUCTION_SUMMARY]	= "Auction Summary",
};

void bats_pitch_print_stats(struct stats *stats)
{
	print_stats(stats, bats_stat_names, ARRAY_SIZE(bats_stat_names));
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void bats_pitch_stat(struct stats *stats, int fd, z_stream *zstream)
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

		err = bats_pitch_read(&stream, &msg);
		if (err)
			error("%s: %s", stats->filename, strerror(err));

		if (!msg)
			break;

		stats->stats[msg->MessageType]++;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
