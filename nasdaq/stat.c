#include "tick/nasdaq/stat.h"

#include "libtrading/proto/nasdaq_itch41_message.h"
#include "libtrading/buffer.h"

#include "tick/nasdaq/itch-proto.h"
#include "tick/progress.h"
#include "tick/stream.h"
#include "tick/error.h"
#include "tick/stats.h"
#include "tick/types.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

static const char *nasdaq_stat_names[256] = {
	[ITCH41_MSG_TIMESTAMP_SECONDS]		= "Timestamp - Seconds",
	[ITCH41_MSG_SYSTEM_EVENT]		= "System Event",
	[ITCH41_MSG_STOCK_DIRECTORY]		= "Stock Directory",
	[ITCH41_MSG_STOCK_TRADING_ACTION]	= "Stock Trading Action",
	[ITCH41_MSG_REG_SHO_RESTRICTION]	= "REG SHO Restriction",
	[ITCH41_MSG_MARKET_PARTICIPANT_POS]	= "Market Participant Position",
	[ITCH41_MSG_ADD_ORDER]			= "Add Order",
	[ITCH41_MSG_ADD_ORDER_MPID]		= "Add Order - MPID Attribution",
	[ITCH41_MSG_ORDER_EXECUTED]		= "Order Executed",
	[ITCH41_MSG_ORDER_EXECUTED_WITH_PRICE]	= "Order Executed With Price",
	[ITCH41_MSG_ORDER_CANCEL]		= "Order Cancel",
	[ITCH41_MSG_ORDER_DELETE]		= "Order Delete",
	[ITCH41_MSG_ORDER_REPLACE]		= "Order Replace",
	[ITCH41_MSG_TRADE]			= "Trade (non-cross)",
	[ITCH41_MSG_CROSS_TRADE]		= "Cross Trade",
	[ITCH41_MSG_BROKEN_TRADE]		= "Broken Trade",
	[ITCH41_MSG_NOII]			= "NOII",
	[ITCH41_MSG_RPII]			= "RPII",
};

void nasdaq_itch41_print_stats(struct stats *stats)
{
	print_stats(stats, nasdaq_stat_names, ARRAY_SIZE(nasdaq_stat_names));
}

#define BUFFER_SIZE	(1ULL << 20) /* 1 MB */

void nasdaq_itch41_stat(struct stats *stats, int fd, z_stream *zstream)
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
		struct itch41_message *msg;
		int err;

		err = nasdaq_itch41_read(&stream, &msg);
		if (err)
			error("%s: %s", stats->filename, strerror(err));

		if (!msg)
			break;

		stats->stats[msg->MessageType]++;
	}

	buffer_munmap(comp_buf);

	buffer_delete(uncomp_buf);
}
