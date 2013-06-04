#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "builtins.h"

#define MAX_LEN 256

#define TIMESTAMP_LEN                   9
#define EXCHANGE_LEN                    1
#define SYMBOL_LEN                      16
#define SALE_CONDITION_LEN              4
#define TRADE_VOLUME_LEN                9
#define TRADE_PRICE_LEN                 11
#define TRADE_STOP_STOCK_INDICATOR_LEN  1
#define TRADE_CORRECTION_INDICATOR_LEN  2
#define TRADE_SEQUENCE_NUMBER_LEN       16
#define SOURCE_OF_TRADE_LEN             1
#define TRADE_REPORTING_FACILITY_LEN    1

struct line_s
{
        char timestamp[TIMESTAMP_LEN];
        char exchange[EXCHANGE_LEN];
        char symbol[SYMBOL_LEN];
        char sale_condition[SALE_CONDITION_LEN];
        char trade_volume[TRADE_VOLUME_LEN];
        char trade_price[TRADE_PRICE_LEN];
        char trade_stop_stock_indicator[TRADE_STOP_STOCK_INDICATOR_LEN];
        char trade_correction_indicator[TRADE_CORRECTION_INDICATOR_LEN];
        char trade_sequence_number[TRADE_SEQUENCE_NUMBER_LEN];
        char source_of_trade[SOURCE_OF_TRADE_LEN];
        char trade_reporting_facility[TRADE_REPORTING_FACILITY_LEN];

};

extern const char *program;

static void error(const char *fmt, ...)
{
	va_list ap;

	fprintf(stderr, "%s: error: ", program);

	va_start(ap, fmt);

	vfprintf(stderr, fmt, ap);

	va_end(ap);

	fprintf(stderr, "\n");

	exit(EXIT_FAILURE);
}

static void usage(void)
{
#define FMT							\
"\n usage: %s nyse-taq <symbol> <input> <output>\n"		\
"\n"
	fprintf(stderr, FMT,
			program);

#undef FMT

	exit(EXIT_FAILURE);
}

static size_t fmt_value(char *buf, const char *value, size_t len, char sep)
{
        size_t ret = 0;

        if (value) {
                memcpy(buf, value, len);

                ret += len;
        }

        buf[ret++] = sep;

        return ret;
}

static char* rtrim_spaces(char* buffer)
{
        char *s = buffer + strlen(buffer);
        while (*--s == ' ');
        *(s + 1) = 0;
        return buffer;
}

static int is_symbol(struct line_s* line, const char* symbol)
{
        char source[SYMBOL_LEN + 1];

        memcpy(source, line->symbol, SYMBOL_LEN);
        source[SYMBOL_LEN] = 0;

        rtrim_spaces(source);

        return !strcmp(source, symbol);
}

static void write_header(FILE* f)
{
        fprintf(f, "Timestamp\t");
        fprintf(f, "Exchange\t");
        fprintf(f, "Symbol\t");
        fprintf(f, "SaleCondition\t");
        fprintf(f, "TradeVolume\t");
        fprintf(f, "TradePrice\t");
        fprintf(f, "TradeStopStockIndicator\t");
        fprintf(f, "TradeCorrectionIndicator\t");
        fprintf(f, "TradeSequenceNumber\t");
        fprintf(f, "SourceOfTrade\t");
        fprintf(f, "TradeReportingFacility\n");
}

static void write_line(FILE* f, struct line_s* line)
{
        size_t idx = 0;
        char buf[1024];

        idx += fmt_value(buf + idx, line->timestamp, TIMESTAMP_LEN, '\t');
        idx += fmt_value(buf + idx, line->exchange, EXCHANGE_LEN, '\t');
        idx += fmt_value(buf + idx, line->symbol, SYMBOL_LEN, '\t');
        idx += fmt_value(buf + idx, line->sale_condition, SALE_CONDITION_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_volume, TRADE_VOLUME_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_price, TRADE_PRICE_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_stop_stock_indicator, TRADE_STOP_STOCK_INDICATOR_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_correction_indicator, TRADE_CORRECTION_INDICATOR_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_sequence_number, TRADE_SEQUENCE_NUMBER_LEN, '\t');
        idx += fmt_value(buf + idx, line->source_of_trade, SOURCE_OF_TRADE_LEN, '\t');
        idx += fmt_value(buf + idx, line->trade_reporting_facility, TRADE_REPORTING_FACILITY_LEN, '\n');

        fwrite(buf, idx, 1, f);
}

int cmd_nyse_taq(int argc, char** argv)
{
        char buffer[MAX_LEN];
        char *symbol, *in_name, *out_name;
        FILE *in, *out;
        struct line_s line;
        size_t target_symbol_len;

        if (argc != 5)
                usage();

        symbol = argv[2];
        in_name = argv[3];
        out_name = argv[4];

        target_symbol_len = strlen(symbol);

        if (target_symbol_len > SYMBOL_LEN)
                error("Symbol '%s' is too long (%ld), max length %d\n", symbol, target_symbol_len, SYMBOL_LEN);

        printf("NYSE Daily TAQ trades:\n");
        printf("  symbol: %s\n", symbol);
        printf("  input:  %s\n", in_name);
        printf("  output: %s\n", out_name);

        in = fopen(in_name, "r");
        out = fopen(out_name, "w");

        if (in == NULL)
                error("Unable to open input '%s'\n", in_name);

        if (out == NULL)
                error("Unable to open output '%s'\n", out_name);

        write_header(out);

        fgets(buffer, MAX_LEN, in);

        while (fgets(buffer, MAX_LEN, in) != NULL) {
                memcpy(&line, buffer, sizeof(line));
                if (is_symbol(&line, symbol))
                        write_line(out, &line);
        }

        fclose(out);
        fclose(in);

        return 0;
}
