#include "tick/error.h"

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

extern const char *program;

void error(const char *fmt, ...)
{
	va_list ap;

	fprintf(stderr, "%s: error: ", program);

	va_start(ap, fmt);

	vfprintf(stderr, fmt, ap);

	va_end(ap);

	fprintf(stderr, "\n");

	exit(EXIT_FAILURE);
}
