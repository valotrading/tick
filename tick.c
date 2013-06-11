#include "tick/builtins.h"
#include "tick/types.h"

#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef int (*builtin_cmd_fn)(int argc, char *argv[]);

struct builtin_cmd {
	const char		*name;
	builtin_cmd_fn		cmd_fn;
};

const char *program;

#define DEFINE_BUILTIN(n, c) { .name = n, .cmd_fn = c }

static struct builtin_cmd builtins[] = {
	DEFINE_BUILTIN("ob",		cmd_ob),
	DEFINE_BUILTIN("stat",		cmd_stat),
	DEFINE_BUILTIN("taq",		cmd_taq),
};

static struct builtin_cmd *parse_builtin_cmd(const char *name)
{
	unsigned int i;

	for (i = 0; i < ARRAY_SIZE(builtins); i++) {
		struct builtin_cmd *cmd = &builtins[i];

		if (!strcmp(name, cmd->name))
			return cmd;
	}

	return NULL;
}

static void usage(void)
{
#define FMT								\
"\n usage: %s COMMAND [ARGS]\n"						\
"\n The commands are:\n"						\
"   ob        Convert file to OB format\n"				\
"   stat      Print stats\n"						\
"   taq       Convert file to TAQ format\n"				\
"\n"
	fprintf(stderr, FMT, program);

#undef FMT

	exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
	struct builtin_cmd *cmd;

	program = basename(argv[0]);

	if (argc < 2)
		usage();

	cmd = parse_builtin_cmd(argv[1]);
	if (!cmd)
		usage();

	return cmd->cmd_fn(argc, argv);
}
