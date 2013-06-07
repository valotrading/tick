# The default target of this Makefile:
all:

# Define V to have verbose build.
#
# Define LDFLAGS=-static to build a static binary.
#
# Define WERROR=0 to disable -Werror.

uname_S	:= $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_R	:= $(shell sh -c 'uname -r 2>/dev/null || echo not')

HAVE_LIBTRADING := $(shell libtrading-config --version >/dev/null 2>&1 && echo 'yes')

ifneq ($(HAVE_LIBTRADING),yes)
$(error Your system does not have libtrading.)
endif

HAVE_GLIB2 := $(shell pkg-config --exists glib-2.0 >/dev/null 2>&1 && echo 'yes')

ifneq ($(HAVE_GLIB2),yes)
$(error Your system does not have GLib. Please install glib2-devel or libglib2.0-dev)
endif

PREFIX ?= $(HOME)
DESTDIR=
BINDIR=$(PREFIX)/bin

ifneq ($(WERROR),0)
	CFLAGS_WERROR = -Werror
endif

EXTRA_WARNINGS := -Wbad-function-cast
EXTRA_WARNINGS += -Wdeclaration-after-statement
EXTRA_WARNINGS += -Wformat-security
EXTRA_WARNINGS += -Wformat-y2k
EXTRA_WARNINGS += -Winit-self
EXTRA_WARNINGS += -Wmissing-declarations
EXTRA_WARNINGS += -Wmissing-prototypes
EXTRA_WARNINGS += -Wnested-externs
EXTRA_WARNINGS += -Wno-system-headers
EXTRA_WARNINGS += -Wold-style-definition
EXTRA_WARNINGS += -Wredundant-decls
EXTRA_WARNINGS += -Wshadow
EXTRA_WARNINGS += -Wstrict-prototypes
EXTRA_WARNINGS += -Wswitch-default
EXTRA_WARNINGS += -Wswitch-enum
EXTRA_WARNINGS += -Wundef
EXTRA_WARNINGS += -Wwrite-strings
EXTRA_WARNINGS += -Wcast-align
EXTRA_WARNINGS += -Wformat

CFLAGS_OPTIMIZE = -O3

CFLAGS = -g -std=gnu99 -Wall -Wextra $(CFLAGS_WERROR) $(CFLAGS_OPTIMIZE) $(EXTRA_WARNINGS)
ALL_CFLAGS = $(CFLAGS) -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE
ALL_LDFLAGS = $(LDFLAGS)

ALL_CFLAGS	+= $(shell libtrading-config --cflags)
ALL_LDFLAGS	+= $(shell libtrading-config --ldflags)
LIBS		+= $(shell libtrading-config --libs)

ALL_CFLAGS	+= $(shell pkg-config --cflags glib-2.0)
LIBS		+= $(shell pkg-config --libs glib-2.0)

# Make the build silent by default
V =
ifeq ($(strip $(V)),)
	E = @echo
	Q = @
else
	E = @\#
	Q =
endif
export E Q

INST_PROGRAMS = tick

PROGRAMS = tick

BUILTIN_OBJS += base36.o
BUILTIN_OBJS += bats-pitch112.o
BUILTIN_OBJS += bats-stat.o
BUILTIN_OBJS += builtin-ob.o
BUILTIN_OBJS += builtin-stat.o
BUILTIN_OBJS += dsv.o
BUILTIN_OBJS += error.o
BUILTIN_OBJS += nasdaq-itch41.o
BUILTIN_OBJS += nasdaq-stat.o
BUILTIN_OBJS += ob.o
BUILTIN_OBJS += stats.o

#
# Build rules
#

all: $(PROGRAMS)

tick: tick.o $(BUILTIN_OBJS)
	$(E) "  LINK    " $@
	$(Q) $(CC) $(ALL_CFLAGS) $(ALL_LDFLAGS) tick.o $(BUILTIN_OBJS) $(LIBS) -o $@

%.o: %.c
	$(E) "  CC      " $@
	$(Q) $(CC) -o $@ -c $(ALL_CFLAGS) $<

#
# Installation rules
#

define INSTALL_EXEC
        install -v $1 $(DESTDIR)$2/$1 || exit 1;
endef

install:
	$(E) "  INSTALL"
	$(Q) install -d $(DESTDIR)$(BINDIR)
	$(Q) $(foreach f,$(INST_PROGRAMS),$(call INSTALL_EXEC,$f,$(BINDIR)))

#
# Cleaning rules
#

clean:
	$(E) "  CLEAN"
	$(Q) rm -f $(BUILTIN_OBJS) $(PROGRAMS)

.PHONY: all install clean
