## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbcd

noinst_HEADERS = \
sfslbcd.h

sfslbcd_SOURCES = \
attrcache.C server.C sfslbcd.C read.C write.C ranges.C
sfslbcd_LDFLAGS = $(NOPAGING) $(MALLOCK)

$(sfslib_PROGRAMS): $(MALLOCK)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

