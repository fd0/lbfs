## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbsd

noinst_HEADERS = \
sfsrwsd.h

sfslbsd_SOURCES = \
client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C
sfslbsd_LDADD = $(LDADD) $(DB3_LIB)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
