## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbsd

noinst_HEADERS = \
sfsrwsd.h

sfslbsd_SOURCES = \
client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
