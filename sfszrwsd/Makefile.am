## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfszrwsd

noinst_HEADERS = \
sfsrwsd.h

sfszrwsd_SOURCES = \
client.C fhtrans.C filesrv.C lease.C sfsrwsd.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
