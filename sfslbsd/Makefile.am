## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbsd mkdb chunk

noinst_HEADERS = sfslbsd.h

sfslbsd_SOURCES = \
  client.C fhtrans.C filesrv.C getfh3.C lease.C sfslbsd.C

mkdb_SOURCES = mkdb.C getfh3.C

chunk_SOURCES = chunk.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
