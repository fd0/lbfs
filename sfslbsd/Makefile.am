## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

# sbin_PROGRAMS = mkdb2
sfslib_PROGRAMS = sfslbsd mkdb chunk #common_chunks

noinst_HEADERS = sfsrwsd.h

sfslbsd_SOURCES = \
  client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C

mkdb_SOURCES = mkdb.C getfh3.C

#mkdb2_SOURCES = mkdb2.C

chunk_SOURCES = chunk.C

#common_chunks_SOURCES = common_chunks.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
