## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbsd mkdb dbtest cfh chunk

noinst_HEADERS = sfsrwsd.h

sfslbsd_SOURCES = \
  client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C
sfslbsd_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

mkdb_SOURCES = mkdb.C getfh3.C
mkdb_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

chunk_SOURCES = chunk.C
chunk_LDADD = $(LDADD) $(DB3_LIB)

cfh_SOURCES = cfh.C getfh3.C
cfh_LDADD = $(LDADD) $(ZLIB_LIB)

dbtest_SOURCES = dbtest.C getfh3.C
dbtest_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
