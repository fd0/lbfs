## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbsd mkdb dbtest

noinst_HEADERS = sfsrwsd.h lbfs.h

sfslbsd_SOURCES = \
  client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C lbfs.C
sfslbsd_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

mkdb_SOURCES = mkdb.C getfh3.C
mkdb_LDADD = $(LDADD) $(DB3_LIB)

dbtest_SOURCES = dbtest.C lbfs.C getfh3.C
dbtest_LDADD = $(LDADD) $(DB3_LIB)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
