## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

# sbin_PROGRAMS = mkdb2
sfslib_PROGRAMS = sfslbsd mkdb chunk #common_chunks

noinst_HEADERS = sfsrwsd.h

sfslbsd_SOURCES = \
  client.C fhtrans.C filesrv.C getfh3.C lease.C sfsrwsd.C
sfslbsd_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

mkdb_SOURCES = mkdb.C getfh3.C
mkdb_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

#mkdb2_SOURCES = mkdb2.C
#mkdb2_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

chunk_SOURCES = chunk.C
chunk_LDADD = $(LDADD) $(DB3_LIB)

#common_chunks_SOURCES = common_chunks.C
#common_chunks_LDADD = $(LDADD) $(DB3_LIB)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
