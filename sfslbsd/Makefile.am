## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = mkdb dbif lbfsusrv

noinst_HEADERS = \
lbfs.h lbfsusrv.h

lbfsusrv_SOURCES = lbfsusrv.C lbfsclient.C

mkdb_SOURCES = mkdb.C
mkdb_LDADD = $(LDADD) $(DB3_LIB)

dbif_SOURCES = dbif.C lbfs.C
dbif_LDADD = $(LDADD) $(DB3_LIB)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
