## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbcd cdtest

noinst_HEADERS = sfslbcd.h kernel.h xfs.h messages.h xfs-sfs.h xfs-nfs.h fh_map.h

sfslbcd_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

sfslbcd_SOURCES = sfslbcd.C kernel.C xfs.C messages.C xfs-sfs.C xfs-nfs.C

cdtest_SOURCES = cdtest.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
