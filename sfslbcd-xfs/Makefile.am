## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbcd #sfsrwcd cdtest 

noinst_HEADERS = sfslbcd.h kernel.h xfs.h xfs-sfs.h xfs-nfs.h cache.h \
		 #sfsrwcd.h lbfsserv.h

sfslbcd_LDADD = $(LDADD) $(DB3_LIB) $(ZLIB_LIB)

sfslbcd_SOURCES = sfslbcd.C kernel.C xfs.C server.C xfs-sfs.C xfs-nfs.C cache.C \
		  helper.C

#sfsrwcd_SOURCES = \
#attrcache.C server.C sfsrwcd.C lbfsserv.C
#sfsrwcd_LDFLAGS = $(NOPAGING) $(MALLOCK)

$(exec_PROGRAMS): $(MALLOCK)

#cdtest_SOURCES = cdtest.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
