## Process this file with automake to produce Makefile.in
# $Id$

$(PROGRAMS): $(LDEPS)

sfslib_PROGRAMS = sfslbcd

noinst_HEADERS = sfslbcd.h kernel.h xfs.h messages.h xfs-sfs.h xfs-nfs.h fh_map.h

sfslbcd_SOURCES = sfslbcd.C kernel.C xfs.C messages.C xfs-sfs.C 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
