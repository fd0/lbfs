## Process this file with automake to produce Makefile.in
# $Id$

sfslib_LTLIBRARIES = liblbfs.la

liblbfs_la_SOURCES = lbfs_prot.C rabinpoly.C lbfsdb.C fingerprint.C lbfsxattr.C

sfsinclude_HEADERS = lbfs_prot.x lbfs_prot.h rabinpoly.h \
			lbfsdb.h fingerprint.h lbfs.h

lbfs_prot.h: $(srcdir)/lbfs_prot.x
	@rm -f $@
	-$(RPCC) -h -DSFSSVC $(srcdir)/lbfs_prot.x || rm -f $@

lbfs_prot.C: $(srcdir)/lbfs_prot.x
	@rm -f $@
	-$(RPCC) -c -DSFSSVC $(srcdir)/lbfs_prot.x || rm -f $@

dist-hook:
	cd $(distdir) && rm -f lbfs_prot.h lbfs_prot.C

lbfs_prot.o: lbfs_prot.h
lbfs_prot.lo: lbfs_prot.h

.PHONY: rpcclean
rpcclean:
	rm -f lbfs_prot.h lbfs_prot.C

EXTRA_DIST = lbfs_prot.x .cvsignore
CLEANFILES = core *.core *~ *.rpo lbfs_prot.h lbfs_prot.C
MAINTAINERCLEANFILES = Makefile.in
