## Process this file with automake to produce Makefile.in
# $Id$

sfslib_LTLIBRARIES = liblbfs.la

liblbfs_la_SOURCES = \
axprt_compress.C fingerprint.C lbfs_prot.C lbfsxattr.C rabinpoly.C

sfsinclude_HEADERS = lbfs_prot.x \
axprt_compress.h fingerprint.h lbfs.h lbfs_prot.h lbfsdb.h rabinpoly.h chunk.h

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

check_PROGRAMS = test_compress
test_compress_SOURCES = test_compress.C
test_compress_LDADD = $(LDADD) $(ZLIB_LIB)
$(check_PROGRAMS): $(LDEPS) liblbfs.la

.PHONY: rpcclean
rpcclean:
	rm -f lbfs_prot.h lbfs_prot.C

EXTRA_DIST = lbfs_prot.x .cvsignore
CLEANFILES = core *.core *~ *.rpo lbfs_prot.h lbfs_prot.C
MAINTAINERCLEANFILES = Makefile.in
