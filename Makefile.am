## Process this file with automake to produce Makefile.in
# $Id$

SUBDIRS = liblbfs sfslbsd sfszrwsd sfslbcd

dist-hook:
	cd $(distdir) && chmod -x setup

.PHONY: rpcclean
rpcclean:
	for dir in liblbfs; do \
		(cd $$dir && $(MAKE) rpcclean); \
	done

CLEANFILES = core *.core *~

EXTRA_DIST = setup .cvsignore
MAINTAINERCLEANFILES = aclocal.m4 install-sh mkinstalldirs \
	missing configure config.guess config.sub config.h.in \
	ltconfig ltmain.sh INSTALL COPYING Makefile.in stamp-h.in
