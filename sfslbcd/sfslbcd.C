/*
 *
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "async.h"
#include "arpc.h"
#include "sfsmisc.h"
#include "crypt.h"
#include "sfsclient.h"
#include "sfsconnect.h"
#include "axprt_compress.h"
#include "sfslbcd.h"
#include "rxx.h"
#include <errno.h>

#define LBFSCACHE "/var/tmp/lbfscache"
#define LBCD_GC_PERIOD 120

static inline void
strip_mountprot(sfs_connectarg &carg, str &proto)
{
  str path = carg.ci5->sname;
  const char *ps = path;
  unsigned i;
  for (i=0; i<path.len(); i++)
    if (ps[i] == ':')
      break;
  if (i >= path.len())
    return;
  proto = substr(path, 0, i);
  carg.ci5->sname = substr(path,i+1);
}

void
sfslbcd_connect (sfsprog*prog, ref<nfsserv> ns, int tcpfd,
                 sfscd_mountarg *ma, sfsserver::fhcb cb, ptr<sfscon> c, str s)
{
  if (c) {
    rpc_ptr<sfs_connectok> cres;
    cres.alloc();
    sfs_connectok *p = cres;
    p->servinfo = c->servinfo->get_xdr ();
    ma->cres = cres;
    vNew refcounted<server>(sfsserverargs (ns, tcpfd, prog, ma, cb));
  }
  else {
    warnx << s << "\n";
    (*cb) (NULL);
  }
}

void
sfslbcd_getfd(sfsprog*prog, ref<nfsserv> ns, int tcpfd,
              sfscd_mountarg *ma, sfsserver::fhcb cb, int fd)
{
  u_int16_t port;
  str location;
  sfs_connectarg carg = ma->carg;
  sfs_parsepath (carg.ci5->sname, &location, NULL, &port);
  if (fd < 0) {
    warnx << location << ": cannot connect to host: "
          << strerror (errno) << "\n";
    (*cb) (NULL);
    return;
  }
  if (!isunixsocket (fd))
    tcp_nodelay (fd);
  ptr<axprt> x = New refcounted<axprt_zcrypt>(fd, axprt_zcrypt::ps());
  sfs_connect_withx
    (ma->carg, wrap(&sfslbcd_connect, prog, ns, tcpfd, ma, cb), x);
}

void
sfslbcd_alloc(sfsprog *prog, ref<nfsserv> ns, int tcpfd,
              sfscd_mountarg *ma, sfsserver::fhcb cb)
{
  if (ma->cres || ma->carg.civers != 5) {
    (*cb) (NULL);
    return;
  }

  sfs_connectarg carg = ma->carg;
  if (!sfs_parsepath (carg.ci5->sname)) {
    str proto;
    strip_mountprot(carg, proto);
    if (proto != "lbfs") {
      (*cb) (NULL);
      return;
    }
  }

  u_int16_t port;
  str location;
  if (!sfs_parsepath (carg.ci5->sname, &location, NULL, &port) ||
      !strchr(location, '.'))
    warn << carg.ci5->sname << ": cannot parse path\n";
  else {
    ma->carg.ci5->sname = carg.ci5->sname;
    tcpconnect
      (location, port, wrap (&sfslbcd_getfd, prog, ns, tcpfd, ma, cb));
    return;
  }
  (*cb) (NULL);
}

server::server (const sfsserverargs &a)
  : sfsserver_auth (a),
    fc(50000, wrap(mkref(this), &server::file_cache_gc_remove)),
    lc(64, wrap(mkref(this), &server::dir_lc_gc_remove))
{
  cdir = strbuf(LBFSCACHE) << "/" << a.ma->carg.ci5->sname;
  if (mkdir(cdir.cstr(), 0755) < 0 && errno != EEXIST)
    fatal ("cannot create cache directory %s\n", cdir.cstr());
  for (int i=0; i<254; i++) {
    str f = cdir << "/" << i;
    if (mkdir(f.cstr(), 0755) < 0 && errno != EEXIST)
      fatal ("cannot create cache directory %s\n", f.cstr());
  }
  rtpref = wtpref = 4096;
  try_compress = true;
  do_lbfs = false;

  bigint verf;
  char xxb[20];
  rnd.getbytes (xxb, 20);
  mpz_set_rawmag_be (&verf, xxb, 20);
  mpz_get_rawmag_be (verf3.base(), NFS3_WRITEVERFSIZE, &verf);
}

void
server::db_sync()
{
  fpdb.sync();
  delaycb (LBCD_GC_PERIOD, wrap(server::db_sync));
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);
  warn ("version %s, pid %d\n", VERSION, getpid ());

  if (argc != 1)
    fatal ("usage: %s\n", progname.cstr ());

  sfsconst_init ();
  random_init_file (sfsdir << "/random_seed");
  server::keygen ();
  
  if (mkdir(LBFSCACHE, 0755) < 0 && errno != EEXIST)
    fatal ("cannot create cache directory %s\n", LBFSCACHE);

  if (ptr<axprt_unix> x = axprt_unix_stdin ())
    // doesn't need simulated close, but mount with close option
    vNew sfsprog (x, &sfslbcd_alloc, false, true);
  else
    fatal ("could not get connection to sfscd.\n");

  server::fpdb.open_and_truncate(FP_DB);
  delaycb (LBCD_GC_PERIOD, wrap(server::db_sync));

  amain ();
}

