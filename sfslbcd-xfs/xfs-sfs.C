/*
 *
 * Copyright (C) 2000 Athicha Muthitacharoen (athicha@mit.edu)
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
#include "lbfs_prot.h"
#include "xfs-sfs.h"
#include "xfs.h"

char *sfs_path = new char[1000];
int server_fd = -1;
ptr<aclnt> sfsc = NULL;
ptr<aclnt> nfsc = NULL;
ptr<asrv>  nfscbs = NULL;
ptr<axprt_compress> x;
ptr<axprt_crypt> xc;
vec<sfs_extension> sfs_extensions;
sfs_connectres conres;
fp_db lbfsdb;
bool lbfsdb_is_dirty = false;
#define LBCD_GC_PERIOD 120

sfs_aid 
xfscred2aid (const xfs_cred *xc) 
{
  if (!xc)
    return sfsaid_nobody;
  else
    return xc->uid;
}

AUTH *
lbfs_authof (sfs_aid sa) 
{
  /* This is very crude. Need better authentication. */
  return authunix_create ("localhost", (uid_t) sa, (gid_t) 100, 0, NULL);
}

bool
cd_parsepath (str path, str *host, sfs_hash *hostid, u_int16_t *portp)
{
  const char *p = path;

  int16_t port = SFS_PORT;
  if (portp)
    *portp = port;

  if (!sfsgethost (p))
    return false;
  if (host)
    host->setbuf (path, p - path);
  return *p++ == ':' && sfs_ascii2hostid (hostid, p);
}

void 
fail (int err) 
{
  errno = err;
}

void 
gotconres (int fd, str hostname, sfs_hash hostid, clnt_stat err) 
{
  if (err) {
    warn << sfs_path << ": " << err << "\n";
    fail (EIO);
    return;
  }
  if (conres.status) {
    warn << sfs_path << ": " << conres.status << "\n";
    switch (conres.status) {
    default:
    case SFS_NOSUCHHOST:
      fail (ENOENT);
      return;
    case SFS_NOTSUPP:
      fail (EPROTONOSUPPORT);
      return;
    case SFS_TEMPERR:
      fail (EAGAIN);
      return;
#if 0
    case SFS_REDIRECT:
      {
	ptr<revocation> r = revocation::alloc (*conres.revoke);
	if (r) {
	  timecb (timenow + 300, wrap (r, &revocation::nop));
	  flush_path (sfs_path);
	  for (size_t n = waitq.size (); n-- > 0;)
	    (*waitq.pop_front ()) (0);
	}
	fail (EAGAIN);
	return;
      }
#endif
    }
  }

  if (conres.reply->servinfo.host.hostname != hostname) {
    warn << sfs_path << ": server is '"
	 << conres.reply->servinfo.host.hostname << "'\n";
    fail (ENOENT);
    return;
  }
  if (!sfs_ckhostid (&hostid, conres.reply->servinfo.host)) {
    warn << sfs_path << ": hostid mismatch\n";
    fail (ENOENT);
    return;
  }
  if (conres.reply->servinfo.host.pubkey.nbits () < sfs_minpubkeysize) {
    warn << sfs_path << " : public key too small\n";
    fail (ENOENT);
    return;
  }

}

void 
sfsConnect (str hostname, sfs_hash hid, str path, int fd) 
{
  if (fd < 0) {
    warn << strerror(errno) << "..can't connect\n";
    return;
  }
  else {
    server_fd = fd;
    sockaddr_in sin;
    socklen_t len = sizeof (sin);
    bzero (&sin, sizeof (sin));
    if (getpeername (fd, reinterpret_cast<sockaddr *> (&sin), &len) < 0) {
      close (fd);
      warn << strerror(errno) << ":bad fd\n";
      return;
    }
#if 0
    if (badaddrs[sin.sin_addr]) {
      close (fd);
      warn ("%s: cannot connect to my own IP address\n", path.cstr ());
      fail (EDEADLK);
      return;
    }
#endif
  }

  tcp_nodelay (fd);
  xc = axprt_crypt::alloc (fd);
  x = axprt_compress::alloc (xc);
  sfsc = aclnt::alloc (x, sfs_program_1);
  nfsc = aclnt::alloc (x, lbfs_program_3);
  nfscbs = asrv::alloc (x, ex_nfscb_program_3, wrap (&cbdispatch));
  sfs_connectarg arg;
#if 1
  arg.set_civers (5);
  arg.ci5->release = sfs_release;
  arg.ci5->service = SFS_SFS;
  arg.ci5->sname = path;
  arg.ci5->extensions.set (sfs_extensions.base (), sfs_extensions.size (),
			   freemode::NOFREE);
#else
  arg.set_civers (4);
  arg.ci4->service = SFS_SFS;
  arg.ci4->name = hostname;
  arg.ci4->hostid = hid;
  arg.ci4->extensions.set (sfs_extensions.base (), sfs_extensions.size (),
			   freemode::NOFREE);
#endif
  sfsc->call (SFSPROC_CONNECT, &arg, &conres, 
	      wrap (&gotconres, fd, hostname, hid));

}

void
db_sync()
{
  if (lbfsdb_is_dirty) {
    lbfsdb.sync();
    lbfsdb_is_dirty = true;
  }
  delaycb(LBCD_GC_PERIOD, wrap(db_sync));
}

int
sfsInit (const char* path) 
{
  str hostname;
  sfs_hash hid;
  u_int16_t port;
  if (!cd_parsepath (path, &hostname, &hid, &port)) {
    warn << strerror(ENOENT) << ": " << path << "\n";
    return -1;
  } 
  warn << "path = " << path << " port = " << port << "\n";
  strcpy(sfs_path, path);
  tcpconnect(hostname, port, wrap(sfsConnect, hostname, hid, path));
  random_init_file (sfsdir << "/random_seed");
  lbfsdb.open_and_truncate(FP_DB);
  delaycb(LBCD_GC_PERIOD, wrap(db_sync));
  if (open("cache", O_RDONLY, 0666) < 0) {
    if (errno == ENOENT) {
      warn << "Creating dir: cache\n";
      if (mkdir("cache", 0777) < 0) {
	warn << strerror(errno) << "(" << errno << ") mkdir cache\n";
	return -1;
      }
    } else {
      warn << strerror(errno) << "(" << errno << ") open cache\n";
      return -1;
    }
  } 
  return 0;
}

  
