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

#include "xfs-sfs.h"
#include "messages.h"

char *sfs_path = new char[1000];
int server_fd = -1;
ptr<aclnt> sfsc = NULL;
ptr<aclnt> nfsc = NULL;
ptr<asrv>  nfscbs = NULL;
ref<axprt> *x;
vec<sfs_extension> sfs_extensions;
sfs_connectres conres;

#if LBFS_READ
lbfs_db lbfsdb;
#endif

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

void fail(int err) {
  errno = err;
}

void gotconres(int fd, str hostname, sfs_hash hostid, clnt_stat err) {

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

void sfsConnect(str hostname, sfs_hash hid, int fd) {
  if (fd < 0) {
    warn << strerror(errno) << ":bad fd\n";
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
  x = New ref<axprt>(axprt_stream::alloc (fd));
  sfsc = aclnt::alloc (*x, sfs_program_1);
  nfsc = aclnt::alloc (*x, lbfs_program_3);
  nfscbs = asrv::alloc (*x, ex_nfscb_program_3,
			wrap (&cbdispatch));
  sfs_connectarg arg;
  arg.release = sfs_release;
  arg.service = SFS_SFS;
  arg.name = hostname;
  arg.hostid = hid;
  arg.extensions.set (sfs_extensions.base (), sfs_extensions.size (),
		      freemode::NOFREE);
  sfsc->call (SFSPROC_CONNECT, &arg, &conres, 
	   wrap (&gotconres, fd, hostname, hid));

}

void sfsInit(const char* path) {

  str hostname;
  sfs_hash hid;
  u_int16_t port;
  if (!cd_parsepath (path, &hostname, &hid, &port)) {
    warn << strerror(ENOENT) << ": " << path << "\n";
    return;
  } 
  warn << "path = " << path << " port = " << port << "\n";
  strcpy(sfs_path, path);
  tcpconnect(hostname, port, wrap(sfsConnect, hostname, hid));
#if LBFS_READ
  lbfsdb.open();
#endif
}

