/*
 *
 * Copyright (C) 2001 Athicha Muthitacharoen (athicha@mit.edu)
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

#include "axprt_compress.h"

int lbfs_compress = 
  (getenv("LBFS_COMPRESS")?atoi(getenv("LBFS_COMPRESS")):Z_DEFAULT_COMPRESSION);

axprt_compress::axprt_compress (ref<axprt> xx)
  : axprt (true, true, xx->socksize), docompress (false), x (xx)
{
  assert (x->reliable && x->connected);
  x->setrcb (NULL);
  bzero (&zin, sizeof (zin));
  if (int zerr = inflateInit (&zin))
    panic ("inflateInit: %d\n", zerr);
  bzero (&zout, sizeof (zout));
  warn << "using compression level " << lbfs_compress << "\n";
  if (int zerr = deflateInit (&zout, lbfs_compress))
    panic ("deflateInit: %d\n", zerr);

  bufsize = ps ();		// XXX - should use x's packet size
  buf = (char *) xmalloc (bufsize);
}

axprt_compress::~axprt_compress ()
{
  inflateEnd (&zin);
  deflateEnd (&zout);
  free (buf);
}

void
axprt_compress::sendv (const iovec *iov, int iovcnt, const sockaddr *sa)
{
  if (!docompress) {
    x->sendv (iov, iovcnt, sa);
    return;
  }

  zout.next_out = (Bytef *) buf;
  zout.avail_out = bufsize;

  for (int i=0; i < iovcnt; i++) {
    zout.next_in  = (Bytef *) iov[i].iov_base;
    zout.avail_in = iov[i].iov_len;

#if 0
    if (!zout.avail_out) {
      buf = (char *) xrealloc (buf, 2 * bufsize);
      zout.next_out = (Bytef *) (buf + bufsize);
      zout.avail_out = bufsize;
      bufsize = 2 * bufsize;
    }
#endif

    if (int zerr = deflate (&zout, i == iovcnt - 1 ? Z_SYNC_FLUSH : 0)) {
      warn ("deflate: %d\n", zerr);
      fail ();
    }
  }

  x->send (buf, (char *) zout.next_out - buf, NULL);
}

void
axprt_compress::rcb(const char *pkt, ssize_t len, const sockaddr *sa)
{
  if (!docompress || len <= 0) {
    (*cb) (pkt, len, sa);
    return;
  }

  zin.next_out = (Bytef *) buf;
  zin.avail_out = bufsize;
  zin.next_in = (Bytef *) pkt;
  zin.avail_in = len;

  if (int zerr = inflate (&zin, Z_SYNC_FLUSH)) {
    warn ("inflate: %d\n", zerr);
    fail ();
  }

  (*cb) (buf, (char *) zin.next_out - buf, sa);
}
