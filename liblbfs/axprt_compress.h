// -*-c++-*-
/* $Id$ */

/*
 *
 * Copyright (C) 2001 David Mazieres (dm@uun.org)
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
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

#ifndef _AXPRT_COMPRESS_H_
#define _AXPRT_COMPRESS_H_ 1

#include "arpc.h"
#include "crypt.h"
#include "refcnt.h"
#include "zlib.h"

extern int lbfs_compress;

template<class T>
class axprt_compress : public T {
protected:
  bool docompress;
  axprt::recvcb_t compress_cb;
  z_stream zin;
  z_stream zout;
  size_t bufsize;
  char *buf;

  VA_TEMPLATE (explicit axprt_compress, : T, { init(); });
  ~axprt_compress ();

  void init ();
  void rcb (const char *buf, ssize_t len, const sockaddr *sa);
  void fail () { if (compress_cb) (*compress_cb) (NULL, -1, NULL); }

public:
  virtual void sendv (const iovec *, int, const sockaddr *);
  virtual void setwcb (cbv cb) { T::setwcb (cb); }
  virtual void setrcb (axprt::recvcb_t c) {
    compress_cb = c;
    if (compress_cb)
      T::setrcb (wrap (this, &axprt_compress::rcb));
    else
      T::setrcb (NULL);
  }
  void compress () { docompress = true; }
  static size_t ps (u_int s = defps) { return s + s/1000 + 13; } // see zlib.h
};

template<class T> 
inline void
axprt_compress<T>::init()
{
  docompress = false;
  assert (T::reliable && T::connected);
  setrcb (NULL);
  bzero (&zin, sizeof (zin));
  if (int zerr = inflateInit (&zin))
    panic ("inflateInit: %d\n", zerr);
  bzero (&zout, sizeof (zout));
  // warn << "using compression level " << lbfs_compress << "\n";
  if (int zerr = deflateInit (&zout, lbfs_compress))
    panic ("deflateInit: %d\n", zerr);

  bufsize = ps ();		// XXX - should use x's packet size
  buf = (char *) xmalloc (bufsize);
}

template<class T>
inline axprt_compress<T>::~axprt_compress ()
{
  inflateEnd (&zin);
  deflateEnd (&zout);
  free (buf);
}

template<class T>
inline void
axprt_compress<T>::sendv (const iovec *iov, int iovcnt, const sockaddr *sa)
{
  if (!docompress) {
    T::sendv (iov, iovcnt, sa);
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
  
  {
    iovec iov2 = {buf, (char*)zout.next_out - buf};
    T::sendv (&iov2, 1, sa);
  }
}

template<class T>
inline void
axprt_compress<T>::rcb(const char *pkt, ssize_t len, const sockaddr *sa)
{
  if (!compress_cb)
    return;

  if (!docompress || len <= 0) {
    (*compress_cb) (pkt, len, sa);
    return;
  }

  zin.next_out = (Bytef *) buf;
  zin.avail_out = bufsize;
  zin.next_in = (Bytef *) pkt;
  zin.avail_in = len;

  if (int zerr = inflate (&zin, Z_SYNC_FLUSH)) {
    warn ("inflate: %d\n", zerr);
#if 1
    warn << "try uncompressed transport\n";
    docompress = false;
    rcb (pkt, len, sa);
#else
    fail ();
#endif
    return;
  }

  (*compress_cb) (buf, (char *) zin.next_out - buf, sa);
}

typedef axprt_compress<axprt_crypt> axprt_zcrypt;

#endif /* _AXPRT_COMPRESS_H_ */
