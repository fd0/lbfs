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

// Implement this -- compress and call x->sendv
void
axprt_compress::sendv(const iovec *iov, int iovcnt, const sockaddr *sa)
{
  if (!docompress) {
    x->sendv (iov, iovcnt, sa);
    return;
  }

  int ret;
  iovec out_iov[iovcnt];
  z_streamp zp = New z_stream;

  zp->zalloc = Z_NULL;
  zp->zfree  = Z_NULL;
  zp->opaque = Z_NULL;
  
  for (int i=0; i<iovcnt; i++) {

    if ((ret = deflateInit(zp, Z_BEST_COMPRESSION)) != Z_OK) { //commented out in zlib.h
      warn << "err: " << ret << "\n";
      return;
    }

    zp->next_in  = (Bytef *) iov[i].iov_base;
    zp->avail_in = iov[i].iov_len;
    out_iov[i].iov_len  = iov[i].iov_len;
    out_iov[i].iov_base = new char[out_iov[i].iov_len];
    zp->next_out  = (Bytef *) out_iov[i].iov_base;
    zp->avail_out = out_iov[i].iov_len;
    
    do {
      ret = deflate(zp, Z_FINISH);
      if (ret == Z_OK && zp->avail_out == 0) {
	zp->avail_out = out_iov[i].iov_len;
	out_iov[i].iov_base = (char *)realloc((void *)out_iov[i].iov_base,
					       (out_iov[i].iov_len *= 2));
	zp->next_out = (Bytef *) ((char *)out_iov[i].iov_base + zp->avail_out);
      } else 
	if (ret < 0) {
	  warn << zp->msg << "\n";
	  return;
	}
    } while (ret != Z_STREAM_END);

    out_iov[i].iov_len = zp->total_out;

    if ((ret = deflateEnd(zp)) != Z_OK) {
      warn << zp->msg << "\n";
      return;
    }
  }
  
  x->sendv(out_iov, iovcnt, sa);
}

// Implement this -- uncompress and call (*cb)
void
axprt_compress::rcb(const char *buf, ssize_t len, const sockaddr *sa)
{
  if (!docompress) {
    (*cb) (buf, len, sa);
    return;
  }


  if (len <= 0) {
    (*cb) (buf, len, sa);
    return;
  }

  int ret;
  ssize_t out_len = 3*len;
  char *out_buf = (char *) malloc(out_len);
  z_streamp zp = New z_stream;
  
  zp->zalloc = Z_NULL;
  zp->zfree  = Z_NULL;
  zp->opaque = Z_NULL;
  
  zp->next_in = (Bytef *) buf;
  zp->avail_in = len;
  zp->next_out = (Bytef *) out_buf;
  zp->avail_out = out_len;

  if ((ret = inflateInit(zp)) != Z_OK) {
    warn << "err: " << ret << "\n";
    return;
  }
  
  do {
    ret = inflate(zp, Z_SYNC_FLUSH);
    if (ret == Z_OK && zp->avail_out == 0) {
      zp->avail_out = out_len;
      out_buf = (char *)realloc((void*)out_buf, (out_len *= 2));
      zp->next_out = (Bytef *) (out_buf + zp->avail_out);
    } else
      if (ret < 0) {
	warn << zp->msg << "\n";
	return;
      }
  } while (ret != Z_STREAM_END);
  
  out_len = zp->total_out;

  if ((ret = inflateEnd(zp)) != Z_OK) {
    warn << zp->msg << "\n";
    return;
  }

  (*cb) (out_buf, out_len, sa);
}
