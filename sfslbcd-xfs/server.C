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

#include "xfs.h"
#include "xfs-sfs.h"
#include "cache.h"
#include "crypt.h"

ex_fsinfo3resok nfs_fsinfo;
u_int64_t cache_entry::nextxh;
ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;

void
xfs_wakeup (ref<xfscall> xfsc) 
{
#if DEBUG > 0
  warn << "Received wakeup from XFS\n";
#endif
  
}

void 
xfs_getroot (ref<xfscall> xfsc) 
{
#if DEBUG > 0
  warn << "Received xfs_getroot\n";
#endif
  
  lbfs_getroot (xfsc->fd, *((xfs_message_getroot *) xfsc->argp), 
		xfsc->getaid (), sfsc, nfsc);

}

void 
xfs_getnode (ref<xfscall> xfsc) 
{
  xfs_message_getnode *h = (xfs_message_getnode *) xfsc->argp;
#if DEBUG > 0
  warn << "Received xfs_getnode\n";
  warn << h->header.sequence_num << ":" <<" xfs_parent_handle ("
    << (int) h->parent_handle.a << ","
    << (int) h->parent_handle.b << ","
    << (int) h->parent_handle.c << ","
    << (int) h->parent_handle.d << ")\n";
#endif
  
  lbfs_getnode (xfsc->fd, *h, xfsc->getaid (), nfsc);
}

void 
xfs_getattr (ref<xfscall> xfsc) 
{
  
}

void 
xfs_getdata (ref<xfscall> xfsc) 
{

  
}

void 
xfs_inactivenode (ref<xfscall> xfsc) 
{

  
}

void 
xfs_open (ref<xfscall> xfsc) 
{
  xfs_message_open *h = (xfs_message_open *) xfsc->argp;
#if DEBUG > 0
  warn << "Received xfs_open\n";
  warn << h->header.sequence_num << ":" <<" xfs_handle ("
    << (int) h->handle.a << ","
    << (int) h->handle.b << ","
    << (int) h->handle.c << ","
    << (int) h->handle.d << ")\n";
#endif
  
  lbfs_open (xfsc->fd, *h, xfsc->getaid (), nfsc);
}

void 
xfs_putdata (ref<xfscall> xfsc) 
{

  
}

void 
xfs_putattr (ref<xfscall> xfsc) 
{

  
}

void 
xfs_create (ref<xfscall> xfsc) 
{
  xfs_message_create *h = (xfs_message_create *) xfsc->argp;
#if DEBUG > 0
  warn << "Received xfs_create\n";
  warn << h->header.sequence_num << ":" <<" xfs_handle ("
    << (int) h->parent_handle.a << ","
    << (int) h->parent_handle.b << ","
    << (int) h->parent_handle.c << ","
    << (int) h->parent_handle.d << ")\n";
  warn << "file name: " << h->name << "\n";
#endif

  lbfs_create (xfsc->fd, *h, xfsc->getaid(), nfsc);
}

void 
xfs_mkdir (ref<xfscall> xfsc) 
{
  xfs_message_create *h = (xfs_message_create *) xfsc->argp;
#if DEBUG > 0
  warn << "Received xfs_create\n";
  warn << h->header.sequence_num << ":" <<" xfs_handle ("
       << (int) h->parent_handle.a << ","
       << (int) h->parent_handle.b << ","
       << (int) h->parent_handle.c << ","
       << (int) h->parent_handle.d << ")\n";
  warn << "file name: " << h->name << "\n";
#endif

  lbfs_create (xfsc->fd, *h, xfsc->getaid (), nfsc);
}

void 
xfs_link (ref<xfscall> xfsc) 
{
  xfs_message_link *h = (xfs_message_link *) xfsc->argp;
#if DEBUG > 0
  warn << "Received xfs_link (hard)\n";
  warn << h->header.sequence_num << ":" <<" parent_handle ("
       << (int) h->parent_handle.a << ","
       << (int) h->parent_handle.b << ","
       << (int) h->parent_handle.c << ","
       << (int) h->parent_handle.d << ")\n";
  warn << "file name: " << h->name << "\n";
  warn << h->header.sequence_num << ":" <<" from_handle ("
       << (int) h->from_handle.a << ","
       << (int) h->from_handle.b << ","
       << (int) h->from_handle.c << ","
       << (int) h->from_handle.d << ")\n";
#endif  
  
  lbfs_link (xfsc->fd, *h, xfsc->getaid (), nfsc);
}

void 
xfs_symlink (ref<xfscall> xfsc) 
{
  
  
}

void 
xfs_remove (ref<xfscall> xfsc) 
{

  
}

void 
xfs_rmdir (ref<xfscall> xfsc) 
{

  
}

void 
xfs_rename (ref<xfscall> xfsc) 
{

  
}

void 
xfs_pioctl (ref<xfscall> xfsc) 
{

  
}

void 
cbdispatch(svccb *sbp) 
{
#if DEBUG > 0
  warn << "cbdispatch triggered\n";
#endif
  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case ex_NFSCBPROC3_NULL:
    sbp->reply (NULL);
    break;
  case ex_NFSCBPROC3_INVALIDATE:
    {
      ex_invalidate3args *xa = sbp->template getarg < ex_invalidate3args > ();
      ex_fattr3 *a = NULL;
      if (xa->attributes.present && xa->attributes.attributes->expire) {
	a = xa->attributes.attributes.addr ();
	a->expire += timenow;
	cache_entry *e = nfsindex[xa->handle];
	if (!e) {
	  warn << "cbdispatch: Can't find handle\n";
	  return;
	}
	e->nfs_attr = *a;
      }
      //delete a; should we delete this?
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
