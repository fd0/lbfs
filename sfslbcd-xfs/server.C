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

#include "sfslbcd.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"
#include "cache.h"
#include "lbfs.h"
#include "crypt.h"
#include "dmalloc.h"

ex_fsinfo3resok nfs_fsinfo;
u_int64_t cache_entry::nextxh;
ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;

void 
display (str msg_type, uint32 seqnum, str hdl_type, xfs_handle *hdl, 
	 uint32 *flag=NULL, str fname=NULL)
{
   if (lbcd_trace > 1) {
     warn << "Received " << msg_type << "\n";
     warn << seqnum << ": " << hdl_type << " ("
	  << (int) hdl->a << ","
	  << (int) hdl->b << ","
	  << (int) hdl->c << ","
	  << (int) hdl->d << ")\n";
     if (flag)
       warn << "flag = " << *flag << "\n";
   } 
}

void 
process_reply (/*time_t rqtime, uint32 proc, void *argp, */
	       void *resp, aclnt_cb cb, clnt_stat err) 
{
#if 0
  //update attr cache
  xattrvec xv;
  lbfs_getxattr (&xv, proc, argp, resp);
  for (xattr *x = xv.base (); x < xv.lim (); x++) {
    if (x->fattr)
      x->fattr->expire += rqtime;
    cache.attr_enter (*x->fh, x->fattr, x->wattr);

    if (lc->nfs_proc() == NFSPROC3_ACCESS) {
      ex_access3res *ares = static_cast<ex_access3res *> (resp);
      access3args *a = lc->template getarg<access3args> ();
      if (ares->status)
	cache.flush_access (a->object, lc->getaid ());
      else 
	cache.access_enter (a->object, nc->getaid (),
			    a->access, ares->resok->access);
    }
  }
#endif
  
  (*cb) (err);
}

void
xfs_wakeup (ref<xfscall> xfsc) 
{
  if (lbcd_trace > 1)
    warn << "Received wakeup from XFS\n";
}

void 
xfs_getroot (ref<xfscall> xfsc) 
{
  if (lbcd_trace > 1)
    warn << "Received xfs_getroot\n";

  xfs_message_getroot *h = msgcast<xfs_message_getroot> (xfsc->argp);
  lbfs_getroot (xfsc->fd, xfsc->argp, 
		xfsc->getaid (&h->cred), sfsc, nfsc);
}

void 
xfs_getnode (ref<xfscall> xfsc) 
{
  xfs_message_getnode *h = msgcast<xfs_message_getnode> (xfsc->argp);
  display ("xfs_getnode", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle);

  lbfs_getnode (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_getattr (ref<xfscall> xfsc) 
{
  xfs_message_getattr *h = msgcast<xfs_message_getattr> (xfsc->argp);
  display ("xfs_getattr", h->header.sequence_num, 
	   "xfs_handle", &h->handle);  
  
  cache_entry *e = xfsindex[h->handle];
  if (!e)
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
  else lbfs_attr (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), e->nh, nfsc, NULL);
}

void 
xfs_getdata (ref<xfscall> xfsc) 
{
  xfs_message_getdata *h = msgcast<xfs_message_getdata> (xfsc->argp);
  display ("xfs_getdata", h->header.sequence_num, 
	   "xfs_handle", &h->handle);  
  
  cache_entry *e = xfsindex[h->handle];
  if (!e) {
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
    return;
  }
#if 0
  assert (e->nfs_attr.type != NF3DIR);
#else
  if (e->nfs_attr.type == NF3DIR) {
    lbfs_open (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
    return;
  }
#endif   
  if (e->incache)
    lbfs_readexist (xfsc->fd, xfsc->argp, e);
  else lbfs_open (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_inactivenode (ref<xfscall> xfsc) 
{
  xfs_message_inactivenode *h = msgcast<xfs_message_inactivenode> (xfsc->argp);
  display ("xfs_inactivenode", h->header.sequence_num, 
	   "xfs_handle", &h->handle);  

  if (h->flag == XFS_DELETE || h->flag == XFS_NOREFS) {
    cache_entry *e = xfsindex[h->handle];
    if (e) delete e;
  }
}

void 
xfs_open (ref<xfscall> xfsc) 
{
  xfs_message_open *h = msgcast<xfs_message_open> (xfsc->argp);
  display ("xfs_open", h->header.sequence_num, 
	   "xfs_handle", &h->handle);  
  
  lbfs_open (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_putdata (ref<xfscall> xfsc) 
{
  xfs_message_putdata *h = msgcast<xfs_message_putdata> (xfsc->argp);
  display ("xfs_putdata", h->header.sequence_num, 
	   "xfs_handle", &h->handle, &h->flag);  
  
  lbfs_putdata (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);  
}

void 
xfs_putattr (ref<xfscall> xfsc) 
{
  xfs_message_putattr *h = msgcast<xfs_message_putattr> (xfsc->argp);
  display ("xfs_putattr", h->header.sequence_num, 
	   "xfs_handle", &h->handle);  
  
  cache_entry *e = xfsindex[h->handle];
  if (!e)
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
  else lbfs_attr (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), e->nh, nfsc, NULL);
}

void 
xfs_create (ref<xfscall> xfsc) 
{
  xfs_message_create *h = msgcast<xfs_message_create> (xfsc->argp);
  display ("xfs_create", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  

  lbfs_create (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_mkdir (ref<xfscall> xfsc) 
{
  xfs_message_create *h = msgcast<xfs_message_create> (xfsc->argp);
  display ("xfs_mkdir", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  

  lbfs_create (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_link (ref<xfscall> xfsc) 
{
  xfs_message_link *h = msgcast<xfs_message_link> (xfsc->argp);
  display ("xfs_link (hard)", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  
  display ("xfs_link (hard)", h->header.sequence_num, 
	   "xfs_from_handle", &h->from_handle);  
  
  lbfs_link (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_symlink (ref<xfscall> xfsc) 
{
  xfs_message_symlink *h = msgcast<xfs_message_symlink> (xfsc->argp);
  display ("xfs_symlink", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  
 
  lbfs_symlink (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_remove (ref<xfscall> xfsc) 
{
  xfs_message_remove *h = msgcast<xfs_message_remove> (xfsc->argp);
  display ("xfs_remove", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  

  lbfs_remove (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_rmdir (ref<xfscall> xfsc) 
{
  xfs_message_remove *h = msgcast<xfs_message_remove> (xfsc->argp);
  display ("xfs_rmdir", h->header.sequence_num, 
	   "xfs_parent_handle", &h->parent_handle, NULL, h->name);  

  lbfs_remove (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_rename (ref<xfscall> xfsc) 
{
  xfs_message_rename *h = msgcast<xfs_message_rename> (xfsc->argp);
  display ("xfs_rename", h->header.sequence_num, 
	   "xfs_old_parent_handle", &h->old_parent_handle, 
	   NULL, h->old_name);  
  display ("xfs_rename", h->header.sequence_num, 
	   "xfs_new_parent_handle", &h->new_parent_handle, 
	   NULL, h->new_name);  

  lbfs_rename (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_pioctl (ref<xfscall> xfsc) 
{
  xfs_message_pioctl *h = msgcast<xfs_message_pioctl> (xfsc->argp);
  if (lbcd_trace > 1) {
    warn << "Received xfs_pioctl!! Return EINVAL no matter what!!!\n";
    warn << "pioctl: opcode = " << h->opcode << "\n";
  }

  xfs_send_message_wakeup (xfsc->fd, h->header.sequence_num, EINVAL);    
}

void 
cbdispatch(svccb *sbp) 
{
  if (lbcd_trace > 1)
    warn << "cbdispatch triggered\n";

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
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
