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
#include "xfs-nfs.h"
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
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0))
    warn << "Received wakeup from XFS\n";
}

void 
xfs_getroot (ref<xfscall> xfsc) 
{
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0))
    warn << "Received xfs_getroot\n";

  xfs_message_getroot *h = msgcast<xfs_message_getroot> (xfsc->argp);
  lbfs_getroot (xfsc->fd, xfsc->argp, 
		xfsc->getaid (&h->cred), sfsc, nfsc);
}

void 
xfs_getnode (ref<xfscall> xfsc) 
{
  xfs_message_getnode *h = msgcast<xfs_message_getnode> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_getnode\n";
    warn << h->header.sequence_num << ":" <<" xfs_parent_handle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
  }

  lbfs_getnode (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_getattr (ref<xfscall> xfsc) 
{
  xfs_message_getattr *h = msgcast<xfs_message_getattr> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_getattr\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
  }
  
  cache_entry *e = xfsindex[h->handle];
  if (!e)
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
  else lbfs_attr (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), e->nh, nfsc, NULL);
}

void 
xfs_getdata (ref<xfscall> xfsc) 
{
  xfs_message_getdata *h = msgcast<xfs_message_getdata> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_getdata\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
  }
  
  cache_entry *e = xfsindex[h->handle];
  if (!e) {
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
    return;
  }
  assert (e->nfs_attr.type != NF3DIR);
  if (e->incache)
    lbfs_readexist (xfsc->fd, xfsc->argp, e);
  else lbfs_open (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_inactivenode (ref<xfscall> xfsc) 
{
  xfs_message_inactivenode *h = msgcast<xfs_message_inactivenode> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_inactivenode\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
  }

  if (h->flag == XFS_DELETE || h->flag == XFS_NOREFS) {
    cache_entry *e = xfsindex[h->handle];
    if (e) delete e;
  }
}

void 
xfs_open (ref<xfscall> xfsc) 
{
  xfs_message_open *h = msgcast<xfs_message_open> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_open\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
  }
  
  lbfs_open (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_putdata (ref<xfscall> xfsc) 
{
  xfs_message_putdata *h = msgcast<xfs_message_putdata> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_putdata\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
    warn << "flag = " << h->flag << "\n";
  }
  
  lbfs_putdata (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);  
}

void 
xfs_putattr (ref<xfscall> xfsc) 
{
  xfs_message_putattr *h = msgcast<xfs_message_putattr> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_putattr\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->handle.a << ","
	 << (int) h->handle.b << ","
	 << (int) h->handle.c << ","
	 << (int) h->handle.d << ")\n";
  }
  
  cache_entry *e = xfsindex[h->handle];
  if (!e)
    xfs_reply_err (xfsc->fd, h->header.sequence_num, ENOENT);
  else lbfs_attr (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), e->nh, nfsc, NULL);
}

void 
xfs_create (ref<xfscall> xfsc) 
{
  xfs_message_create *h = msgcast<xfs_message_create> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_create\n";
    warn << h->header.sequence_num << ":" <<" parent_handle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
    warn << "file name: " << h->name << "\n";
  }

  lbfs_create (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_mkdir (ref<xfscall> xfsc) 
{
  xfs_message_create *h = msgcast<xfs_message_create> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_mkdir\n";
    warn << h->header.sequence_num << ":" <<" xfs_handle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
    warn << "file name: " << h->name << "\n";
  }

  lbfs_create (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_link (ref<xfscall> xfsc) 
{
  xfs_message_link *h = msgcast<xfs_message_link> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
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
  }
  
  lbfs_link (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_symlink (ref<xfscall> xfsc) 
{
  xfs_message_symlink *h = msgcast<xfs_message_symlink> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_symlink \n";
    warn << h->header.sequence_num << ":" <<" parent_handle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
    warn << "file name: " << h->name << "\n";
  }
 
  lbfs_symlink (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_remove (ref<xfscall> xfsc) 
{
  xfs_message_remove *h = msgcast<xfs_message_remove> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_remove\n";
    warn << h->header.sequence_num << ":" <<" xfs_parenthandle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
    warn << "file name: " << h->name << "\n";
  }

  lbfs_remove (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_rmdir (ref<xfscall> xfsc) 
{
  xfs_message_remove *h = msgcast<xfs_message_remove> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_rmdir\n";
    warn << h->header.sequence_num << ":" <<" xfs_parenthandle ("
	 << (int) h->parent_handle.a << ","
	 << (int) h->parent_handle.b << ","
	 << (int) h->parent_handle.c << ","
	 << (int) h->parent_handle.d << ")\n";
    warn << "file name: " << h->name << "\n";
  }

  lbfs_remove (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_rename (ref<xfscall> xfsc) 
{
  xfs_message_rename *h = msgcast<xfs_message_rename> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_rename\n";
    warn << h->header.sequence_num << ":" <<" xfs_old_parenthandle ("
	 << (int) h->old_parent_handle.a << ","
	 << (int) h->old_parent_handle.b << ","
	 << (int) h->old_parent_handle.c << ","
	 << (int) h->old_parent_handle.d << ")\n";
    warn << "old name: " << h->old_name << "\n";
    warn << h->header.sequence_num << ":" <<" xfs_new_parenthandle ("
	 << (int) h->new_parent_handle.a << ","
	 << (int) h->new_parent_handle.b << ","
	 << (int) h->new_parent_handle.c << ","
	 << (int) h->new_parent_handle.d << ")\n";
    warn << "new name: " << h->new_name << "\n";
  }

  lbfs_rename (xfsc->fd, xfsc->argp, xfsc->getaid (&h->cred), nfsc);
}

void 
xfs_pioctl (ref<xfscall> xfsc) 
{
  xfs_message_pioctl *h = msgcast<xfs_message_pioctl> (xfsc->argp);
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0)) {
    warn << "Received xfs_pioctl!! Return EINVAL no matter what!!!\n";
    warn << "pioctl: opcode = " << h->opcode << "\n";
  }

  xfs_send_message_wakeup (xfsc->fd, h->header.sequence_num, EINVAL);    
}

void 
cbdispatch(svccb *sbp) 
{
  if (getenv("LBCD_TRACE") && (strcmp(getenv ("LBCD_TRACE"), "0") > 0))
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
