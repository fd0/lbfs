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
#include "kernel.h"
#include "messages.h"
#include "xfs-sfs.h"

u_int *seqnums;

unsigned sent_stat[XFS_MSG_COUNT];

unsigned recv_stat[XFS_MSG_COUNT];

char *rcvfuncs_name[] = 
{
  "version",
  "wakeup",
  "getroot",
  "installroot",
  "getnode",
  "installnode",
  "getattr",
  "installattr",
  "getdata",
  "installdata",
  "inactivenode",
  "invalidnode",
  "open",
  "put_data",
  "put_attr",
  "create",
  "mkdir",
  "link",
  "symlink",
  "remove",
  "rmdir",
  "rename",
  "pioctl",
  "wakeup_data",
  "updatefid",
  "advlock",
  "gc nodes"
};

xfs_message_function rcvfuncs[] = {
NULL,					/* version */
(xfs_message_function)xfs_wakeup,	/* wakeup */
(xfs_message_function)xfs_getroot,	/* getroot */
NULL,					/* installroot */
(xfs_message_function)xfs_getnode, 	/* getnode */
NULL,					/* installnode */
(xfs_message_function)xfs_getattr,	/* getattr */
NULL,					/* installattr */
(xfs_message_function)xfs_getdata,	/* getdata */
NULL,					/* installdata */
(xfs_message_function)xfs_inactivenode,	/* inactivenode */
NULL,					/* invalidnode */ 
(xfs_message_function)xfs_open,		/* open */
(xfs_message_function)xfs_putdata,      /* put_data */
(xfs_message_function)xfs_putattr,      /* put attr */
(xfs_message_function)xfs_create,       /* create */
(xfs_message_function)xfs_mkdir,	/* mkdir */
(xfs_message_function)xfs_link,		/* link */
(xfs_message_function)xfs_symlink,      /* symlink */
(xfs_message_function)xfs_remove,	/* remove */
(xfs_message_function)xfs_rmdir,	/* rmdir */
(xfs_message_function)xfs_rename,	/* rename */
(xfs_message_function)xfs_pioctl,	/* pioctl */
#if 0
NULL,	                                        /* wakeup_data */
NULL,						/* updatefid */
NULL,						/* advlock */
NULL						/* gc nodes */
#endif
};

void
xfs_message_init (void)
{

  seqnums = (u_int *)malloc (sizeof (*seqnums) * getdtablesize ());
  assert (seqnums != NULL);
  for (int i = 0; i < getdtablesize (); ++i)
    seqnums[i] = 0;
  
  assert (sizeof(rcvfuncs_name) / sizeof(*rcvfuncs_name) == XFS_MSG_COUNT);
}

int
xfs_message_receive (int fd, struct xfs_message_header *h, u_int size)
{

  unsigned opcode = h->opcode;
  
  if (opcode >= XFS_MSG_COUNT || rcvfuncs[opcode] == NULL ) {
#if DEBUG > 0
    warn << "Bad message opcode = " << opcode << "!!!!!!!!!\n";
#endif
    return -1;
  }

#if DEBUG > 0
  warn << "Rec message: opcode = " << opcode << "("
       << rcvfuncs_name[opcode] << "), seq = " << h->sequence_num 
       << " size = " << h->size << "\n";
#endif

  ++recv_stat[opcode];
  str msgstr = str((const char *)h, size);
  xfscall *xfsc;

  switch (opcode) {
  case XFS_MSG_GETROOT: {
    struct xfs_message_getroot *hh = 
      (struct xfs_message_getroot *)msgstr.cstr();
    ref<struct xfs_message_getroot> msg = 
      New refcounted<struct xfs_message_getroot>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    ref<struct xfs_getroot_args> gra = 
      New refcounted<struct xfs_getroot_args> ();
    xfsc = New xfscall (opcode, fd, msg, gra);
    break;
  }
  case XFS_MSG_GETNODE: {
    struct xfs_message_getnode *hh = 
      (struct xfs_message_getnode *)msgstr.cstr();
    ref<struct xfs_message_getnode> msg = 
      New refcounted<struct xfs_message_getnode>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    break;
  }
  case XFS_MSG_GETATTR: {
    struct xfs_message_getattr *hh = 
      (struct xfs_message_getattr *)msgstr.cstr();
    ref<struct xfs_message_getattr> msg = 
      New refcounted<struct xfs_message_getattr>;
    msg->header = hh->header;
    msg->cred = hh->cred;    
    break;
  }
  case XFS_MSG_INACTIVENODE: {
    struct xfs_message_inactivenode *hh = 
      (struct xfs_message_inactivenode*)msgstr.cstr();
    ref<struct xfs_message_inactivenode> msg = 
      New refcounted<struct xfs_message_inactivenode>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->flag = hh->flag;
    msg->pad1 = hh->pad1;    
    break;
  }
  case XFS_MSG_GETDATA: {
    struct xfs_message_getdata *hh = 
      (struct xfs_message_getdata *)msgstr.cstr();
    ref<struct xfs_message_getdata> msg = 
      New refcounted<struct xfs_message_getdata>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->handle = hh->handle;
    msg->tokens = hh->tokens;
    msg->pad1 = hh->pad1;
    break;
  }
  case XFS_MSG_OPEN: {
    struct xfs_message_open *hh = 
      (struct xfs_message_open *)msgstr.cstr();
    ref<struct xfs_message_open> msg = 
      New refcounted<struct xfs_message_open>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->handle = hh->handle;
    msg->tokens = hh->tokens;
    msg->pad1 = hh->pad1;
    break;
  }
  case XFS_MSG_PUTDATA: {
    struct xfs_message_putdata *hh = 
      (struct xfs_message_putdata*)msgstr.cstr();
    ref<struct xfs_message_putdata> msg = 
      New refcounted<struct xfs_message_putdata>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->attr = hh->attr;
    msg->cred = hh->cred;    
    msg->flag = hh->flag;
    msg->pad1 = hh->pad1;    
    break;
  }
  case XFS_MSG_PUTATTR: {
    struct xfs_message_putattr *hh = 
      (struct xfs_message_putattr*)msgstr.cstr();
    ref<struct xfs_message_putattr> msg = 
      New refcounted<struct xfs_message_putattr>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->attr = hh->attr;
    msg->cred = hh->cred;
    break;
  }
  case XFS_MSG_CREATE: {
    struct xfs_message_create *hh = 
      (struct xfs_message_create*)msgstr.cstr();
    ref<struct xfs_message_create> msg = 
      New refcounted<struct xfs_message_create>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->attr = hh->attr;
    msg->mode = hh->mode;
    msg->pad1 = hh->pad1;    
    msg->cred = hh->cred;    
    break;
  }
  case XFS_MSG_MKDIR: {
    struct xfs_message_mkdir *hh = 
      (struct xfs_message_mkdir*)msgstr.cstr();
    ref<struct xfs_message_mkdir> msg = 
      New refcounted<struct xfs_message_mkdir>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->attr = hh->attr;
    msg->cred = hh->cred;         
    break;
  }
  case XFS_MSG_LINK: {
    struct xfs_message_link *hh = (struct xfs_message_link*)msgstr.cstr();
    ref<struct xfs_message_link> msg = 
      New refcounted<struct xfs_message_link>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->from_handle = hh->from_handle;
    msg->cred = hh->cred;             
    break;
  }
  case XFS_MSG_SYMLINK: {
    struct xfs_message_symlink *hh = 
      (struct xfs_message_symlink*)msgstr.cstr();
    ref<struct xfs_message_symlink> msg = 
      New refcounted<struct xfs_message_symlink>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    strcpy(msg->contents,hh->contents);
    msg->attr = hh->attr;
    msg->cred = hh->cred;             
    break;
  }
  case XFS_MSG_REMOVE: {
    struct xfs_message_remove *hh = (struct xfs_message_remove*)msgstr.cstr();
    ref<struct xfs_message_remove> msg = 
      New refcounted<struct xfs_message_remove>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->cred = hh->cred;                
    break;
  }
  case XFS_MSG_RMDIR: {
    struct xfs_message_rmdir *hh = (struct xfs_message_rmdir*)msgstr.cstr();
    ref<struct xfs_message_rmdir> msg = 
      New refcounted<struct xfs_message_rmdir>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->cred = hh->cred;                
    break;
  }
  case XFS_MSG_RENAME: {
    struct xfs_message_rename *hh = (struct xfs_message_rename *)msgstr.cstr();
    ref<struct xfs_message_rename> msg = 
      New refcounted<struct xfs_message_rename>;
    msg->header = hh->header;
    msg->old_parent_handle = hh->old_parent_handle;
    strcpy(msg->old_name, hh->old_name);
    msg->new_parent_handle = hh->new_parent_handle;
    strcpy(msg->new_name, hh->new_name);
    msg->cred = hh->cred;                
    break;
  }
  case XFS_MSG_PIOCTL: {
    struct xfs_message_pioctl *hh = (struct xfs_message_pioctl*)msgstr.cstr();
    ref<struct xfs_message_pioctl> msg = 
      New refcounted<struct xfs_message_pioctl>;
    msg->header = hh->header;
    msg->opcode = hh->opcode;
    msg->pad1 = hh->pad1;    
    msg->cred = hh->cred;    
    msg->insize = hh->insize;    
    msg->outsize = hh->outsize;  
    strcpy(msg->msg, hh->msg);
    msg->handle = hh->handle;    
    break;
  }
#if 0 /* Other cases */
  case XFS_MSG_UPDATEFID:
  case XFS_MSG_ADVLOCK:
  case XFS_MSG_GC_NODES:
#endif /* Other cases */
  default:
    return 0;
  }

  return (*rcvfuncs[opcode])(xfsc);
}

int
xfs_message_send (int fd, struct xfs_message_header *h, u_int size)
{
  unsigned opcode = h->opcode;
  int ret;
  
  h->size = size;
  h->sequence_num = seqnums[fd]++;

  if (opcode >= XFS_MSG_COUNT) {
#if DEBUG > 0
    warn << "Bad message opcode = " << opcode << "\n";
#endif
    return -1;
  }

  ++sent_stat[opcode];

#if DEBUG > 0
  warn << "Send message: opcode = " << opcode << "("<<
    rcvfuncs_name[opcode] << "), size = " << h->size << "\n";
#endif

  //NOTE: this is still a blocking socket..
  ret = kern_write (fd, h, size);
  if (u_int(ret) != size) {
#if DEBUG > 0
    warn << strerror(errno) << " xfs_message_send: write\n";
#endif
    return errno;
  } else
    return 0;
}

int
xfs_send_message_wakeup (int fd, u_int seqnum, int error)
{
  struct xfs_message_wakeup msg;

  msg.header.opcode = XFS_MSG_WAKEUP;
  msg.sleepers_sequence_num = seqnum;
  msg.error = error;
#if DEBUG > 0
  warn << "sending wakeup: seq = " << seqnum << ", error = " << error <<"\n";
#endif
  return xfs_message_send (fd, (struct xfs_message_header*)(&msg), 
			   sizeof(msg));
}

struct write_buf {
  unsigned char buf[MAX_XMSG_SIZE];
  size_t len;
};

static int
add_new_msg (int fd, 
	     struct xfs_message_header *h, size_t size,
	     struct write_buf *buf)
{
  /* align on 8 byte boundery */
  if (size > sizeof (buf->buf) - buf->len)
    return 1;
  
  h->sequence_num 	= seqnums[fd]++;
  h->size		= (size + 8) & ~ 7;

  assert (h->opcode >= 0 && h->opcode < XFS_MSG_COUNT);
  ++sent_stat[h->opcode];

#if DEBUG > 0
  warn << "Multi-send: opcode = " << h->opcode << "(" 
       << rcvfuncs_name[h->opcode]
       << ") size = " << h->size << "\n";
#endif
    
  memcpy (buf->buf + buf->len, h, size);
  memset (buf->buf + buf->len + size, 0, h->size - size);
  buf->len += h->size;
  return 0;
}

static int
send_msg (int fd, struct write_buf *buf)
{
  u_int ret;
  
  if (buf->len == 0)
    return 0;
  
  ret = kern_write (fd, buf->buf, buf->len);
#if DEBUG > 0
  warn << "ret = " << ret << " buf->len = " << buf->len << "\n";
#endif
  if (ret != buf->len) {
    if (errno == EBADF) 
      warn << "EBADF: ";
    else 
      if (errno == EAGAIN)
	warn << "EAGAIN: ";
#if DEBUG > 0
    warn << strerror(errno) << "(" << errno << ") :send_msg: write to fd=" 
	 << fd << "\n";
#endif
    buf->len = 0;
    return errno;
  }
  buf->len = 0;
  return 0;
}

int
xfs_send_message_vmultiple (int fd,
			    va_list args)
{
  struct xfs_message_header *h;
  struct write_buf *buf;
  size_t size;
  int ret;
  
  buf = (struct write_buf*) malloc (sizeof (*buf));
  if (buf == NULL)
    return ENOMEM;
  
  h = va_arg (args, struct xfs_message_header *);
  size = va_arg (args, size_t);
  buf->len = 0;
  while (h != NULL) {
    if (add_new_msg (fd, h, size, buf)) {
      ret = send_msg (fd, buf);
      if (ret) {
	free (buf);
	return ret;
      }
      if (add_new_msg (fd, h, size, buf))
	warn << "xfs_send_message_vmultiple: add_new_msg failed\n";
    }
    
    h = va_arg (args, struct xfs_message_header *);
    size = va_arg (args, size_t);
  }
  ret = send_msg (fd, buf);
  free (buf);
  return ret;
}

/*
 * Send multiple messages to the kernel (for performace reasons)
 */

int
xfs_send_message_wakeup_vmultiple (int fd,
				   u_int seqnum,
				   int error,
				   va_list args)
{
  struct xfs_message_wakeup msg;
  int ret;

  ret = xfs_send_message_vmultiple (fd, args);
#if DEBUG > 0
  if (ret)
    warn << 
      "xfs_send_message_wakeup_vmultiple: failed sending messages with error "
	 << ret << "\n";
#endif

  msg.header.opcode = XFS_MSG_WAKEUP;
  msg.header.size  = sizeof(msg);
  msg.header.sequence_num = seqnums[fd]++;
  msg.sleepers_sequence_num = seqnum;
  msg.error = error;

  ++sent_stat[XFS_MSG_WAKEUP];

#if DEBUG > 0
  warn << "multi-sending wakeup: seq = " << seqnum << ", error = " 
       << error << "\n";
#endif

  ret = kern_write (fd, &msg, sizeof(msg));
  if (ret != sizeof(msg)) {
#if DEBUG > 0
    warn << errno << "xfs_send_message_wakeup_vmultiple: writev";
#endif
    return -1;
  }
  return 0;
}

int
xfs_send_message_wakeup_multiple (int fd,  u_int seqnum, int error, ...)
{
  va_list args;
  int ret;
  
  va_start (args, error);
  ret = xfs_send_message_wakeup_vmultiple (fd, seqnum, error, args);
  va_end (args);
  return ret;
}


