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
#include "messages.h"
#include "xfs-sfs.h"

u_int *seqnums;

/* number of times each type of message has been sent */

unsigned sent_stat[XFS_MSG_COUNT];

/* number of times each type of message has been received */

unsigned recv_stat[XFS_MSG_COUNT];

//we're not eliminating the function names because xfs enforces them to exist
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

#if 0
/*
 * A interface for the userland to talk the kernel and recv
 * back a integer. For larger messages implement a simularfunction
 * that uses the `wakeup_data' message.
 */

int
xfs_message_rpc (int fd, struct xfs_message_header *h, u_int size)
{
    int ret;

    ret = xfs_message_send (fd, h, size);
    if (ret)
	return ret;
    return xfs_message_sleep (h->sequence_num);
}

/*
 * Try to probe the version on `fd', returning the version.
 */

static int
xfs_send_message_version (int fd)
{
     struct xfs_message_version msg;
     int ret;

     msg.header.opcode = XFS_MSG_VERSION;
     warn << "sending version\n";
     ret = xfs_message_rpc (fd, (struct xfs_message_header *)&msg, 
			    sizeof(msg));
     return ret;
}

/*
 * Probe for version on `fd'.  Fail if != version
 */

void
xfs_probe_version (int fd, int version)
{
    int ret = xfs_send_message_version (fd);

    if (ret != version)
      warn << "Wrong version of xfs.  Please {up,down}grade to" << version << "\n";
}


/*
 * Send `num' `fids' to xfs on `fd' as proposed gc-able fids
 * If `num' is 0 xfs should gc everything gc:able.
 */

/* XXX VenusFid is wrong here */

//This is for garbage collecting..

void
xfs_send_message_gc_nodes (int fd, int num, VenusFid *fids)
{
    struct xfs_message_gc_nodes msg;
    int i;
    
    arla_warnx (ADEBMSG, 
		"xfs_send_message_gc_nodes sending gc: num = %d", num);
    
    if (num > XFS_GC_NODES_MAX_HANDLE)
	num = XFS_GC_NODES_MAX_HANDLE;
    
    msg.header.opcode = XFS_MSG_GC_NODES;
    msg.len = num;
    
    for (i = 0; i < num; i++)
	memcpy (&msg.handle[i], &fids[i], sizeof(*fids));
     
    xfs_message_send (fd, (struct xfs_message_header *)&msg, 
		      sizeof(msg));
}

#endif

void
xfs_message_init (void)
{

     seqnums = (u_int *)malloc (sizeof (*seqnums) * getdtablesize ());
     if (seqnums == NULL)
       warn << strerror(errno) << ":xfs_message_init: malloc\n";
     for (int i = 0; i < getdtablesize (); ++i)
	  seqnums[i] = 0;

     assert (sizeof(rcvfuncs_name) / sizeof(*rcvfuncs_name) == XFS_MSG_COUNT);
}


/*
 * Go to entry in jump-table depending on entry.
 */

int
xfs_message_receive (int fd, struct xfs_message_header *h, u_int size)
{

  warn ("*********CLIENT: bytes SENT = %lu   bytes RECEIVED = %lu  ***********\n", xc->bytes_sent, xc->bytes_recv);

  unsigned opcode = h->opcode;
  
  if (opcode >= XFS_MSG_COUNT || rcvfuncs[opcode] == NULL ) {
    warn << "Bad message opcode = " << opcode << "!!!!!!!!!\n";
    return -1;
  }
  
  ++recv_stat[opcode];
  
  warn << "Rec message: opcode = " << opcode << "("
       << rcvfuncs_name[opcode] << "), seq = " << h->sequence_num 
       << " size = " << h->size << "\n";

  str msgstr = str((const char *)h, size);
  switch (opcode) {
  case XFS_MSG_GETROOT: {
    struct xfs_message_getroot *hh = (struct xfs_message_getroot *)msgstr.cstr();
    ref<struct xfs_message_getroot> msg = New refcounted<struct xfs_message_getroot>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    return xfs_message_getroot(fd, msg, size);
    break;
  }
  case XFS_MSG_GETNODE: {
    struct xfs_message_getnode *hh = (struct xfs_message_getnode *)msgstr.cstr();
    ref<struct xfs_message_getnode> msg = New refcounted<struct xfs_message_getnode>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    return xfs_message_getnode(fd, msg, size);
    break;
  }
  case XFS_MSG_GETATTR: {
    struct xfs_message_getattr *hh = (struct xfs_message_getattr *)msgstr.cstr();
    ref<struct xfs_message_getattr> msg = New refcounted<struct xfs_message_getattr>;
    msg->header = hh->header;
    msg->cred = hh->cred;    
    return xfs_message_getattr(fd, msg, size);
    break;
  }
  case XFS_MSG_INACTIVENODE: {
    struct xfs_message_inactivenode *hh = (struct xfs_message_inactivenode*)msgstr.cstr();
    ref<struct xfs_message_inactivenode> msg = New refcounted<struct xfs_message_inactivenode>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->flag = hh->flag;
    msg->pad1 = hh->pad1;    
    return xfs_message_inactivenode(fd, msg, size);
    break;
  }
  case XFS_MSG_GETDATA:
    {
    struct xfs_message_getdata *hh = (struct xfs_message_getdata *)msgstr.cstr();
    ref<struct xfs_message_getdata> msg = New refcounted<struct xfs_message_getdata>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->handle = hh->handle;
    msg->tokens = hh->tokens;
    msg->pad1 = hh->pad1;
    return  xfs_message_getdata(fd, msg, size);
    break;
  }
  case XFS_MSG_OPEN:
    {
    struct xfs_message_open *hh = (struct xfs_message_open *)msgstr.cstr();
    ref<struct xfs_message_open> msg = New refcounted<struct xfs_message_open>;
    msg->header = hh->header;
    msg->cred = hh->cred;
    msg->handle = hh->handle;
    msg->tokens = hh->tokens;
    msg->pad1 = hh->pad1;
    return  xfs_message_open(fd, msg, size);
    break;
  }
  case XFS_MSG_PUTDATA: {
    struct xfs_message_putdata *hh = (struct xfs_message_putdata*)msgstr.cstr();
    ref<struct xfs_message_putdata> msg = New refcounted<struct xfs_message_putdata>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->attr = hh->attr;
    msg->cred = hh->cred;    
    msg->flag = hh->flag;
    msg->pad1 = hh->pad1;    
    return xfs_message_putdata(fd, msg, size);
    break;
  }
  case XFS_MSG_PUTATTR: {
    struct xfs_message_putattr *hh = (struct xfs_message_putattr*)msgstr.cstr();
    ref<struct xfs_message_putattr> msg = New refcounted<struct xfs_message_putattr>;
    msg->header = hh->header;
    msg->handle = hh->handle;
    msg->attr = hh->attr;
    msg->cred = hh->cred;        
    return xfs_message_putattr(fd, msg, size);
  }
  case XFS_MSG_CREATE: {
    struct xfs_message_create *hh = (struct xfs_message_create*)msgstr.cstr();
    ref<struct xfs_message_create> msg = New refcounted<struct xfs_message_create>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->attr = hh->attr;
    msg->mode = hh->mode;
    msg->pad1 = hh->pad1;    
    msg->cred = hh->cred;    
    return xfs_message_create(fd, msg, size);
    break;
  }
  case XFS_MSG_MKDIR: {
    struct xfs_message_mkdir *hh = (struct xfs_message_mkdir*)msgstr.cstr();
    ref<struct xfs_message_mkdir> msg = New refcounted<struct xfs_message_mkdir>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->attr = hh->attr;
    msg->cred = hh->cred;         
    return xfs_message_mkdir(fd, msg, size);
    break;
  }
  case XFS_MSG_LINK: {
    struct xfs_message_link *hh = (struct xfs_message_link*)msgstr.cstr();
    ref<struct xfs_message_link> msg = New refcounted<struct xfs_message_link>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->from_handle = hh->from_handle;
    msg->cred = hh->cred;             
    return xfs_message_link(fd, msg, size);
    break;
  }
  case XFS_MSG_SYMLINK: {
    struct xfs_message_symlink *hh = (struct xfs_message_symlink*)msgstr.cstr();
    ref<struct xfs_message_symlink> msg = New refcounted<struct xfs_message_symlink>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    strcpy(msg->contents,hh->contents);
    msg->attr = hh->attr;
    msg->cred = hh->cred;             
    return xfs_message_symlink(fd, msg, size);
    break;
  }
  case XFS_MSG_REMOVE: {
    struct xfs_message_remove *hh = (struct xfs_message_remove*)msgstr.cstr();
    ref<struct xfs_message_remove> msg = New refcounted<struct xfs_message_remove>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->cred = hh->cred;                
    return xfs_message_remove(fd, msg, size);
    break;
  }
  case XFS_MSG_RMDIR: {
    struct xfs_message_rmdir *hh = (struct xfs_message_rmdir*)msgstr.cstr();
    ref<struct xfs_message_rmdir> msg = New refcounted<struct xfs_message_rmdir>;
    msg->header = hh->header;
    msg->parent_handle = hh->parent_handle;
    strcpy(msg->name, hh->name);
    msg->cred = hh->cred;                
    return xfs_message_rmdir(fd, msg, size);
    break;
  }
  case XFS_MSG_RENAME: {
    struct xfs_message_rename *hh = (struct xfs_message_rename *)msgstr.cstr();
    ref<struct xfs_message_rename> msg = New refcounted<struct xfs_message_rename>;
    msg->header = hh->header;
    msg->old_parent_handle = hh->old_parent_handle;
    strcpy(msg->old_name, hh->old_name);
    msg->new_parent_handle = hh->new_parent_handle;
    strcpy(msg->new_name, hh->new_name);
    msg->cred = hh->cred;                
    return xfs_message_rename(fd, msg, size);
    break;
  }
  case XFS_MSG_PIOCTL: {
    struct xfs_message_pioctl *hh = (struct xfs_message_pioctl*)msgstr.cstr();
    ref<struct xfs_message_pioctl> msg = New refcounted<struct xfs_message_pioctl>;
    msg->header = hh->header;
    msg->opcode = hh->opcode;
    msg->pad1 = hh->pad1;    
    msg->cred = hh->cred;    
    msg->insize = hh->insize;    
    msg->outsize = hh->outsize;  
    strcpy(msg->msg, hh->msg);
    msg->handle = hh->handle;    
    return xfs_message_pioctl(fd, msg, size);
    break;
  }
#if 0
  case : {
    *hh = ()msgstr.cstr();
    ref<> msg = New refcounted<>;
  return (fd, msg, size);

    break;
  }
#endif
   default:
    return 0;
    break;
  }
  //return (*rcvfuncs[opcode])(fd, (struct xfs_message_header *)msg.cstr(), size);
}


/*
 * Send a message to the kernel module.
 */

int
xfs_message_send (int fd, struct xfs_message_header *h, u_int size)
{
     unsigned opcode = h->opcode;
     int ret;

     h->size = size;
     h->sequence_num = seqnums[fd]++;

     if (opcode >= XFS_MSG_COUNT) {
       warn << "Bad message opcode = " << opcode << "\n";
       return -1;
     }

     ++sent_stat[opcode];

     warn << "Send message: opcode = " << opcode << "("<<
       rcvfuncs_name[opcode] << "), size = " << h->size << "\n";

     //this is still a non-blocking socket..
     ret = kern_write (fd, h, size);
     if (u_int(ret) != size) {
       warn << strerror(errno) << " xfs_message_send: write\n";
       return errno;
     } else
	 return 0;
}

/*
 * Wake up a sleeping kernel-thread that sleeps on `seqnum'
 * and pass on `error' as an error the thread.
 */

int
xfs_send_message_wakeup (int fd, u_int seqnum, int error)
{
     struct xfs_message_wakeup msg;

     msg.header.opcode = XFS_MSG_WAKEUP;
     msg.sleepers_sequence_num = seqnum;
     msg.error = error;
     warn << "sending wakeup: seq = " << seqnum << ", error = " << error <<"\n";
     return xfs_message_send (fd, (struct xfs_message_header*)(&msg), 
			      sizeof(msg));
}


#if 0
/*
 * This code can only wake up message of type `xfs_message_wakeup'
 */

int
xfs_message_wakeup (int fd, struct xfs_message_wakeup *h, u_int size)
{
     Listitem *i, *next;
     struct xfs_message_wakeup *w;

     assert (sizeof(*w) >= size);

     for (i = listhead (sleepers); i; i = next) {
	  next = listnext (sleepers, i);
	  w = (struct xfs_message_wakeup *)listdata(i);
	  if (w->header.sequence_num == h->sleepers_sequence_num) {
	       listdel (sleepers, i);
	       memcpy (w, h, size);
	       LWP_SignalProcess ((char *)w);
	       break;
	  }
     }
     if (i == NULL)
       warn << "xfs_message_wakeup: no message to wakeup!\n";
     return 0;
}


/*
 * The middle and last part of the xfs_message_rpc.
 */

int
xfs_message_sleep (u_int seqnum)
{
    struct xfs_message_wakeup h;

    h.header.sequence_num = seqnum;
    
    listaddtail (sleepers, &h);
    LWP_WaitProcess ((char *)&h);
    return h.error;
}

/*
 * Wake-up a kernel-thread with `seqnum', and pass on `error'
 * ad return value. Add also a data blob for gerneric use.
 */

int
xfs_send_message_wakeup_data (int fd, u_int seqnum, int error,
			      void *data, int size)
{
     struct xfs_message_wakeup_data msg;
     
     msg.header.opcode = XFS_MSG_WAKEUP_DATA;
     msg.sleepers_sequence_num = seqnum;
     msg.error = error;
     arla_warnx (ADEBMSG,
		 "sending wakeup: seq = %u, error = %d", seqnum, error);

     if (sizeof(msg) >= size && size != 0) {
	 memcpy(msg.msg, data, size);
     }

     msg.len = size;

     return xfs_message_send (fd, (struct xfs_message_header *)&msg, 
			      sizeof(msg));
}

#endif

/*
 *
 */

struct write_buf {
    unsigned char buf[MAX_XMSG_SIZE];
    size_t len;
};

/*
 * Return 1 it buf is full, 0 if it's not.
 */

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

    warn << "Multi-send: opcode = " << h->opcode << "(" << rcvfuncs_name[h->opcode]
	 << ") size = " << h->size << "\n";
    
    memcpy (buf->buf + buf->len, h, size);
    memset (buf->buf + buf->len + size, 0, h->size - size);
    buf->len += h->size;
    return 0;
}

/*
 * Blast of a `buf' to `fd'.
 */

static int
send_msg (int fd, struct write_buf *buf)
{
    u_int ret;

    if (buf->len == 0)
	return 0;

    ret = kern_write (fd, buf->buf, buf->len);
    warn << "ret = " << ret << " buf->len = " << buf->len << "\n";
    if (ret != buf->len) {
      if (errno == EBADF) 
	warn << "EBADF: ";
      else 
	if (errno == EAGAIN)
	  warn << "EAGAIN: ";
      warn << strerror(errno) << "(" << errno << ") :send_msg: write to fd=" << fd << "\n";
      buf->len = 0;
      return errno;
    }
    buf->len = 0;
    return 0;
}

/*
 *
 */

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

#if 0
/*
 * Same as above but diffrent.
 */

int
xfs_send_message_multiple (int fd,
			   ...)
{
    va_list args;
    int ret;

    va_start (args, fd);
    ret = xfs_send_message_vmultiple (fd, args);
    va_end (args);
    return ret;
}
			   
/*
 * Almost same as above but diffrent.
 */

int
xfs_send_message_multiple_list (int fd,
				struct xfs_message_header *h,
				size_t size,
				u_int num)
{
    struct write_buf *buf;
    int ret = 0;

    buf = malloc (sizeof (*buf));
    if (buf == NULL)
	return ENOMEM;
    buf->len = 0;
    while (num && ret == 0) {
	if (add_new_msg (fd, h, size, buf)) {
	    ret = send_msg (fd, buf);
	    if (add_new_msg (fd, h, size, buf))
		arla_warnx (ADEBERROR, 
			    "xfs_send_message_multiple_list: "
			    "add_new_msg failed");
	}
	h = (struct xfs_message_header *) (((unsigned char *)h) + size);
	num--;
    }
    if (ret) {
	free (buf);
	return ret;
    }
    ret = send_msg (fd, buf);
    free (buf);
    return ret;
}
#endif

/*
 * Send multiple message to the kernel (for performace/simple resons)
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
    if (ret)
      warn << "xfs_send_message_wakeup_vmultiple: failed sending messages with error " << ret << "\n";

    msg.header.opcode = XFS_MSG_WAKEUP;
    msg.header.size  = sizeof(msg);
    msg.header.sequence_num = seqnums[fd]++;
    msg.sleepers_sequence_num = seqnum;
    msg.error = error;

    ++sent_stat[XFS_MSG_WAKEUP];

    warn << "multi-sending wakeup: seq = " << seqnum << ", error = " << error << "\n";

    ret = kern_write (fd, &msg, sizeof(msg));
    if (ret != sizeof(msg)) {
      warn << errno << "xfs_send_message_wakeup_vmultiple: writev";
	return -1;
    }
    return 0;
}

/*
 * Same as above but diffrent.
 */

int
xfs_send_message_wakeup_multiple (int fd,
				  u_int seqnum,
				  int error,
				  ...)
{
    va_list args;
    int ret;

    va_start (args, error);
    ret = xfs_send_message_wakeup_vmultiple (fd, seqnum, error, args);
    va_end (args);
    return ret;
}


