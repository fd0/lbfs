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
#ifndef _SFSLBCD_H_
#define _SFSLBCD_H_

#define export _export		/* C++ keyword gets used in C */

#include <sys/param.h>
#include <sys/mount.h>

#if defined(HAVE_DIRENT_H)
#include <dirent.h>
#endif
#if DIRENT_AND_SYS_DIR_H
#include <sys/dir.h>
#endif

#include <stdarg.h>
#include <xfs/xfs_message.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "lbfs_prot.h"
#include "xfs-sfs.h"
#include "cache.h"

#ifndef MOUNT_XFS
#define MOUNT_XFS       "xfs"           /* xfs */
#endif

/* xfs.C */
void xfs_message_init (void);
int  xfs_message_send (int fd, struct xfs_message_header *h, u_int size);
void  xfs_message_receive (int fd, struct xfs_message_header *h, u_int size);
int  xfs_send_message_wakeup (int fd, u_int seqnum, int error);
int  xfs_send_message_wakeup_multiple (int fd, u_int seqnum, int error, ...);
int  xfs_send_message_wakeup_vmultiple (int fd,	u_int seqnum, int error, 
					va_list args);
template<class T> inline T *
msgcast (ref<xfs_message_header> h)
{
  return reinterpret_cast<T *> (h.get ());
}

struct xfscall {

  xfs_cred *cred;
  u_int32_t opcode;
  int inst;
  int fd;
  time_t rqtime;
  ref<xfs_message_header> argp;
  void *resp[5];

  xfscall (u_int32_t oc, int file_des, ref<xfs_message_header> ap, xfs_cred *xc = NULL) : 
    cred(xc), opcode(oc), inst(-1), fd(file_des), argp(ap) { }
  ~xfscall () { }
  sfs_aid getaid () const { return xfscred2aid (cred); }
  sfs_aid getaid (const xfs_cred *xc) const { return xfscred2aid (xc); }
  ref<xfs_message_header> getarg () { return argp; }
  void *getvoidres (int i) { return resp[i]; }
};
  
typedef void (*xfs_message_function) (ref<xfscall>);
extern xfs_message_function rcvfuncs[XFS_MSG_COUNT];
/* xfs.C */

extern int lbcd_trace;

/* server.C */
extern ex_fsinfo3resok nfs_fsinfo;
//void nfs_dispatch (ref<xfscall>, time_t, clnt_stat err);
void xfs_wakeup (ref<xfscall>);
void xfs_getroot (ref<xfscall>);
void xfs_getnode (ref<xfscall>);
void xfs_getdata (ref<xfscall>);
void xfs_open (ref<xfscall>);
void xfs_getattr (ref<xfscall>);
void xfs_inactivenode (ref<xfscall>);
void xfs_putdata (ref<xfscall>);
void xfs_putattr (ref<xfscall>);
void xfs_create (ref<xfscall>);
void xfs_mkdir (ref<xfscall>);
void xfs_link (ref<xfscall>);
void xfs_symlink (ref<xfscall>);
void xfs_remove (ref<xfscall>);
void xfs_rmdir (ref<xfscall>);
void xfs_rename (ref<xfscall>);
void xfs_pioctl (ref<xfscall>);

void cbdispatch(svccb *sbp);
/* server.C */

/* helper.C */
#define NFS_MAXDATA 8192
#define LBFS_MAXDATA 65536

static inline int
compare_sha1_hash(unsigned char *data, size_t count, sfs_hash &hash)
{
  char h[sha1::hashsize];
  sha1_hash(h, data, count);
  return strncmp(h, hash.base(), sha1::hashsize);
}

void lbfs_getroot (int, ref<xfs_message_header>, sfs_aid, 
		   ref<aclnt> sc1, ref<aclnt> nc1);
void lbfs_getnode (int, ref<xfs_message_header>, sfs_aid, ref<aclnt>);
typedef callback<void, ptr<ex_getattr3res>, time_t, clnt_stat>::ptr attr_cb_t;
void lbfs_attr (int, ref<xfs_message_header>, sfs_aid, const nfs_fh3 &, 
		   ref<aclnt>, attr_cb_t);
//callback<void, const ex_getattr3res *, clnt_stat> = NULL);
void lbfs_open (int fd, ref<xfs_message_header>, sfs_aid sa, 
		ref<aclnt> c);
void lbfs_readexist (int fd, ref<xfs_message_header> h, cache_entry *e);
void lbfs_create (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		  ref<aclnt> c);
void lbfs_link (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		ref<aclnt> c);
void lbfs_symlink (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		   ref<aclnt> c);
void lbfs_setattr (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		   ref<aclnt> c);
void lbfs_remove (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		  ref<aclnt> c);
void lbfs_rename (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		  ref<aclnt> c);
void lbfs_putdata (int fd, ref<xfs_message_header> h, sfs_aid sa, 
		   ref<aclnt> c);
/* helper.C */

#undef export

#endif /* _SFSLBCD_H_ */

