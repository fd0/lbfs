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

#ifndef __XFS_H_V
#define __XFS_H_V 1
#define DEBUG 3

#include <stdarg.h>
#include <xfs/xfs_message.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "lbfs_prot.h"
#include "xfs-sfs.h"
#include "cache.h"

/* xfs.C */
void xfs_message_init (void);
int  xfs_message_send (int fd, struct xfs_message_header *h, u_int size);
void  xfs_message_receive (int fd, struct xfs_message_header *h, u_int size);
int  xfs_send_message_wakeup (int fd, u_int seqnum, int error);
int  xfs_send_message_wakeup_multiple (int fd, u_int seqnum, int error, ...);
int  xfs_send_message_wakeup_vmultiple (int fd,	u_int seqnum, int error, 
					va_list args);
struct xfscall {

  xfs_cred *cred;
  u_int32_t opcode;
  int inst;
  int fd;
  time_t rqtime;
  void *const argp;
  void *resp[5];

  xfscall (u_int32_t oc, int file_des, void *const ap, xfs_cred *xc = NULL) : 
    cred(xc), opcode(oc), inst(-1), fd(file_des), argp(ap) { }
  ~xfscall () {
#if 0
    if (argp) 
      delete argp;
    for (int i=0; i<5; i++)
      if (resp[i])
	delete resp[i];
#endif
  }
  sfs_aid getaid () const { return xfscred2aid (cred); }
  void *getvoidarg () { return argp; }
  void *getvoidres (int i) { return resp[i]; }
};
  
typedef void (*xfs_message_function) (ref<xfscall>);
extern xfs_message_function rcvfuncs[XFS_MSG_COUNT];
/* xfs.C */

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

void lbfs_getroot (int, xfs_message_getroot &, sfs_aid, 
		   ref<aclnt> sc1, ref<aclnt> nc1);
void lbfs_getnode (int, xfs_message_getnode &, sfs_aid, ref<aclnt>);
void lbfs_getattr (int, xfs_message_getattr &, sfs_aid, const nfs_fh3 &, 
		   ref<aclnt>, callback<void, const ex_getattr3res *, clnt_stat>);
void lbfs_open (int fd, const xfs_message_open &h, sfs_aid sa, 
		ref<aclnt> c);
void lbfs_readexist (int fd, const xfs_message_getdata &h, cache_entry *e);
void lbfs_create (int fd, const xfs_message_create &h, sfs_aid sa, 
		  ref<aclnt> c);
void lbfs_link (int fd, const xfs_message_link &h, sfs_aid sa, 
		ref<aclnt> c);
void lbfs_symlink (int fd, const xfs_message_symlink &h, sfs_aid sa, 
		   ref<aclnt> c);

/* helper.C */

#endif /* __XFS_H_V */
