#ifndef	_PIOCTL_H_
#define	_PIOCTL_H_

#include <xfs/xfs_message.h>

int viocflushvolume (int fd, struct xfs_message_pioctl *h, u_int size);
int viocgetacl(int fd, struct xfs_message_pioctl *h, u_int size);
int viocsetacl(int fd, struct xfs_message_pioctl *h, u_int size);
int viocgetvolstat(int fd, struct xfs_message_pioctl *h, u_int size);
int viocsetvolstat(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_afs_stat_mt_pt(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_afs_delete_mt_pt(int fd, struct xfs_message_pioctl *h, u_int size);
int viocwhereis(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_get_cell(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_get_cellstatus(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_set_cellstatus(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_new_cell(int fd, struct xfs_message_pioctl *h, u_int size);
int viocgettok (int fd, struct xfs_message_pioctl *h, u_int size);
int viocsettok (int fd, struct xfs_message_pioctl *h, u_int size);
int viocunlog (int fd, struct xfs_message_pioctl *h, u_int size);
int viocflush (int fd, struct xfs_message_pioctl *h, u_int size);
int viocconnect(int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_fpriostatus (int fd, struct xfs_message_pioctl *h, u_int size);
int viocgetfid (int fd, struct xfs_message_pioctl *h, u_int size);
int viocvenuslog (int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_afs_sysname (int fd, struct xfs_message_pioctl *h, u_int size);
int viocfilecellname (int fd, struct xfs_message_pioctl *h, u_int size);
int viocgetwscell (int fd, struct xfs_message_pioctl *h, u_int size);
int viocsetcachesize (int fd, struct xfs_message_pioctl *h, u_int size);
int viocckserv (int fd, struct xfs_message_pioctl *h, u_int size);
int viocgetcacheparms (int fd, struct xfs_message_pioctl *h, u_int size);
int getrxkcrypt (int fd, struct xfs_message_pioctl *h, u_int size);
int setrxkcrypt (int fd, struct xfs_message_pioctl *h, u_int size);
int viocaviator (int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_arladebug (int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_gcpags (int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_calculate_cache (int fd, struct xfs_message_pioctl *h, u_int size);
int vioc_breakcallback(int fd, struct xfs_message_pioctl *h, u_int size);


#endif
