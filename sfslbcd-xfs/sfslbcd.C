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
#include "xfs.h"
#include "kernel.h"
#include "xfs-sfs.h"
#include "async.h"

char *device_file = "/dev/xfs0";
//char *hostpath = "new-york.lcs.mit.edu:85xq6pznt4mgfvj4mb23x6b8adak55ue";
char *hostpath = "pastwatch.lcs.mit.edu:ksg8iisdfirs62ncwewt7jb7v7g3hrsd";

int main(int argc, char **argv) {

  int err;

  if (argc != 3) {
    warn << "usage: " << argv[0] << " device_filename sfs_hostname:hostinfo\n";
    return -1;
  } else {
    device_file = new char[MAXPATHLEN];
    hostpath = new char[MAXPATHLEN];
    strcpy(device_file, argv[1]);
    strcpy(hostpath, argv[2]);
  }

  sigcb (SIGINT, wrap (exit, 1));
  sigcb (SIGTERM, wrap (exit, 1));

  if (mount(MOUNT_XFS, "/mnt", /*MNT_UNION*/0, device_file)) {
    if (errno == EOPNOTSUPP)
      warn << strerror(errno) << ":" << errno << "\n";
  }

  signal(SIGPIPE, SIG_IGN);

  xfs_message_init ();

  err = kernel_opendevice(device_file);
  if (err == -1) {
    warn << "failed to open " << device_file << "\n";
    return -1;
  }
  warn("Opened device file\n");
  
  sfsInit(hostpath); // or connect when usr sends a /sfs/hostname:hostid request

#if 0
  skernel();
#else
  warn << "calling fdcb on kernel_fd " << kernel_fd << "\n";
  fdcb(kernel_fd, selread, wrap(&akernel));
  amain();
#endif

  delete device_file;
  delete hostpath;
  return 0;
}
