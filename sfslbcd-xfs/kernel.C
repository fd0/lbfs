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

#include <xfs/xfs_message.h>
#include "kernel.h"
#include "async.h"
#include "sfslbcd.h"
#include "dmalloc.h"

/*
 * The fd we use to talk with the kernel on.
 */

int kernel_fd = -1;

void
process_message (int msg_length, char *msg)
{
  struct xfs_message_header *header;
  char *p = msg;

  for (p = msg;
       msg_length > 0;
       p += header->size, msg_length -= header->size) {
    header = (struct xfs_message_header *) p;
    xfs_message_receive (kernel_fd, header, header->size);
  }
}

/*
 * The cdev communication unit
 */

static int
dev_open (const char *filename)
{
    char fn[MAXPATHLEN];
    snprintf (fn, MAXPATHLEN, "/%s", filename);
    return open (fn, O_RDWR);
}

static ssize_t
dev_read (int fd, void *msg, size_t len)
{
    return read (fd, msg, len);
}

static ssize_t
dev_write (int fd, const void *msg, size_t len)
{
    return write (fd, msg, len);
}


/*
 * Ways to communticate with the kernel
 */ 

struct kern_interface {
    char *prefix;
    int (*open) (const char *filename);
    ssize_t (*read) (int fd, void *msg, size_t len);
    ssize_t (*write) (int fd, const void *msg, size_t len);
} kern_comm[] = {
    { "/",	dev_open, dev_read, dev_write},
#if 0
    { "file:/",	dev_fileopen, dev_read, dev_write},
    { "tcpport:", tcp_open, tcp_read, tcp_write},
    { "tcp",	tcp_opendef, tcp_read, tcp_write},
    { "null",	null_open, null_read, null_write},
    { NULL }
#endif
} ;

struct kern_interface *kern_cur = NULL;

static int
kern_open (const char *filename)
{
    struct kern_interface *ki = &kern_comm[0];
    int len;

    while (ki->prefix) {
	len = strlen (ki->prefix);
	if (strncasecmp (ki->prefix, filename, len) == 0) {
	    break;
	}    
	ki++;
    }
    if (ki->prefix == NULL)
	return -1;
    kern_cur = ki;
    return (ki->open) (filename+len); 
}

ssize_t
kern_read (int fd, void *data, size_t len)
{
    assert (kern_cur != NULL);
    return (kern_cur->read) (fd, data, len);
}

ssize_t
kern_write (int fd, const void *data, size_t len)
{
    assert (kern_cur != NULL);
    return (kern_cur->write) (fd, data, len);
}

int
kernel_opendevice (const char *dev)
{

  int fd = kern_open (dev);
  if (fd < 0) {
    warn << strerror(errno) << ": kern_open " << dev << "\n";
    return -1;
  }
#if 0 /*Making fd async*/
  int err = ioctl(fd, FIONBIO, 1);
  if (err == -1) {
    warn << strerror(errno) << "(" << errno << ")" << ": ioctl " << dev << "\n";
    return -1;
  }
  make_async(fd);
  close_on_exec(fd);
#endif /*Making fd async*/

  kernel_fd = fd;
  return 0;
}

void akernel() {
  u_int32_t data_size = MAX_XMSG_SIZE;
  mstr data(MAX_XMSG_SIZE);
  int len = kern_read(kernel_fd, data.cstr(), data_size);

  if ((len == -1) && (errno != EAGAIN)) { 
    warn << "can't read from kernel_fd = " << kernel_fd << ":" 
	 << strerror(errno) << "\n";
    return;
  } else if (len == 0) {
    warn << "akernel: len = 0..wierd..maybe device is closed\n";
    fdcb(kernel_fd, selread, NULL);
    return;
  } else {
#if DEBUG > 0
    warn << "akernel: received data\n";
#endif
    process_message(len, data.cstr());
  }
  fdcb(kernel_fd, selread, wrap(&akernel));
}

