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

#ifndef _KERNEL_H_
#define _KERNEL_H_

#include <xfs/xfs_message.h>
#include "sfslbcd.h"
#include "xfs.h"

void kernel_interface(struct kernel_args *args);

int kernel_opendevice(const char *dev);

extern int kernel_fd;

ssize_t
kern_read(int fd, void *data, size_t len);

ssize_t
kern_write (int fd, const void *data, size_t len);

void akernel();
void skernel(); //may not use

#endif

