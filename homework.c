/*
 * file:        homework.c
 * description: skeleton file for CS 7600 homework 3
 *
 * CS 7600, Intensive Computer Systems, Northeastern CCIS
 * Peter Desnoyers, November 2015
 */

#define FUSE_USE_VERSION 27

#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>

#include "fs7600.h"
#include "blkdev.h"

/* prototypes */
int path_translate(const char *, struct path_trans *);

extern int homework_part;       /* set by '-part n' command-line option */

/* 
 * disk access - the global variable 'disk' points to a blkdev
 * structure which has been initialized to access the image file.
 *
 * NOTE - blkdev access is in terms of 1024-byte blocks
 */
extern struct blkdev *disk;

/*
 * cache - you'll need to create a blkdev which "wraps" this one
 * and performs LRU caching with write-back.
 */
int cache_nops(struct blkdev *dev) 
{
    struct blkdev *d = dev->private;
    return d->ops->num_blocks(d);
}
int cache_read(struct blkdev *dev, int first, int n, void *buf)
{
    return SUCCESS;
}
int cache_write(struct blkdev *dev, int first, int n, void *buf)
{
    return SUCCESS;
}
struct blkdev_ops cache_ops = {
    .num_blocks = cache_nops,
    .read = cache_read,
    .write = cache_write
};
struct blkdev *cache_create(struct blkdev *d)
{
    struct blkdev *dev = malloc(sizeof(*d));
    dev->ops = &cache_ops;
    dev->private = d;
    return dev;
}

/* by defining bitmaps as 'fd_set' pointers, you can use existing
 * macros to handle them. 
 *   FD_ISSET(##, inode_map);
 *   FD_CLR(##, block_map);
 *   FD_SET(##, block_map);
 */
fd_set *inode_map;              /* = malloc(sb.inode_map_size * FS_BLOCK_SIZE); */
fd_set *data_map;
struct fs7600_inode *inodes;

/* starting points in block numbers */
const uint32_t inode_map_start = 1;
uint32_t data_map_start, inode_start, data_start;


/* init - this is called once by the FUSE framework at startup. Ignore
 * the 'conn' argument.
 * recommended actions:
 *   - read superblock
 *   - allocate memory, read bitmaps and inodes
 */
void* fs_init(struct fuse_conn_info *conn)
{
    struct fs7600_super sb;
    if (disk->ops->read(disk, 0, 1, &sb) < 0)
    {
        fprintf(stderr, "failed to read superblock from disk.\n");
        exit(1);
    }

    /* allocate memory for bitmaps and inodes */
    inode_map = (fd_set *) malloc(sb.inode_map_sz * FS_BLOCK_SIZE);
    if (inode_map == NULL)
    {
        fprintf(stderr, "malloc failed for inode_map: %s\n", strerror(errno));
        exit(1);
    }

    data_map = (fd_set *) malloc(sb.block_map_sz * FS_BLOCK_SIZE);
    if (data_map == NULL)
    {
        fprintf(stderr, "malloc failed for data_map: %s\n", strerror(errno));
        exit(1);
    }

    inodes = (struct fs7600_inode *) malloc(sb.inode_region_sz * FS_BLOCK_SIZE);
    if (inodes == NULL)
    {
        fprintf(stderr, "malloc failed for inodes: %s\n", strerror(errno));
        exit(1);
    }

    /*  read bitmaps and inodes into memory */
    data_map_start = inode_map_start + sb.inode_map_sz;
    inode_start    = data_map_start  + sb.block_map_sz;
    data_start     = inode_start     + sb.inode_region_sz;

    if (disk->ops->read(disk, inode_map_start, sb.inode_map_sz, inode_map) < 0)
    {
        fprintf(stderr, "failed to read inode map from disk.\n");
        exit(1);
    }

    if (disk->ops->read(disk, data_map_start, sb.block_map_sz, data_map) < 0)
    {
        fprintf(stderr, "failed to read data map from disk.\n");
        exit(1);
    }

    if (disk->ops->read(disk, inode_start, sb.inode_region_sz, inodes) < 0)
    {
       fprintf(stderr, "failed to read inodes from disk.\n");
        exit(1);
    }

    if (homework_part > 3)
        disk = cache_create(disk);

    return NULL;
}

/* Note on path translation errors:
 * In addition to the method-specific errors listed below, almost
 * every method can return one of the following errors if it fails to
 * locate a file or directory corresponding to a specified path.
 *
 * ENOENT - a component of the path is not present.
 * ENOTDIR - an intermediate component of the path (e.g. 'b' in
 *           /a/b/c) is not a directory
 */

/* note on splitting the 'path' variable:
 * the value passed in by the FUSE framework is declared as 'const',
 * which means you can't modify it. The standard mechanisms for
 * splitting strings in C (strtok, strsep) modify the string in place,
 * so you have to copy the string and then free the copy when you're
 * done. One way of doing this:
 *
 *    char *_path = strdup(path);
 *    int inum = translate(_path);
 *    free(_path);
 */

/* getattr - get file or directory attributes. For a description of
 *  the fields in 'struct stat', see 'man lstat'.
 *
 * Note - fields not provided in CS7600fs are:
 *    st_nlink - always set to 1
 *    st_atime, st_ctime - set to same value as st_mtime
 *
 * errors - path translation, ENOENT
 */
static int fs_getattr(const char *path, struct stat *sb)
{
    struct path_trans pt;
    uint32_t inode_index;
    int ret;

    ret = path_translate(path, &pt);
    if (ret < 0)
        return ret;

    inode_index  = pt.inode_index;
    sb->st_ino   = inode_index;
    sb->st_mode  = inodes[inode_index].mode;
    sb->st_nlink = 1;
    sb->st_uid   = inodes[inode_index].uid;
    sb->st_gid   = inodes[inode_index].gid;
    sb->st_atime = inodes[inode_index].mtime;
    sb->st_mtime = inodes[inode_index].mtime;
    sb->st_ctime = inodes[inode_index].ctime;

    /* values not set */
    // sb->st_dev     =
    // sb->st_rdev    =
    // sb->st_blksize =
    // sb->st_blocks  =

    /*
     * TODO: if path == /, set atime to time(NULL) ?
     */
    return SUCCESS;
}

/* readdir - get directory contents.
 *
 * for each entry in the directory, invoke the 'filler' function,
 * which is passed as a function pointer, as follows:
 *     filler(buf, <name>, <statbuf>, 0)
 * where <statbuf> is a struct stat, just like in getattr.
 *
 * Errors - path resolution, ENOTDIR, ENOENT
 */
static int fs_readdir(const char *path, void *ptr, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
    return -EOPNOTSUPP;
}

/* see description of Part 2. In particular, you can save information 
 * in fi->fh. If you allocate memory, free it in fs_releasedir.
 */
static int fs_opendir(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

static int fs_releasedir(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

/* mknod - create a new file with permissions (mode & 01777)
 *
 * Errors - path resolution, EEXIST
 *          in particular, for mknod("/a/b/c") to succeed,
 *          "/a/b" must exist, and "/a/b/c" must not.
 *
 * If a file or directory of this name already exists, return -EEXIST.
 * If this would result in >32 entries in a directory, return -ENOSPC
 * if !S_ISREG(mode) return -EINVAL [i.e. 'mode' specifies a device special
 * file or other non-file object]
 */
static int fs_mknod(const char *path, mode_t mode, dev_t dev)
{
    return -EOPNOTSUPP;
}

/* mkdir - create a directory with the given mode.
 * Errors - path resolution, EEXIST
 * Conditions for EEXIST are the same as for create. 
 * If this would result in >32 entries in a directory, return -ENOSPC
 *
 * Note that you may want to combine the logic of fs_mknod and
 * fs_mkdir. 
 */ 
static int fs_mkdir(const char *path, mode_t mode)
{
    return -EOPNOTSUPP;
}

/* truncate - truncate file to exactly 'len' bytes
 * Errors - path resolution, ENOENT, EISDIR, EINVAL
 *    return EINVAL if len > 0.
 */
static int fs_truncate(const char *path, off_t len)
{
    /* you can cheat by only implementing this for the case of len==0,
     * and an error otherwise.
     */
    if (len != 0)
	return -EINVAL;		/* invalid argument */
    return -EOPNOTSUPP;
}

/* unlink - delete a file
 *  Errors - path resolution, ENOENT, EISDIR
 * Note that you have to delete (i.e. truncate) all the data.
 */
static int fs_unlink(const char *path)
{
    return -EOPNOTSUPP;
}

/* rmdir - remove a directory
 *  Errors - path resolution, ENOENT, ENOTDIR, ENOTEMPTY
 */
static int fs_rmdir(const char *path)
{
    return -EOPNOTSUPP;
}

/* rename - rename a file or directory
 * Errors - path resolution, ENOENT, EINVAL, EEXIST
 *
 * ENOENT - source does not exist
 * EEXIST - destination already exists
 * EINVAL - source and destination are not in the same directory
 *
 * Note that this is a simplified version of the UNIX rename
 * functionality - see 'man 2 rename' for full semantics. In
 * particular, the full version can move across directories, replace a
 * destination file, and replace an empty directory with a full one.
 */
static int fs_rename(const char *src_path, const char *dst_path)
{
    return -EOPNOTSUPP;
}

/* chmod - change file permissions
 * utime - change access and modification times
 *         (for definition of 'struct utimebuf', see 'man utime')
 *
 * Errors - path resolution, ENOENT.
 */
static int fs_chmod(const char *path, mode_t mode)
{
    return -EOPNOTSUPP;
}

int fs_utime(const char *path, struct utimbuf *ut)
{
    return -EOPNOTSUPP;
}

/* read - read data from an open file.
 * should return exactly the number of bytes requested, except:
 *   - if offset >= file len, return 0
 *   - if offset+len > file len, return bytes from offset to EOF
 *   - on error, return <0
 * Errors - path resolution, ENOENT, EISDIR
 */
static int fs_read(const char *path, char *buf, size_t len, off_t offset,
		    struct fuse_file_info *fi)
{
    return -EOPNOTSUPP;
}

/* write - write data to a file
 * It should return exactly the number of bytes requested, except on
 * error.
 * Errors - path resolution, ENOENT, EISDIR
 *  return EINVAL if 'offset' is greater than current file length.
 *  (POSIX semantics support the creation of files with "holes" in them, 
 *   but we don't)
 */
static int fs_write(const char *path, const char *buf, size_t len,
		     off_t offset, struct fuse_file_info *fi)
{
    return -EOPNOTSUPP;
}

static int fs_open(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

static int fs_release(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

/* statfs - get file system statistics
 * see 'man 2 statfs' for description of 'struct statvfs'.
 * Errors - none. 
 */
static int fs_statfs(const char *path, struct statvfs *st)
{
    /* needs to return the following fields (set others to zero):
     *   f_bsize = BLOCK_SIZE
     *   f_blocks = total image - metadata
     *   f_bfree = f_blocks - blocks used
     *   f_bavail = f_bfree
     *   f_namelen = <whatever your max namelength is>
     *
     * this should work fine, but you may want to add code to
     * calculate the correct values later.
     */
    st->f_bsize = FS_BLOCK_SIZE;
    st->f_blocks = 0;           /* probably want to */
    st->f_bfree = 0;            /* change these */
    st->f_bavail = 0;           /* values */
    st->f_namemax = 27;

    return 0;
}

/* operations vector. Please don't rename it, as the skeleton code in
 * misc.c assumes it is named 'fs_ops'.
 */
struct fuse_operations fs_ops = {
    .init = fs_init,
    .getattr = fs_getattr,
    .opendir = fs_opendir,
    .readdir = fs_readdir,
    .releasedir = fs_releasedir,
    .mknod = fs_mknod,
    .mkdir = fs_mkdir,
    .unlink = fs_unlink,
    .rmdir = fs_rmdir,
    .rename = fs_rename,
    .chmod = fs_chmod,
    .utime = fs_utime,
    .truncate = fs_truncate,
    .open = fs_open,
    .read = fs_read,
    .write = fs_write,
    .release = fs_release,
    .statfs = fs_statfs,
};

/*
 *  Helper functions
 */

/*
 * Path transtation
 */

int path_translate(const char *path, struct path_trans *pt)
{
    uint32_t i, block_number, inode_index;
    const char * delimiter = "/";
    char *pathc, *pathcc, *token;
    bool found;

    if (path == NULL || strlen(path) == 0)
    {
        return -EOPNOTSUPP;
    }

    /* break the string into componets
     * strdup is safe for the purposes of this assignment
     */
    pathc = strdup(path);
    /* need to free the strdup memory. 
     * pointer "pathc" is lost after replacing pathc with NULL later
     * as required by strtok.
     */
    pathcc = pathc;
    if (!pathc)
        return -ENOMEM;

    /* path is the rootdir, hence return it */
    if (strcmp(pathc, delimiter) == 0)
    {
        pt->inode_index = 1;
        return 0;
    }

    /* allocate memory for one data block */
    struct fs7600_dirent * dblock = (struct fs7600_dirent *) malloc (FS_BLOCK_SIZE);
    if (!dblock)
        return -ENOMEM;

    /* point to the rootdir to start the search */
    inode_index = 1;
    while (token = strtok(pathc, delimiter))
    {
        /* needed by strtok after getting the first token */
        if (pathc)
            pathc = NULL;

        /* search only inside directories */
        if (!S_ISDIR(inodes[inode_index].mode))
            return -ENOENT;

        /* fetch the data block from the disk.
         * data bitmap block is checked relative actual block number.
         * we should re-fetch the block even if it is the same 
         * in case some other process invalided it.
         * TODO: Is the above logic ok? Because if the block is the same, we could save a block fetch.
         */
        block_number = inodes[inode_index].direct[0];
        if (FD_ISSET(block_number - data_start, data_map))
            disk->ops->read(disk, block_number, 1, dblock);
        else
            return -ENOENT;

        /* try to find the entry's inode number for each component of the path */
        found = false;
        for (i = 0; i < DIRENT_PER_BLK; i++)
        {
            if (dblock[i].valid && strcmp(token, dblock[i].name) == 0)
            {
                inode_index  = dblock[i].inode;
                found = true;
                break;
            }
        }
    }

    if (!found)
        return -ENOENT;

    pt->inode_index = inode_index;
    free(pathcc);

    return 0;
}

