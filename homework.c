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
void inode_to_stat(struct fs7600_inode *, uint32_t , struct stat *);
int fetch_inode_data_block(struct fs7600_inode * inode, case_level level, uint32_t i,
                           uint32_t j, uint32_t * idx_ary_second, bool fill_ary_second,
                           void * data);
int disk_write_inode(struct fs7600_inode, uint32_t);

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

    /* zero out everything */
    memset(sb, 0, sizeof(struct fs7600_inode));

    inode_index  = pt.inode_index;

    inode_to_stat( &(inodes[inode_index]), inode_index, sb );

    /*
     * TODO: if path == /, set atime to time(NULL)?
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
    struct path_trans pt;
    uint32_t i, inode_index, block_number;
    int ret;

    ret = path_translate(path, &pt);
    if (ret < 0)
        return ret;

    /* we check the validity of the inode and if the path is a directory.
     * we separate the if checks that results in more CPU cycles in order to return the correct error message.
     */
    inode_index = pt.inode_index;
    if (!FD_ISSET(inode_index, inode_map))
        return -ENOENT;

    if (!S_ISDIR(inodes[inode_index].mode))
        return -ENOTDIR;

    /* allocate memory for one data block
     * there is lots of code duplication from path_translate
     */
    struct fs7600_dirent * dblock = (struct fs7600_dirent *) malloc (FS_BLOCK_SIZE);
    if (!dblock)
        return -ENOMEM;

    /* fetch the inode's first data block and iterate in its contents */
    block_number = inodes[inode_index].direct[0];
    if (FD_ISSET(block_number - data_start, data_map))
        disk->ops->read(disk, block_number, 1, dblock);
    else
        return -ENOENT;

    for (i = 0; i < DIRENT_PER_BLK; i++)
    {
        if (dblock[i].valid)
        {
            struct stat sb;
            memset(&sb, 0, sizeof(sb));

            inode_index = dblock[i].inode;

            /* FUSE only checks inode number and mode of sb */
            inode_to_stat( &(inodes[inode_index]), inode_index, &sb );
            filler(ptr, dblock[i].name, &sb, 0);
        }
    }

    free(dblock);

    return SUCCESS;
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
    struct path_trans pt;
    uint32_t inode_index;
    int ret;

    ret = path_translate(path, &pt);
    if (ret < 0)
        return ret;

    /* we deal only with files and dirs */
    inode_index = pt.inode_index;
    if (S_ISDIR(inodes[inode_index].mode))
        inodes[inode_index].mode = mode | S_IFDIR;
    else if (S_ISREG(inodes[inode_index].mode))
        inodes[inode_index].mode = mode | S_IFREG;

    /* sync disk and memory */
    return disk_write_inode(inodes[inode_index], inode_index);
}

int fs_utime(const char *path, struct utimbuf *ut)
{
    struct path_trans pt;
    uint32_t inode_index;
    int ret;

    ret = path_translate(path, &pt);
    if (ret < 0)
        return ret;

    /* we deal only with files and dirs */
    inode_index = pt.inode_index;
    inodes[inode_index].ctime  = ut->actime;
    inodes[inode_index].mtime  = ut->modtime;

    /* sync disk and memory */
    return disk_write_inode(inodes[inode_index], inode_index);
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
    struct path_trans pt;
    struct fs7600_inode inode;
    char data[1024];
    uint32_t idx_ary_second[IDX_PER_BLK];
    uint32_t i, j, k, i2, j2, nbytes = 0;
    off_t size;

    case_level level;
    size_t pos_start, pos_final;

    /* These are indices in terms of absolute values,
     * i.e. a single file can have (6 + 256 + 256^2) pointers to data blocks
     * that are indexed starting by zero, i.e. 0..(6 + 1024 + 1024^2 - 1)
     */
    uint32_t abs_block_start_idx, abs_block_final_idx;

    /* These are indices in terms of relative values.
     * i.e. for direct and indir_1, first var ranges in 0..5 and 0..1023 respectively.
     * For indir_2, first and second vars range in 0..1023.
     */
    struct block_idx block_start_idx, block_final_idx;
    bool start_in_indir1, start_in_indir2;
    bool stay_in_direct, stay_in_indir1;

    int ret;

    /* Do some error checking */
    if (offset < 0)
    {
        /* TODO: next message should be printed in debug only mode? */
        fprintf(stderr, "Negative values for offset are not supported\n");
        return -EOPNOTSUPP;
    }

    ret = path_translate(path, &pt);
    if (ret < 0)
        return ret;

    inode = inodes[pt.inode_index];
    size  = inode.size;
    if (offset >= size || len == 0)
        return 0;

    /* path must be a file */
    if (!S_ISREG(inode.mode))
        return -EISDIR;

    /* Same as: offset + len > size. Writing it in next form, we "cache" the (size - offset) calculation. */
    if (len > size - offset)
        len = size - offset;

    /* Based on the file size, we know that we have a valid offset and len. */
    /* These are the actual indexs of the first and last elements in the first and last data blocks */
    pos_start = offset % FS_BLOCK_SIZE;
    pos_final = (offset + len - 1) % FS_BLOCK_SIZE;

    abs_block_start_idx = (uint32_t) (offset / FS_BLOCK_SIZE);
    abs_block_final_idx = (uint32_t) ((offset + len - 1) / FS_BLOCK_SIZE);

    /* the check is in term of absolute values, so we check in increasing order:
     * N_DIRECT < (N_DIRECT + IDX_PER_BLK) < (N_DIRECT + IDX_PER_BLK ++)
     */
    if (abs_block_start_idx < N_DIRECT)
    {
        /* based on the (absolute) offset we start from some direct block */
        level = DIRECT;
        start_in_indir1 = false;
        start_in_indir2 = false;
        block_start_idx.first  = abs_block_start_idx;
        block_start_idx.second = 0; // not used in this case
    }
    else if (abs_block_start_idx < N_DIRECT + IDX_PER_BLK) // implied: && >= N_DIRECT
    {
        /* based on the (absolute) offset we start from some indir_1 block */
        level = INDIR_1;
        start_in_indir1 = true;
        start_in_indir2 = false;
        block_start_idx.first  = abs_block_start_idx - N_DIRECT;
        block_start_idx.second = 0; // not used in this case
    }
    else
    {
        /* based on the (absolute) offset we start from some indir_2 block */
        level = INDIR_2;
        start_in_indir1 = false;
        start_in_indir2 = true;
        block_start_idx.first  = (uint32_t) ( (abs_block_start_idx - N_DIRECT - IDX_PER_BLK) / IDX_PER_BLK );
        block_start_idx.second = (uint32_t) ( (abs_block_start_idx - N_DIRECT - IDX_PER_BLK) % IDX_PER_BLK );
    }

    /* same logic as above */
    if (abs_block_final_idx < N_DIRECT)
    {
        /* based on the (absolute) (offset + len) we finish in some direct block */
        stay_in_direct = true;
        stay_in_indir1 = false;
        block_final_idx.first  = abs_block_final_idx;
        block_final_idx.second = 0; // not used in this case
    }
    else if (abs_block_final_idx < N_DIRECT + IDX_PER_BLK) // implied: && >= N_DIRECT
    {
        /* based on the (absolute) (offset + len) we finish in some indir_1 block */
        stay_in_direct = false;
        stay_in_indir1 = true;
        block_final_idx.first  = abs_block_final_idx - N_DIRECT;
        block_final_idx.second = 0; // not used in this case
    }
    else
    {
        /* based on the (absolute) (offset + len) we finish in some indir_2 block */
        stay_in_direct = false;
        stay_in_indir1 = false;
        block_final_idx.first  = (uint32_t) ( (abs_block_final_idx - N_DIRECT - IDX_PER_BLK) / IDX_PER_BLK );
        block_final_idx.second = (uint32_t) ( (abs_block_final_idx - N_DIRECT - IDX_PER_BLK) % IDX_PER_BLK );
    }

    /* We jump to the next case without a break if we need to read more block
     * from the next level. I.e. we just from direct to indir_1 and from indir_1 to indir_2.
     * We assume that a file is stored sequentially in the sense that we start to fill
     * sequentially indir_1 after we have sequentially filled direct, and the same for indir_2.
     *
     */
    switch(level)
    {
        case DIRECT:
            i = block_start_idx.first;
            if (stay_in_direct)
                j = block_final_idx.first;
            else
                j = N_DIRECT;

            ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, false, data);
            if (ret < 0)
                return ret;

            /* there is the case where we start and end in the same block
             * so just grab the data and exit the switch.
             */
            if (stay_in_direct && i == j) //i == j, should be enought
            {
                /* grab the data fom pos_start to pos_final */
                memcpy(buf, &data[pos_start], pos_final - pos_start + 1);
                nbytes = pos_final - pos_start + 1;
                break;
            }

            /* (i != j) && (don't care about stay_in_direct), so we can put and else here.
             * grab the data fom pos_start to the end of the block
             */
            memcpy(buf+nbytes, &data[pos_start], FS_BLOCK_SIZE - pos_start);
            nbytes += FS_BLOCK_SIZE - pos_start;

            /* we immediately move to the next block, i.e. ++i, after the memcpy above */
            while (++i < j)
            {
                ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_direct)
            {
                ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, false, data);
                if (ret < 0)
                    return ret;

                /* grab the first pos_final data only */
                memcpy(buf+nbytes, data, pos_final + 1);
                nbytes += pos_final + 1;
                break;
            }

        case INDIR_1:
            /* set the start and final block indices */
            if (start_in_indir1)
            {
                i = block_start_idx.first;
            }
            else
            {
                i = 0;
                /* we have crossed here from case DIRECT, therefore pos_start
                 * has been used and we can reset it here. This avoids puting the
                 * block fetch and memcpy inside the if-else stmt.
                 */
                pos_start = 0;
            }

            if (stay_in_indir1)
                j = block_final_idx.first;
            else
                j = IDX_PER_BLK;

            ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, NULL, false, data);
            if (ret < 0)
                return ret;

            /* there is the case where we start and end in the same block
             * so just grab the data and exit the switch.
             */
            if (i == j) //implies that stay_in_indir1 is also true, but that's not the criterion
            {
                /* grab the data fom pos_start to pos_final and exit the switch */
                if (start_in_indir1)
                {
                    memcpy(buf+nbytes, &data[pos_start], pos_final - pos_start + 1);
                    nbytes += pos_final - pos_start + 1;
                }
                else
                {
                    memcpy(buf+nbytes, data, pos_final + 1);
                    nbytes += pos_final + 1;
                }

                break;
            }

            /* if we start_in_indir1 then nbytes = 0 as expected and needed
             * else if we came from upstairs, i.e. case DIRECT, then we increase
             * the pointer accordingly.
             */
            memcpy(buf+nbytes, &data[pos_start], FS_BLOCK_SIZE - pos_start);
            nbytes += FS_BLOCK_SIZE - pos_start;

            /* the rest of the logic is the same as in the DIRECT case. */

            /* we immediately move to the next block, i.e. ++i, after the memcpy above */
            while (++i < j)
            {
                ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, NULL, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_indir1)
            {
                ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, NULL, false, data);
                if (ret < 0)
                    return ret;

                /* grab the first pos_final data only */
                memcpy(buf+nbytes, data, pos_final + 1);
                nbytes += pos_final + 1;

                break;
            }

        case INDIR_2:
            /* set the start and final block indices */
            if (start_in_indir2)
            {
                i = block_start_idx.first;
                j = block_start_idx.second;
            }
            else
            {
                i = 0;
                j = 0;
                /* we have crossed from case INDIR_1, therefore pos_start
                 * has been used and we can reset it here. This avoids puting the
                 * block fetch and memcpy inside the if-else stmt.
                 */
                pos_start = 0;
            }

            i2 = block_final_idx.first;
            j2 = block_final_idx.second;
            ret = fetch_inode_data_block(&inode, INDIR_2, i, j, idx_ary_second, true, data);
            if (ret < 0)
                return ret;


            /* there is the case where we start and end in the same row.
             * so just grab the data of the respected columns and exit the switch.
             */
            if (i == i2)
            {
                /* this if-else stmt uses the same row, so we don't need to look
                 * into another indir_2 row
                 */
                if (j == j2)    //same row and column
                {
                    if (start_in_indir2)
                    {
                        memcpy(buf+nbytes, &data[pos_start], pos_final - pos_start + 1);
                        nbytes += pos_final - pos_start + 1;
                    }
                    else
                    {
                        memcpy(buf+nbytes, data, pos_final + 1);
                        nbytes += pos_final + 1;
                    }
                }
                else    // grab the data from the next rows, i.e. more that one
                {
                    /* Copy the last columns of the last row.
                     * Must go until j2-1 in order to pull the last data with j2 and pos_final
                     */
                    for (k = j; k < j2; k++)
                    {
                        ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
                        if (ret < 0)
                            return ret;

                        memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                        nbytes += FS_BLOCK_SIZE;
                    } //for

                    ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
                    if (ret < 0)
                        return ret;

                    memcpy(buf+nbytes, data, pos_final + 1);
                    nbytes += pos_final + 1;
                }

                break;
            } // if (i == i2)

            /* its  the case that i < i2 so we read the remaining parts of that row */
            memcpy(buf+nbytes, &data[pos_start], FS_BLOCK_SIZE - pos_start);
            nbytes += FS_BLOCK_SIZE - pos_start;

            for (k = j+1; k < IDX_PER_BLK; k++)
            {
                ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }//for k


            while (++i < i2)
            {
                /* Load next row, from column 0 and fill idx_ary_second
                 * We fill idx_ary_second when changing i in INDIR_2.
                 */
                ret = fetch_inode_data_block(&inode, INDIR_2, i, 0, idx_ary_second, true, data);
                if (ret < 0)
                    return ret;

                for (k = 0; k < IDX_PER_BLK; k++)
                {
                    /* load the columns using our cached idx_ary_second */
                    ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
                    if (ret < 0)
                        return ret;

                    memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                    nbytes += FS_BLOCK_SIZE;
                }//for
            } //while

            /* load the last row, from column 0 and fill idx_ary_second */
            ret = fetch_inode_data_block(&inode, INDIR_2, i, 0, idx_ary_second, true, data);
            if (ret < 0)
                return ret;

            /* Copy the last columns of the last row.
             * Must go until j2-1 in order to pull the last data with j2 and pos_final
             */
            for (k = 0; k < j2; k++)
            {
                ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }//for

            /* load the last column */
            ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_second, false, data);
            if (ret < 0)
                return ret;

            memcpy(buf+nbytes, data, pos_final + 1);
            nbytes += pos_final + 1;

            break; //for fun
    }//switch

    return nbytes;
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
 *
 * on error returns: -EOPNOTSUPP, -ENOMEM, -ENOENT (most common)
 * on success returns SUCCESS and the inode number of path,
 * that is within the struct path_trans.
 */

int path_translate(const char *path, struct path_trans *pt)
{
    uint32_t i, block_number, inode_index;
    const char * delimiter = "/";
    char *pathc, *token;
    bool found;

    if (path == NULL || strlen(path) == 0)
    {
        return -EOPNOTSUPP;
    }

    /* break the string into componets
     * strdup is safe for the purposes of this assignment
     */
    pathc = strdup(path);
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
    token = strtok(pathc, delimiter);
    while (token)
    {
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

        /* find the next token */
        token = strtok(NULL, delimiter);
    }

    if (!found)
        return -ENOENT;

    pt->inode_index = inode_index;
    free(pathc);
    free(dblock);

    return SUCCESS;
}

/* fill a stat struct with inode's information.
 * the caller is responsible for zero'ing the stat struct, sb.
 * we pay the overhead of calling a function instead of doing it in place.
 */
void inode_to_stat(struct fs7600_inode * inode, uint32_t inode_index, struct stat * sb)
{
    sb->st_ino   = inode_index;
    sb->st_mode  = inode->mode;
    sb->st_nlink = 1;
    sb->st_uid   = inode->uid;
    sb->st_gid   = inode->gid;
    sb->st_size  = inode->size;
    sb->st_atime = inode->mtime;
    sb->st_mtime = inode->mtime;
    sb->st_ctime = inode->ctime;
    /* values not set */
    // sb->st_dev     =
    // sb->st_rdev    =
    // sb->st_blksize =
    // sb->st_blocks  =

    return;
}

/* depending on the level, we fetch the corresponding data block.
 * we pay the overhead of calling a function with a lot of args
 */
int fetch_inode_data_block(struct fs7600_inode * inode, case_level level, uint32_t i,
                           uint32_t j, uint32_t * idx_ary_second, bool fill_ary_second,
                           void * data)
{
    uint32_t block_number;
    uint32_t idx_ary_first[IDX_PER_BLK];

    switch(level)
    {
        case DIRECT:
            block_number = inode->direct[i];
            if (FD_ISSET(block_number - data_start, data_map))
                disk->ops->read(disk, block_number, 1, data);
            else
                return -ENOENT;

            break;

        case INDIR_1:
            /* is indir_1 valid? */
            if (FD_ISSET(inode->indir_1 - data_start, data_map))
                disk->ops->read(disk, inode->indir_1, 1, idx_ary_first);
            else
                return -ENOENT;

            /* only fetch data if valid */
            block_number = idx_ary_first[i];
            if (FD_ISSET(block_number - data_start, data_map))
                disk->ops->read(disk, block_number, 1, data);
            else
                return -ENOENT;

            break;

        case INDIR_2:
            if (fill_ary_second)
            {
                /* is indir_2 valid? (1st level of indices) */
                if (FD_ISSET(inode->indir_2 - data_start, data_map))
                    disk->ops->read(disk, inode->indir_2, 1, idx_ary_first);
                else
                    return -ENOENT;

            /* We can fill the columns array in the first call and use it for subsequent calls.
             * That is the purpose of this boolean variable.
             */

                /* is indir_2[i] valid? (2nd level of indices) */
                block_number = idx_ary_first[i];
                if (FD_ISSET(block_number - data_start, data_map))
                    disk->ops->read(disk, block_number, 1, idx_ary_second);
                else
                    return -ENOENT;
            }

            /* only fetch data if valid */
            block_number = idx_ary_second[j];
            if (FD_ISSET(block_number - data_start, data_map))
                disk->ops->read(disk, block_number, 1, data);
            else
                return -ENOENT;

            break; //for fun
    }

    return SUCCESS;
}

/* write an inode onto the disk */
int disk_write_inode(struct fs7600_inode inode, uint32_t inode_index)
{
        struct fs7600_inode inode_block[INODES_PER_BLK];

        uint32_t block_number = inode_start + (inode_index / INODES_PER_BLK);
        /* index of the inode within its block */
        uint32_t iblock_index = inode_index % INODES_PER_BLK;

        /* load the inode block */
        disk->ops->read(disk, block_number, 1, inode_block);

        /* change the contents of the inode */
        inode_block[ iblock_index ] = inode;

        /* write the block back to disk */
        disk->ops->write(disk, block_number, 1, inode_block);

        return SUCCESS;
}
