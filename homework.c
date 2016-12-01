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
#include <libgen.h>
#include <time.h>

#include "fs7600.h"
#include "blkdev.h"

/* prototypes */
int path_translate(const char *, struct path_trans *, struct fuse_file_info *);
void inode_to_stat(struct fs7600_inode *, uint32_t , struct stat *);
int fetch_inode_data_block(struct fs7600_inode *, case_level, uint32_t,
                           uint32_t, uint32_t *, uint32_t *, bool, bool,
                           void *);
int disk_write_inode(struct fs7600_inode, uint32_t);
int mknod_mkdir_helper(const char *, mode_t, bool);
int validate_inode_data_block(struct fs7600_inode *, uint32_t,
                              case_level, uint32_t, uint32_t,
                              uint32_t *, uint32_t *, uint32_t *);
int free_data_block_search(uint32_t *);
int write_data_block(const char *, uint32_t, uint32_t, uint32_t);
int disk_write_bitmaps(bool, bool);
int unlink_rmdir_helper(const char *, bool);
/*
 * LRU cache
 */
uint32_t dcache_search(uint32_t, char *);
void dcache_add(struct dce);
void dcache_remove(uint32_t, char *);


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
uint32_t num_blocks, num_inodes, num_dblocks;
uint32_t max_filesize;

/* part3 cache LRU de cache of 50 entries */
struct    dce * dcache;
int       dcache_count = 0;     /* items currently in cache */


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

    /* zero out the bitmaps */
    FD_ZERO(inode_map);
    FD_ZERO(data_map);

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

    num_blocks    = sb.num_blocks;
    num_dblocks   = sb.num_blocks - data_start;
    num_inodes    = sb.inode_region_sz * INODES_PER_BLK;
    max_filesize  = ( N_DIRECT
                      + (FS_BLOCK_SIZE)/sizeof(uint32_t)
                      + (FS_BLOCK_SIZE*FS_BLOCK_SIZE)/(sizeof(uint32_t)*sizeof(uint32_t))
                    ) * FS_BLOCK_SIZE;

    /* part 3 - directory entry cache init */
    if (homework_part > 2)
    {
        dcache = (struct dce *) calloc(DCACHE_SIZE, sizeof(struct dce));
        if (dcache == NULL)
        {
            fprintf(stderr, "calloc failed for directory entry cache with %d elements: %s\n", DCACHE_SIZE, strerror(errno));
            exit(1);
        }
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

    ret = path_translate(path, &pt, NULL);
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


    /* part 2 caching */
    if (homework_part > 1)
    {
        ret = fi->flags;
        if (ret < 0)
            return ret;

        pt.inode_index = (uint32_t) fi->fh;
    }
    else
    {
        /* part 1 */
        ret = path_translate(path, &pt, NULL);
        if (ret < 0)
            return ret;
    }

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
    struct fs7600_dirent * dblock = (struct fs7600_dirent *) calloc (DIRENT_PER_BLK, sizeof(struct fs7600_dirent));
    if (!dblock)
        return -ENOMEM;

    /* fetch the inode's first data block and iterate in its contents */
    block_number = inodes[inode_index].direct[0];
    if (block_number && FD_ISSET(block_number - data_start, data_map))
        disk->ops->read(disk, block_number, 1, dblock);
    else
    {
        free(dblock);
        return -ENOENT;
    }

    for (i = 0; i < DIRENT_PER_BLK; i++)
    {
        if (dblock[i].valid && FD_ISSET(dblock[i].inode, inode_map))
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

    if (homework_part > 1)
    {
        struct path_trans pt;
        int ret;

        fi->fh = 0;

        ret = path_translate(path, &pt, fi);

        fi->fh    = (uint64_t) pt.inode_index;
        fi->flags = ret;                        /* abuse the flags */
    }

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
    bool isDir = false;     /* make clear what's the last argument */

    return mknod_mkdir_helper(path, mode, isDir);
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
    bool isDir = true;     /* make clear what's the last argument */

    return mknod_mkdir_helper(path, mode, isDir);
}

/* truncate - truncate file to exactly 'len' bytes
 * Errors - path resolution, ENOENT, EISDIR, EINVAL
 *    return EINVAL if len > 0.
 */
static int fs_truncate(const char *path, off_t len)
{
    struct path_trans pt;
    struct fs7600_inode inode;
    struct block_idx block_final_idx;
    uint32_t i, j, i2, j2;
    uint32_t inode_index, block_number;
    uint32_t idx_ary_first[IDX_PER_BLK], idx_ary_second[IDX_PER_BLK];
    uint32_t abs_block_final_idx;
    bool stay_in_direct, stay_in_indir1;
    case_level level;
    int ret;

    /* you can cheat by only implementing this for the case of len==0,
     * and an error otherwise.
     */
    if (len != 0)
        return -EINVAL;		/* invalid argument */

    ret = path_translate(path, &pt, NULL);
    if (ret < 0)
        return ret;

    inode_index = pt.inode_index;
    inode       = inodes[inode_index];

    /* only truncate files */
    if (!S_ISREG(inode.mode))
        return -EISDIR;

    if (inode.size == 0)
        return SUCCESS;

    abs_block_final_idx = (uint32_t) ((inode.size - 1) / FS_BLOCK_SIZE);

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


    /* len = 0 => level = DIRECT
     * we need the switch for later, to support len > 0.
     */
    level = DIRECT;

    switch(level)
    {
        case DIRECT:            /* for len = 0 we always start from here */
            if (stay_in_direct)
                j = block_final_idx.first + 1;
            else
                j = N_DIRECT;

            for (i = 0; i < j; i++)
            {
                block_number = inode.direct[i];
                if (block_number && FD_ISSET(block_number - data_start, data_map))
                {
                    FD_CLR(block_number - data_start, data_map);
                    inode.direct[i] = 0;
                }
                else
                {
                    return -EINVAL; // should not happen
                }
            }// for i

            if (stay_in_direct)
                break;

        case INDIR_1:
            if (stay_in_indir1)
                j = block_final_idx.first + 1;
            else
                j = IDX_PER_BLK;

            block_number = inode.indir_1;
            if (block_number && FD_ISSET(block_number - data_start, data_map))
            {
                /* load the first level of pointer and clear them */
                disk->ops->read(disk, block_number, 1, idx_ary_first);
                for (i = 0; i < j; i++)
                {
                    block_number = idx_ary_first[i];
                    if (block_number && FD_ISSET(block_number - data_start, data_map))
                    {
                        FD_CLR(block_number - data_start, data_map);
                        idx_ary_first[i] = 0;
                    }
                    else
                    {
                        return -EINVAL; // should not happen
                    }
                }// for i

                /* write cleared pointer to disk and clear ndir_1 pointer */
                disk->ops->read(disk, inode.indir_1, 1, idx_ary_first);
                FD_CLR(inode.indir_1 - data_start, data_map);
                inode.indir_1 = 0;
            }
            else
            {
                return -EINVAL; // should not happen
            }// if indir_1

            if (stay_in_indir1)
                break;

        case INDIR_2:
            i2 = block_final_idx.first  + 1 ;

            block_number = inode.indir_2;
            if (!block_number || !FD_ISSET(block_number - data_start, data_map))
                return -EINVAL; //should not happen

            /* load the first level of pointers and iterate until the second to last row, i.e. i2 - 1 */
            disk->ops->read(disk, block_number, 1, idx_ary_first);
            for (i = 0; i < i2; i++)
            {
                block_number = idx_ary_first[i];
                if (!block_number || !FD_ISSET(block_number - data_start, data_map))
                    return -EINVAL; //should not happen

                /* read the second level of pointers and clear it */
                disk->ops->read(disk, block_number, 1, idx_ary_second);

                /* TODO: we should have done this also in read, write.
                 * That is, pay the cost of a few if checks, to recude the lines of code by
                 * not taking outside the loops the initial and last cases.
                 */
                if (i < i2 - 1)
                    j2 = IDX_PER_BLK;
                else
                    j2 = block_final_idx.second + 1;


                for (j = 0; j < j2; j++)
                {
                    block_number = idx_ary_second[j];
                    if (block_number && FD_ISSET(block_number - data_start, data_map))
                    {
                        FD_CLR(block_number - data_start, data_map);
                        idx_ary_second[j] = 0;
                    }
                    else
                    {
                        return -EINVAL; //should not happen
                    }
                }

                /* clear the first level of pointer, after clearing the second level */
                FD_CLR(idx_ary_first[i] - data_start, data_map);
                idx_ary_first[i] = 0;
            }

            /* finally clear indir_2 */
            FD_CLR(inode.indir_2 - data_start, data_map);
            inode.indir_2 = 0;

            break; //for fun
    } // switch

    inode.size = 0;
    inodes[inode_index] = inode;
    disk_write_inode(inode, inode_index);
    disk_write_bitmaps(false, true);

    return SUCCESS;
}

/* unlink - delete a file
 *  Errors - path resolution, ENOENT, EISDIR
 * Note that you have to delete (i.e. truncate) all the data.
 */
static int fs_unlink(const char *path)
{
    bool isDir = false;

    return unlink_rmdir_helper(path, isDir);
}

/* rmdir - remove a directory
 *  Errors - path resolution, ENOENT, ENOTDIR, ENOTEMPTY
 */
static int fs_rmdir(const char *path)
{
    bool isDir = true;

    return unlink_rmdir_helper(path, isDir);
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
    struct path_trans pt;
    uint32_t block_number, parent_len, idx;
    char *pathc, *bname, *parent, *pathc2, *bname2;
    bool found_src = false, found_dst = false;
    int i, ret;

    pathc  = strdup(src_path);
    pathc2 = strdup(dst_path);

    if (!pathc || !pathc2)
        return -ENOMEM;

    /* get the basenames */
    bname  = basename(pathc);
    bname2 = basename(pathc2);

    /* get the parent direrntry */
    parent_len = strlen(src_path) - strlen(bname);
    parent = (char *) calloc(parent_len + 1,  sizeof(char));
    if (!parent)
        return -ENOMEM;

    strncpy(parent, src_path, parent_len);

    /* Cannot rename the root directory or use more that 27 chars for the new name */
    if (strcmp(src_path, "/") == 0 || strlen(bname2) > 27)
        return -EOPNOTSUPP;

    /* Cannot move to another directory */
    if (strncmp(parent, dst_path, parent_len))
        return -EINVAL;

    ret = path_translate(parent, &pt, NULL);
    if (ret < 0)
        return ret;

    struct fs7600_dirent * dblock = (struct fs7600_dirent *) calloc (DIRENT_PER_BLK, sizeof(struct fs7600_dirent));
    if (!dblock)
        return -ENOMEM;

    /* get the parent's direntry data block, if valid */
    block_number = inodes[pt.inode_index].direct[0];
    if (!block_number || !FD_ISSET(block_number - data_start, data_map))
        return -ENOENT;

    disk->ops->read(disk, block_number, 1, dblock);

    /* search for src/dst */
    for (i = 0; i < DIRENT_PER_BLK; i++)
    {
        if (dblock[i].valid && FD_ISSET(dblock[i].inode, inode_map))
        {
            if (strcmp(bname, dblock[i].name) == 0)
            {
                idx = i;
                found_src = true;
            }

            if (strcmp(bname2, dblock[i].name) == 0)
                found_dst = true;
        }
    }

    /* source does not exist? */
    if (!found_src)
    {
        free(dblock);
        free(parent);
        free(pathc);
        free(pathc2);
        return -ENOENT;
    }

    /* destination exists already? */
    if (found_dst)
    {
        free(dblock);
        free(parent);
        free(pathc);
        free(pathc2);
        return -EEXIST;
    }

    /* copy new name, write to disk and free the memory */
    strcpy(dblock[idx].name, bname2);
    disk->ops->write(disk, block_number, 1, dblock);

    free(dblock);
    free(parent);
    free(pathc);
    free(pathc2);

    return SUCCESS;
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

    ret = path_translate(path, &pt, NULL);
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

    ret = path_translate(path, &pt, NULL);
    if (ret < 0)
        return ret;

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
    uint32_t idx_ary_first[IDX_PER_BLK], idx_ary_second[IDX_PER_BLK];
    uint32_t i, j, k, i2, j2, nbytes = 0;
    off_t size;

    case_level level;
    size_t pos_start, pos_final;

    /* These are indices in terms of absolute values,
     * i.e. a single file can have (6 + 256 + 256^2) pointers to data blocks
     * and the data is indexed starting by zero, i.e. 0..(6 + 1024 + 1024^2 - 1)
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

    /* part 2 caching */
    if (homework_part > 1)
    {
        ret = fi->flags;
        if (ret < 0)
            return ret;

        pt.inode_index = (uint32_t) fi->fh;
    }
    else
    {
        /* part 1 */
        ret = path_translate(path, &pt, NULL);
        if (ret < 0)
            return ret;
    }

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

    /* The check is performed in terms of absolute values and in increasing order:
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

            ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, NULL, false, false, data);
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

            /* (i != j) && (don't care about stay_in_direct), so we could put and else stmt here.
            * grab the data fom pos_start to the end of the block
            */
            memcpy(buf+nbytes, &data[pos_start], FS_BLOCK_SIZE - pos_start);
            nbytes += FS_BLOCK_SIZE - pos_start;

            /* we immediately move to the next block, i.e. ++i, after the memcpy above */
            while (++i < j)
            {
                ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, NULL, false, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_direct)
            {
                ret = fetch_inode_data_block(&inode, DIRECT, i, 0, NULL, NULL, false, false, data);
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

            // TODO: cache the idx_ary_first here
            ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, idx_ary_first, NULL, true, false, data);
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
                ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, idx_ary_first, NULL, false, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_indir1)
            {
                ret = fetch_inode_data_block(&inode, INDIR_1, i, 0, idx_ary_first, NULL, false, false, data);
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
            ret = fetch_inode_data_block(&inode, INDIR_2, i, j, idx_ary_first, idx_ary_second, false, true, data);
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
                        ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
                        if (ret < 0)
                            return ret;

                        memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                        nbytes += FS_BLOCK_SIZE;
                    } //for

                    ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
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
                ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }//for k


            while (++i < i2)
            {
                /* Load next row, from column 0 (k = 0) and fill idx_ary_second
                * We fill idx_ary_second when changing i in INDIR_2.
                */
                ret = fetch_inode_data_block(&inode, INDIR_2, i, 0, idx_ary_first, idx_ary_second, false, true, data);        /* k = 0 */
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;

                for (k = 1; k < IDX_PER_BLK; k++)       /* k > 0, i.e. k = 1 */
                {
                    /* load the columns using our cached idx_ary_second */
                    ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
                    if (ret < 0)
                        return ret;

                    memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                    nbytes += FS_BLOCK_SIZE;
                }//for
            } //while

            /* Load the last row, from column 0 and fill idx_ary_second.
             * Here you cannot do a read after you fill idx_ary_second because the last for-loop
             * might not be executed, and hence you must only read the pos_final bytes.
             */
            ret = fetch_inode_data_block(&inode, INDIR_2, i, 0, idx_ary_first, idx_ary_second, false, true, data);
            if (ret < 0)
                return ret;

            /* Copy the last columns of the last row.
            * Must go until j2-1 in order to pull the last data with j2 and pos_final
            */
            for (k = 0; k < j2; k++)
            {
                ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
                if (ret < 0)
                    return ret;

                memcpy(buf+nbytes, data, FS_BLOCK_SIZE);
                nbytes += FS_BLOCK_SIZE;
            }//for

            /* load the last column */
            ret = fetch_inode_data_block(&inode, INDIR_2, i, k, idx_ary_first, idx_ary_second, false, false, data);
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
    struct path_trans pt;
    struct fs7600_inode inode;
    uint32_t i, j, k, i2, j2, n, nbytes = 0;
    uint32_t inode_index, block_number;
    uint32_t idx_ary_first[IDX_PER_BLK], idx_ary_second[IDX_PER_BLK];
    size_t size, pos_start, pos_final;
    case_level level;

    int ret, ret2;

    /* These are indices in terms of absolute values,
    * i.e. a single file can have (6 + 256 + 256^2) pointers to data blocks
    * and the data is indexed starting by zero, i.e. 0..(6 + 1024 + 1024^2 - 1)
    */
    uint32_t abs_block_start_idx, abs_block_final_idx;

    /* These are indices in terms of relative values.
     * i.e. for direct and indir_1, first var ranges in 0..5 and 0..1023 respectively.
     * For indir_2, first and second vars range in 0..1023.
     */
    struct block_idx block_start_idx, block_final_idx;
    bool start_in_indir1, start_in_indir2;
    bool stay_in_direct, stay_in_indir1;


    if (homework_part > 1)
    {
        ret = fi->flags;
        pt.inode_index = fi->fh;
        if (ret < 0)
        {
            /* if file does not exist create it, otherwise something else went wrong */
            if (ret == -ENOENT)
            {
                mode_t mode = 01644 | S_IFREG;
                ret2 = fs_mknod(path, mode, 0);
                if (ret2 < 0)
                    return ret2;
                else
                {
                    path_translate(path, &pt, NULL);  /* get the inode_index of the newly created file */
                    fi->fh = pt.inode_index;          /* cache the inode of the new file */
                }
            }
            else
            {
                return ret;
            }
        }

    }
    else
    {
        /* part 1 */
        ret = path_translate(path, &pt, NULL);
        if (ret < 0)
        {
            /* if file does not exist create it, otherwise something else went wrong */
            if (ret == -ENOENT)
            {
                mode_t mode = 01644 | S_IFREG;
                ret2 = fs_mknod(path, mode, 0);
                if (ret2 < 0)
                    return ret2;
                else
                    path_translate(path, &pt, NULL);  /* get the inode_index of the newly created file */
            }
            else
            {
                return ret;
            }
        }
    }

    inode_index = pt.inode_index;
    inode       = inodes[inode_index];
    size        = inode.size;

    /* Write only to files. We only deal with files and dirs, that why
     * a check !S_ISREG is accompanied by a return value of -EISDIR.
     */
    if (!S_ISREG(inode.mode))
        return -EISDIR;

    if (offset + len > max_filesize)
    {
        fprintf(stderr, "The maximum supported filesize is %lu\n", (unsigned long) max_filesize);
        return -EOPNOTSUPP;
    }

    /* file data are within 0..size-1. offset can take value(s):
     * i)  0..size-1 to support overwrite
     * ii) size to support append
     */
    if (offset > size)
    {
        return -EINVAL;
    }

    /* Beyond this line, offset and len are validated
     * The logic is very similar to fs_read. Instead of fetching blocks to read,
     * we are looking for free blocks to write.
     */

    /* Based on the file size, we know that we have a valid offset and len. */
    /* These are the actual indexs of the first and last elements in the first and last data blocks */
    pos_start = offset % FS_BLOCK_SIZE;
    pos_final = (offset + len - 1) % FS_BLOCK_SIZE;

    abs_block_start_idx = (uint32_t) (offset / FS_BLOCK_SIZE);
    abs_block_final_idx = (uint32_t) ((offset + len - 1) / FS_BLOCK_SIZE);

    /* The check is performed in terms of absolute values and in increasing order:
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
    */
    switch(level)
    {
        case DIRECT:
            i = block_start_idx.first;
            if (stay_in_direct)
                j = block_final_idx.first;
            else
                j = N_DIRECT;

            ret = validate_inode_data_block(&inode, inode_index, DIRECT, i, 0, NULL, NULL, &block_number);
            if (ret < 0)
                return ret;

            /* there is the case where we start and end in the same block
            * so just grab the data and exit the switch.
            */
            if (stay_in_direct && i == j) //i == j, should be enought
            {
                /* write the data from pos_start to pos_final */
                n = pos_final - pos_start + 1;
                write_data_block(buf+nbytes, block_number, pos_start, n);
                nbytes = n;
                break;
            }

            /* write to dblock, update inode size, flush updated inode and dblock to disk */
            n = FS_BLOCK_SIZE - pos_start;
            write_data_block(buf+nbytes, block_number, 0, n);
            nbytes += n;

            /* we immediately move to the next block, i.e. ++i, after the memcpy above */
            while (++i < j)
            {
                ret = validate_inode_data_block(&inode, inode_index, DIRECT, i, 0, NULL, NULL, &block_number);
                if (ret < 0)
                    return ret;

                n = FS_BLOCK_SIZE;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_direct)
            {
                ret = validate_inode_data_block(&inode, inode_index, DIRECT, i, 0, NULL, NULL, &block_number);
                if (ret < 0)
                    return ret;

                n = pos_final + 1;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;
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

            ret = validate_inode_data_block(&inode, inode_index, INDIR_1, i, 0, idx_ary_first, NULL, &block_number);
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
                    n = pos_final - pos_start + 1;
                    write_data_block(buf+nbytes, block_number, pos_start, n);
                    nbytes += n;
                }
                else
                {
                    n = pos_final + 1;
                    write_data_block(buf+nbytes, block_number, 0, n);
                    nbytes += n;
                }

                break;
            }

            /* write to dblock, update inode size, flush updated inode and dblock to disk */
            n = FS_BLOCK_SIZE - pos_start;
            write_data_block(buf+nbytes, block_number, 0, n);
            nbytes += n;

            /* the rest of the logic is the same as in the DIRECT case. */

            /* we immediately move to the next block, i.e. ++i */
            while (++i < j)
            {
                ret = validate_inode_data_block(&inode, inode_index, INDIR_1, i, 0, idx_ary_first, NULL, &block_number);
                if (ret < 0)
                    return ret;

                n = FS_BLOCK_SIZE;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;
            }

            /* copy the last of our data and exit the switch */
            if (stay_in_indir1)
            {
                ret = validate_inode_data_block(&inode, inode_index, INDIR_1, i, 0, idx_ary_first, NULL, &block_number);
                if (ret < 0)
                    return ret;

                n = pos_final + 1;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;

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
            ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, j, idx_ary_first, idx_ary_second, &block_number);
            if (ret < 0)
                return ret;


            /* This is the case where we start and end in the same row.
             * So just write the data of the respected columns and exit the switch.
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
                        n = pos_final - pos_start + 1;
                        write_data_block(buf+nbytes, block_number, pos_start, n);
                        nbytes += n;
                    }
                    else
                    {
                        n = pos_final + 1;
                        write_data_block(buf+nbytes, block_number, 0, n);
                        nbytes += n;
                    }
                }
                else    // write the data to the next rows, i.e. more that one
                {
                    /* Copy the last columns of the last row.
                     * Must go until j2-1 in order to write the last data with j2 and pos_final
                     */
                    for (k = j; k < j2; k++)
                    {
                        ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
                        if (ret < 0)
                            return ret;

                        n = FS_BLOCK_SIZE;
                        write_data_block(buf+nbytes, block_number, 0, n);
                        nbytes += n;
                    } //for

                    ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
                    if (ret < 0)
                        return ret;

                    n = pos_final + 1;
                    write_data_block(buf+nbytes, block_number, 0, n);
                    nbytes += n;
                }

                break;
            } // if (i == i2)

            /* it is the case that i < i2 so we write the remaining parts of that row */
            n = FS_BLOCK_SIZE - pos_start;
            write_data_block(buf+nbytes, block_number, pos_start, n);
            nbytes += n;


            for (k = j+1; k < IDX_PER_BLK; k++)
            {
                ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
                if (ret < 0)
                    return ret;

                n = FS_BLOCK_SIZE;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;
            }//for k


            while (++i < i2)
            {
                /* Load next row, from column 0 and fill idx_ary_second
                 * We fill idx_ary_second when changing i in INDIR_2.
                 * TODO: cache the indir_2 + idx_ary_first here, so save some reads,
                 * by taking k = 0, outside the loop. Only here it's possible.
                 */
                for (k = 0; k < IDX_PER_BLK; k++)
                {
                    /* load the columns using our cached idx_ary_second */
                    ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
                    if (ret < 0)
                        return ret;

                    n = FS_BLOCK_SIZE;
                    write_data_block(buf+nbytes, block_number, 0, n);
                    nbytes += n;
                }//for
            } //while

            /* Write the last columns of the last row.
             * Must go until j2-1 in order to write the last data with j2 and pos_final
             */
            for (k = 0; k < j2; k++)
            {
                ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
                if (ret < 0)
                    return ret;

                n = FS_BLOCK_SIZE;
                write_data_block(buf+nbytes, block_number, 0, n);
                nbytes += n;
            }//for

            /* write the last column */
            ret = validate_inode_data_block(&inode, inode_index, INDIR_2, i, k, idx_ary_first, idx_ary_second, &block_number);
            if (ret < 0)
                return ret;

            n = pos_final + 1;
            write_data_block(buf+nbytes, block_number, 0, n);
            nbytes += n;

            break; //for fun
    } //switch

    /* It is ok to update the inode size here, i.e. after the all writes happen,
     * because we also update the data bitmap here, hence if something
     * goes wrong before we reach here, on the next boot those data blocks
     * will be free.
     */

    inode.size += nbytes;           /* update the inode's size */
    inodes[inode_index] = inode;
    disk_write_inode(inode, inode_index);

    disk_write_bitmaps(false, true);

    return nbytes;
}

static int fs_open(const char *path, struct fuse_file_info *fi)
{
    if (homework_part > 1)
    {
        struct path_trans pt;
        int ret;

        fi->fh = 0;

        ret = path_translate(path, &pt, fi);

        fi->fh    = (uint64_t) pt.inode_index;
        fi->flags = ret;                        /* abuse the flags */
    }

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

    uint32_t i, c = 0;

    for (i = 0; i < num_dblocks; i++)
        if (!FD_ISSET(i, data_map))
                c++;


    st->f_bsize = FS_BLOCK_SIZE;
    st->f_blocks = num_dblocks;           /* probably want to */
    st->f_bfree = c;            /* change these */
    st->f_bavail = c;           /* values */
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
 * Helper functions
 *
 * TODO: Move the logic to a separate file.
 */

/*
 * Path transtation
 *
 * on error returns: -EOPNOTSUPP, -ENOMEM, -ENOENT (most common)
 * on success returns SUCCESS and the inode number of path,
 * that is within the struct path_trans.
 */

int path_translate(const char *path, struct path_trans *pt, struct fuse_file_info *fi)
{
    struct dce ce;
    uint32_t i, block_number, inode_index, dst = 0;
    const char * delimiter = "/";
    char *pathc, *token;
    bool found = false;

    if (path == NULL || strlen(path) == 0)
    {
        return -EOPNOTSUPP;
    }

    /* If fi != NULL means that the call is from open, opendir, read, readdir, write.
     * If also fh != 0 then the call is from read, readdir, write. i.e. we have already
     * cached the inode.
     */
    if (homework_part > 1 && fi != NULL && fi->fh)
    {
        pt->inode_index = fi->fh;
        return SUCCESS;
    }

    /* if path is the rootdir then return its inode num */
    if (strcmp(path, delimiter) == 0)
    {
        pt->inode_index = 1;
        return SUCCESS;
    }

    /* break the string into componets
     * strdup is safe for the purposes of this assignment
     */
    pathc = strdup(path);
    if (!pathc)
        return -ENOMEM;

    /* allocate memory for one data block */
    struct fs7600_dirent * dblock = (struct fs7600_dirent *) malloc (FS_BLOCK_SIZE);
    if (!dblock)
        return -ENOMEM;

    /* point to the rootdir and start the search recursively */
    inode_index = 1;
    token = strtok(pathc, delimiter);
    while (token)
    {
        /* search only inside directories */
        if (!S_ISDIR(inodes[inode_index].mode))
            return -ENOENT;

        found = false;      /* token is found? */
        switch(homework_part)
        {
            case 3:
            case 4:
                /* always keep the source index, in case we must cache the element */
                ce.src = inode_index;
                dst    = dcache_search(inode_index, token);     /* updates the time for hits */
                if (dst)
                {
                    found       = true;
                    inode_index = dst;
                    break;
                }

            /* parts 1, 2 or cache miss */
            default:
                /* Fetch the dir entry block from the disk */
                block_number = inodes[inode_index].direct[0];
                if (block_number && FD_ISSET(block_number - data_start, data_map))
                    disk->ops->read(disk, block_number, 1, dblock);
                else
                {
                    free(dblock);
                    free(pathc);
                    return -ENOENT;
                }

                /* try to find the entry's inode number for each component of the path */
                for (i = 0; i < DIRENT_PER_BLK; i++)
                {
                    if (dblock[i].valid
                        && strcmp(token, dblock[i].name) == 0
                        && FD_ISSET(dblock[i].inode, inode_map))
                    {
                        inode_index  = dblock[i].inode;
                        found = true;
                        break;
                    }
                }

                if (homework_part > 2 && found)
                {
                    ce.dst   = inode_index;
                    memset(ce.name, 0, sizeof(ce.name)); /* seem to be faster than                      */
                    strcpy(ce.name, token);              /* strncpy(ce.name, token, strlen(token));     */
                    ce.valid = true;
                    ce.tm    = time(NULL);
                    dcache_add(ce);
                }
        }//switch homework_part

        /* find the next token */
        token = strtok(NULL, delimiter);
    }//while token

    if (!found)
    {
        free(dblock);
        free(pathc);
        return -ENOENT;
    }

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
    sb->st_atime = inode->ctime;
    sb->st_mtime = inode->mtime;
    sb->st_ctime = inode->ctime;
    /* values not set */
    // sb->st_dev     =
    // sb->st_rdev    =
    // sb->st_blksize =
    // sb->st_blocks  =

    return;
}

/* Depending on the level, we fetch the corresponding data block.
 * i is an index for the case of DIRECT and INDIR_1.
 * i,j are indices for the case of INDIR_2.
 * We pay the overhead of calling a function with a lot of args.
 * So manu args...
 */
int fetch_inode_data_block(struct fs7600_inode * inode, case_level level, uint32_t i,
                           uint32_t j, uint32_t * idx_ary_first, uint32_t * idx_ary_second,
                           bool fill_ary_first, bool fill_ary_second, void * data)
{
    uint32_t block_number;

    switch(level)
    {
        case DIRECT:
            block_number = inode->direct[i];
            if (FD_ISSET(block_number - data_start, data_map))
                disk->ops->read(disk, block_number, 1, data);
            else
                return -EINVAL;

            break;

        case INDIR_1:
            switch (fill_ary_first)
            {
                case true:
                    /* is indir_1 valid? */
                    if (FD_ISSET(inode->indir_1 - data_start, data_map))
                        disk->ops->read(disk, inode->indir_1, 1, idx_ary_first);
                    else
                        return -EINVAL;

                default:
                    /* only fetch data if valid */
                    block_number = idx_ary_first[i];
                    if (FD_ISSET(block_number - data_start, data_map))
                        disk->ops->read(disk, block_number, 1, data);
                    else
                        return -EINVAL;
            }

            break;

        case INDIR_2:
            switch (fill_ary_second)
            {
                /* We can fill the columns array in the first call and use it for subsequent calls.
                * That is the purpose of this boolean variable.
                */
                case true:
                    /* is indir_2 valid? (1st level of indices) */
                    if (FD_ISSET(inode->indir_2 - data_start, data_map))
                        disk->ops->read(disk, inode->indir_2, 1, idx_ary_first);
                    else
                        return -EINVAL;

                    /* is indir_2[i] valid? (2nd level of indices) */
                    block_number = idx_ary_first[i];
                    if (FD_ISSET(block_number - data_start, data_map))
                        disk->ops->read(disk, block_number, 1, idx_ary_second);
                    else
                        return -EINVAL;

                default:
                    /* only fetch data if valid */
                    block_number = idx_ary_second[j];
                    if (block_number && FD_ISSET(block_number - data_start, data_map))
                        disk->ops->read(disk, block_number, 1, data);
                    else
                        return -EINVAL;
            }

            break; //for fun
    }

    return SUCCESS;
}

/* Write an inode to the disk.
 * The callers has updates the inodes array
 */
int disk_write_inode(struct fs7600_inode inode, uint32_t inode_index)
{
        struct fs7600_inode inode_block[INODES_PER_BLK];
        uint32_t i, block_number, iblock_start, iblock_index;


        block_number = inode_start + (inode_index / INODES_PER_BLK);

        /* what is the index in the inodes array, that this inode block starts */
        iblock_start = (inode_index / INODES_PER_BLK);
        /* index of the inode within its block */
        iblock_index = inode_index % INODES_PER_BLK;

        for (i = 0; i < INODES_PER_BLK; i++)
            inode_block[i] = inodes[ i + iblock_start ];

        inode_block[iblock_index] = inode;

        /* write the block back to disk */
        disk->ops->write(disk, block_number, 1, inode_block);

        return SUCCESS;
}

/* based on isDir we essentially run either mknod or mkfile */
int mknod_mkdir_helper(const char *path, mode_t mode, bool isDir)
{
    struct fs7600_inode inode;
    struct path_trans pt;
    char *pathc, *bname, *parent;
    uint32_t i, idx_free, parent_len, block_number, inode_index, entry_count = 0;
    bool found, new_block;
    int j, ret;

    pathc = strdup(path);
    if (!pathc)
        return -ENOMEM;

    /* get the basename of the file/dir */
    bname = basename(pathc);
    if (strlen(bname) > 27)
    {
        free(pathc);
        fprintf(stderr, "Maximum file or directory name is 27 characters\n");
        return -EINVAL;
    }

    parent_len = strlen(path) - strlen(bname);
    parent = (char *) calloc(parent_len + 1,  sizeof(char));
    if (!parent)
        return -ENOMEM;

    /* check if the parent path exists */
    strncpy(parent, path, parent_len);
    ret = path_translate(parent, &pt, NULL);
    if (ret < 0)
        return ret;

    /* parent path has indeed to be a directory */
    if (!S_ISDIR(inodes[pt.inode_index].mode))
        return -EINVAL;

    struct fs7600_dirent * dblock = (struct fs7600_dirent *) calloc (DIRENT_PER_BLK, sizeof(struct fs7600_dirent));
    if (!dblock)
        return -ENOMEM;

    block_number = inodes[pt.inode_index].direct[0];

    /* if parent directory is empty, find the next available data block
     * and assign it, else fetch the directory's entry block
     */
    found = false;
    if (block_number == 0 || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
    {
        for (i = 0; i < num_dblocks; i++)
            if (!FD_ISSET(i, data_map))
            {
                block_number = i + data_start;
                found        = true;
                new_block    = true;
                idx_free     = 0;
                inodes[pt.inode_index].direct[0] = block_number;
                FD_SET(i, data_map);
                break;
            }
    }
    else
    {
        disk->ops->read(disk, block_number, 1, dblock);
        found     = true;       /* becomes true if disk read does not abort/crash as it always succeeds */
        new_block = false;
    }

    /* there is no available data block */
    if (!found)
        return -ENOSPC;

    if (!new_block)
    {
        /* search the directory entries for free space or the same file/dir */
        found = false;
//        for (i = DIRENT_PER_BLK; i-- > 0;)
        for (i = 0; i < DIRENT_PER_BLK; i++)
        {
            if (dblock[i].valid && FD_ISSET(dblock[i].inode, inode_map))
            {
                /* count the valid entries */
                entry_count++;
                /* maybe the file/dir already exists */
                if (strcmp(bname, dblock[i].name) == 0)
                {
                    found = true;
                    break;
                }
            }
            else
            {
                idx_free = i;   /* keep track of the furthest free (invalid) direntry */
            }
        }

        /* same name exists? */
        if (found)
            return -EEXIST;

        /* is direntry full? */
        if (entry_count == DIRENT_PER_BLK)
            return -ENOSPC;
    } //if !new_block

    /* find the next available inode */
    found = false;
    for (i = 0; i < num_inodes; i++)
    {
        if (!FD_ISSET(i, inode_map))
        {
             found       = true;
             inode_index = i;
             FD_SET(i, inode_map);
             break;
        }
     }

     if (!found)
         return -ENOSPC;

    /* Do we have the right type of mode? */
    if ( (isDir && S_ISREG(mode)) || (!isDir && S_ISDIR(mode)) )
        return -EINVAL;

    /* set the parent's directory entry */
    dblock[idx_free].valid = 1;
    dblock[idx_free].isDir = isDir;
    dblock[idx_free].inode = inode_index;
    strcpy(dblock[idx_free].name, bname);

    /* write the updated direntry block back to disk */
    disk->ops->write(disk, block_number, 1, dblock);

    inode.uid   = getuid();
    inode.gid   = getgid();
    if (isDir)
        inode.mode  = (mode & 01777) | S_IFDIR;
    else
        inode.mode  = (mode & 01777) | S_IFREG;

    inode.ctime = time(NULL);
    inode.mtime = inode.ctime;      /* don't call time(NULL) again */
    inode.size  = 0;

    /* don't use any space for empty dir or empty file */
    for (j = 0; j < N_DIRECT; j++)
        inode.direct[j] = 0;

    inode.indir_1 = 0;
    inode.indir_2 = 0;

    /* update the memory */
    inodes[inode_index] = inode;

    /* sync the disk with memory */
    disk_write_inode(inodes[inode_index], inode_index);

    /* only a new block changes the data bitmap */
    if (new_block)
        disk_write_bitmaps(true, true);
    else
        disk_write_bitmaps(true, false);


    free(dblock);
    free(parent);
    free(pathc);

    return SUCCESS;
}

/* We check if an inode's data block is valid so that we can write.
 * If not, we search for an available data block and assign that to the inode's pointer.
 * i is an index for the case of DIRECT and INDIR_1.
 * i,j are indices for the case of INDIR_2.
 *
 * writes to inode, idx_ary_{first, second}, block_number
 *
 * TODO: So many args...
 */
int validate_inode_data_block(struct fs7600_inode * inode, uint32_t inode_index,
                              case_level level, uint32_t i, uint32_t j,
                              uint32_t * idx_ary_first, uint32_t * idx_ary_second,
                              uint32_t * ret_block_number)
{
    uint32_t block_number;
    int ret;

    switch(level)
    {
        case DIRECT:
            block_number = inode->direct[i];
            if (block_number == 0 || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    inode->direct[i] = block_number;
                    /* sync the inodes array and then the disk with inodes array */
                    inodes[inode_index] = *inode;
                    disk_write_inode(inodes[inode_index], inode_index);
                }
            }

            *ret_block_number = block_number;
            break;

        case INDIR_1:
            block_number = inode->indir_1;
            /* is indir_1 valid? */
            if (block_number == 0 || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                /* allocate a datablock for indir_1 */
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    inode->indir_1 = block_number;
                    /* sync the inodes array and then the disk with inodes array
                     * TODO: probably the disk write below can be delayed until when allocating
                     * the first data block for indir_1, since indir_1 was invalid in the first place.
                     */
                    inodes[inode_index] = *inode;
                    disk_write_inode(inodes[inode_index], inode_index);
                }
            }

            /* read the first level of pointers */
            disk->ops->read(disk, block_number, 1, idx_ary_first);

            block_number = idx_ary_first[i];

            if (block_number < data_start || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    idx_ary_first[i]  = block_number;
                    /* write the pointers of indir_1 back to disk */
                    disk->ops->write(disk, inode->indir_1, 1, idx_ary_first);
                }
            }

            *ret_block_number = block_number;
            break;

        case INDIR_2:
            /* is indir_2 valid? */
            block_number = inode->indir_2;
            if (block_number == 0 || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                /* allocate a datablock for indir_2 */
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    inode->indir_2 = block_number;
                    /* sync the inodes array and then the disk with inodes array
                     * TODO: probably the disk write below can be delayed until when allocating
                     * the first data block for indir_2, since indir_2 was invalid in the first place.
                     */
                    inodes[inode_index] = *inode;
                    disk_write_inode(inodes[inode_index], inode_index);
                }
            }

            /* read the first level of pointers */
            disk->ops->read(disk, block_number, 1, idx_ary_first);

            block_number = idx_ary_first[i];

            if (block_number < data_start || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    idx_ary_first[i]  = block_number;
                    /* write the pointers of indir_1 back to disk */
                    disk->ops->write(disk, inode->indir_2, 1, idx_ary_first);
                }
            }

            /* read the second level of pointers */
            disk->ops->read(disk, block_number, 1, idx_ary_second);

            block_number = idx_ary_second[j];

            if (block_number < data_start || block_number > num_blocks || !FD_ISSET(block_number - data_start, data_map))
            {
                ret = free_data_block_search( &block_number );
                if (ret < 0)
                    return ret;
                else
                {
                    idx_ary_second[j]  = block_number;
                    /* write the pointers of indir_2[i] back to disk */
                    disk->ops->write(disk, idx_ary_first[i], 1, idx_ary_second);
                }
            }

            *ret_block_number = block_number;
            break; //for fun
    }

    return SUCCESS;
}

/* Find the next available data block, if any */
int free_data_block_search(uint32_t * block_number)
{
    uint32_t i;

    for (i = 0; i < num_dblocks; i++)
    {
        if (!FD_ISSET(i, data_map))
        {
            FD_SET(i, data_map);
            *block_number = i + data_start;

            return SUCCESS;
        }
    }

    return -ENOSPC;
}

/* Write data to a dblock.
 * We load its data, if necessary
 * We change the data
 * We write the new data back to the disk
 */
int write_data_block(const char * buf, uint32_t block_number, uint32_t pos_start, uint32_t n)
{
    char data[FS_BLOCK_SIZE];

    /* When n = FS_BLOCK_SIZE, we are overwriting the block
     * so there is no need to read the block's data.
     */
    if (n < FS_BLOCK_SIZE || pos_start > 0)             /* actually one check implies the other... */
        disk->ops->read(disk, block_number, 1, data);

    memcpy(&data[pos_start], buf, n);

    disk->ops->write(disk, block_number, 1, data);

    return SUCCESS;
}

/*
 * Write inode and data bitmaps to disk
 */
int disk_write_bitmaps(bool write_inode, bool write_data)
{
    if (write_inode)
    {
        disk->ops->write(disk, inode_map_start, data_map_start - inode_map_start, inode_map);
    }

    if (write_data)
    {
        disk->ops->write(disk, data_map_start, inode_start - data_map_start, data_map);
    }

   return SUCCESS;
}

int unlink_rmdir_helper(const char *path, bool isDir)
{
    struct path_trans pt;
    uint32_t block_number, parent_len, inode_index, parent_block_number;
    char *pathc, *bname, *parent;
    bool found = false;
    int i, ret;

    /* Cannot delete the root directory */
    if (strcmp(path, "/") == 0 && isDir)
        return -EOPNOTSUPP;

    pathc = strdup(path);
    if (!pathc)
        return -ENOMEM;

    /* get the basename of the file */
    bname = basename(pathc);

    /* get the parent direrntry */
    parent_len = strlen(path) - strlen(bname);
    parent = (char *) calloc(parent_len + 1,  sizeof(char));
    if (!parent)
        return -ENOMEM;

    strncpy(parent, path, parent_len);
    ret = path_translate(parent, &pt, NULL);
    if (ret < 0)
        return ret;

    if (!S_ISDIR(inodes[pt.inode_index].mode))
        return -ENOENT;

    struct fs7600_dirent * dblock = (struct fs7600_dirent *) calloc (DIRENT_PER_BLK, sizeof(struct fs7600_dirent));
    if (!dblock)
        return -ENOMEM;

    /* get the parent's direntry data block, if valid */
    block_number = inodes[pt.inode_index].direct[0];
    if (!block_number || !FD_ISSET(block_number - data_start, data_map))
        return -ENOENT;

    disk->ops->read(disk, block_number, 1, dblock);
    parent_block_number = block_number;

    /* look for the filename/dir to delete */
    for (i = 0; i < DIRENT_PER_BLK; i++)
    {
        if (dblock[i].valid
            && strcmp(bname, dblock[i].name) == 0
            && FD_ISSET(dblock[i].inode, inode_map))
        {
            /* is it a dir when deleting a file? */
            if (!isDir && dblock[i].isDir)
            {
                free(dblock);
                return -EISDIR;
            }

            /* is it a file when deleting a dir? */
            if (isDir && !(dblock[i].isDir))
            {
                free(dblock);
                return -ENOTDIR;
            }

            dblock[i].valid = false;
            inode_index = dblock[i].inode;
            found = true;
            break;
        }
    }

    /* remove the entry from the cache. if it exists */
    if (homework_part > 2)
    {
        dcache_remove(pt.inode_index, bname);
    }

    free(parent);
    free(pathc);

    if (!found)
    {
        free(dblock);
        return -ENOENT;
    }
    else
    {
        if (isDir)
        {
            block_number = inodes[inode_index].direct[0];
            /* check if the block is valid */
            if (block_number && block_number < num_blocks && FD_ISSET(block_number - data_start, data_map))
            {
                struct fs7600_dirent * dir_dblock = (struct fs7600_dirent *) calloc (DIRENT_PER_BLK, sizeof(struct fs7600_dirent));
                if (!dir_dblock)
                    return -ENOMEM;

                /* now read a valid block */
                disk->ops->read(disk, block_number, 1, dir_dblock);
                for (i = 0; i < DIRENT_PER_BLK; i++)
                {
                    /* directory is not empty, so don't write anything to disk */
                    if (dir_dblock[i].valid && FD_ISSET(dir_dblock[i].inode, inode_map))
                    {
                        free(dblock);
                        free(dir_dblock);

                        return -ENOTEMPTY;
                    }
                }

                free(dir_dblock);
            }
        }
        else
        {
            /* truncate the file */
            ret = fs_truncate(path, 0);
            if (ret < 0)
            {
                free(dblock);
                return ret;
            }
        }

        /* clear the inode, write the direntry block */
        FD_CLR(inode_index, inode_map);
        disk->ops->write(disk, parent_block_number, 1, dblock);
        disk_write_bitmaps(true, false);
    }

    free(dblock);

    return SUCCESS;
}

/*
 * LRU directory entry cache functions
 */

/*
 * Search an entry
 */
uint32_t dcache_search(uint32_t src, char * name)
{
    int i;

    for (i = 0; i < DCACHE_SIZE; i++)
        if (dcache[i].valid
            && dcache[i].src == src
            && strcmp(dcache[i].name, name) == 0)
        {
            dcache[i].tm = time(NULL);

            return dcache[i].dst;
        }

    return 0; /* item not found in cache */
}

/*
 * Remove an entry, if it exists
 */
void dcache_remove(uint32_t src, char * name)
{
    int i;

    for (i = 0; i < DCACHE_SIZE; i++)
        if (dcache[i].src == src
            && strcmp(dcache[i].name, name) == 0
            && dcache[i].valid)
        {
            dcache[i].valid = false;
            dcache_count--;

            break;
        }
}

/*
 * Add an element to the cache
 */
void dcache_add(struct dce e)
{
    int i, idx;
    time_t tm;

    switch (dcache_count)
    {
        /* we replace an element */
        case DCACHE_SIZE:
            tm  = dcache[0].tm;
            idx = 0;

            for (i = 1; i < DCACHE_SIZE; i++)
            {
                if (dcache[i].tm < tm)
                {
                    tm  = dcache[i].tm;
                    idx = i;
                }
            }

            dcache[idx] = e;

            break;

        /* If cache is empty, we add an item in position 0 */
        case 0:
            dcache[0] = e;
            dcache_count++;

            break;

        /* default is to add an element.
         * Implies, count > 0 && count < size.
         */
        default:
            for (i = 0; i < DCACHE_SIZE; i++)
            {
                if (!dcache[i].valid)
                {
                    dcache[i] = e;
                    dcache_count++;

                    break;
                }
            }
    }//switch
}

