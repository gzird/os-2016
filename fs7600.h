/*
 * file:        cs7600fs.h
 * description: Data structures for CS 7600 homework 3 file system.
 *
 * CS 7600, Intensive Computer Systems, Northeastern CCIS
 * Peter Desnoyers,  November 2015
 */
#ifndef __CS7600FS_H__
#define __CS7600FS_H__

#define FS_BLOCK_SIZE 1024
#define FS7600_MAGIC 0x37363030

/* Entry in a directory
 */
struct fs7600_dirent {
    uint32_t valid : 1;
    uint32_t isDir : 1;
    uint32_t inode : 30;
    char name[28];              /* with trailing NUL */
};

/* Superblock - holds file system parameters. 
 */
struct fs7600_super {
    uint32_t magic;
    uint32_t inode_map_sz;       /* in blocks */
    uint32_t inode_region_sz;    /* in blocks */
    uint32_t block_map_sz;       /* in blocks */
    uint32_t num_blocks;         /* total, including SB, bitmaps, inodes */
    uint32_t root_inode;        /* always inode 1 */

    /* pad out to an entire block */
    char pad[FS_BLOCK_SIZE - 6 * sizeof(uint32_t)]; 
};

#define N_DIRECT 6
struct fs7600_inode {
    uint16_t uid;
    uint16_t gid;
    uint32_t mode;
    uint32_t ctime;
    uint32_t mtime;
     int32_t size;
    uint32_t direct[N_DIRECT];
    uint32_t indir_1;
    uint32_t indir_2;
    uint32_t pad[3];            /* 64 bytes per inode */
};

struct path_trans {
    uint32_t inode_index;
};

struct block_idx {
    uint32_t first, second;     /* two levels of indices */
};

enum {BITS_PER_BLK   = FS_BLOCK_SIZE * 8};
enum {INODE_SIZE     = sizeof(struct fs7600_inode)};
enum {DIRENT_PER_BLK = FS_BLOCK_SIZE / sizeof(struct fs7600_dirent)};
enum {INODES_PER_BLK = FS_BLOCK_SIZE / sizeof(struct fs7600_inode)};
enum {IDX_PER_BLK    = FS_BLOCK_SIZE / sizeof(uint32_t)};
typedef enum {false, true} bool;
typedef enum {DIRECT, INDIR_1, INDIR_2} case_level;

/* directory entry cache element for part 3
 * /a/b => inode(/) + name(a) => inode(a)
 *           src    +  name   =    dst
 */
#define DCACHE_SIZE 50
struct dce {
    uint32_t src, dst;       /* source and destination inodes */
    time_t   tm;
    char     name[28];
    bool     valid;
};

/* write-back cache element for part 4
 * we keep the cache data in a separate array.
 */
#define DIRTY_SIZE 10
#define CLEAN_SIZE 30
struct wbce {
    uint32_t block_number;       /* block number */
    time_t   tm;
    bool     valid;
};

#endif

