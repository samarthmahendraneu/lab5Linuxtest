/*
 * file:        fs5600.c
 * description: skeleton file for CS 5600 system
 *
 * CS 5600, Computer Systems, Northeastern CCIS
 * Peter Desnoyers, November 2019
 *
 * Modified by CS5600 staff, fall 2021.
 */

#define FUSE_USE_VERSION 27
#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "fs5600.h"

/* if you don't understand why you can't use these system calls here,
 * you need to read the assignment description another time
 */
#define stat(a,b) error do not use stat()
#define open(a,b) error do not use open()
#define read(a,b,c) error do not use read()
#define write(a,b,c) error do not use write()


// === block manipulation functions ===

/* disk access.
 * All access is in terms of 4KB blocks; read and
 * write functions return 0 (success) or -EIO.
 *
 * read/write "nblks" blocks of data
 *   starting from block id "lba"
 *   to/from memory "buf".
 *     (see implementations in misc.c)
 */
extern int block_read(void *buf, int lba, int nblks);
extern int block_write(void *buf, int lba, int nblks);

/* Global variables */
static struct fs_super superblock;
static unsigned char block_bitmap[FS_BLOCK_SIZE];
static int next_free_blk = 3;

/* Write-back cache */
static int cache_valid = 0;
static int cache_lba = -1;
static int cache_dirty = 0;
static unsigned char cache_buf[FS_BLOCK_SIZE];

static int ic_valid = 0;
static int ic_inum = -1;
static int ic_dirty = 0;
static struct fs_inode ic_inode;

/* Constants for indirect pointers */
#define NUM_DIRECT 1016
#define NUM_INDIRECT 3
#define PTRS_PER_BLOCK (FS_BLOCK_SIZE / sizeof(uint32_t))

/* bitmap functions
 */
void bit_set(unsigned char *map, int i)
{
    map[i/8] |= (1 << (i%8));
}
void bit_clear(unsigned char *map, int i)
{
    map[i/8] &= ~(1 << (i%8));
}
int bit_test(unsigned char *map, int i)
{
    return map[i/8] & (1 << (i%8));
}


/*
 * Allocate a free block from the disk.
 *
 * success - return free block number
 * no free block - return -ENOSPC
 *
 * hint:
 *   - bit_set/bit_test might be useful.
 */
int alloc_blk() {
    for (int i = next_free_blk; i < superblock.disk_size; i++) {
        if (!bit_test(block_bitmap, i)) {
            bit_set(block_bitmap, i);
            next_free_blk = i + 1;
            if (block_write(block_bitmap, 1, 1) < 0) {
                return -EIO;
            }
            return i;
        }
    }
    return -ENOSPC;
}

/*
 * Return a block to disk, which can be used later.
 *
 * hint:
 *   - bit_clear might be useful.
 */
void free_blk(int i) {
    if (cache_valid && cache_lba == i) {
        cache_valid = 0;
        cache_dirty = 0;
        cache_lba = -1;
    }
    if (ic_valid && ic_inum == i) {
        ic_valid = 0;
        ic_dirty = 0;
        ic_inum = -1;
    }

    bit_clear(block_bitmap, i);
    block_write(block_bitmap, 1, 1);
}

/* Cache helper functions */
static int flush_block_cache(void) {
    if (cache_valid && cache_dirty) {
        if (block_write(cache_buf, cache_lba, 1) < 0) return -EIO;
        cache_dirty = 0;
    }
    return 0;
}

static int load_block_cache(int lba) {
    if (cache_valid && cache_lba == lba) return 0;
    int rv = flush_block_cache();
    if (rv < 0) return rv;

    if (block_read(cache_buf, lba, 1) < 0) return -EIO;
    cache_valid = 1;
    cache_lba = lba;
    cache_dirty = 0;
    return 0;
}

static int flush_inode_cache(void) {
    if (ic_valid && ic_dirty) {
        if (block_write(&ic_inode, ic_inum, 1) < 0) return -EIO;
        ic_dirty = 0;
    }
    return 0;
}

static int load_inode_cache(int inum) {
    if (ic_valid && ic_inum == inum) return 0;
    int rv = flush_inode_cache();
    if (rv < 0) return rv;

    if (block_read(&ic_inode, inum, 1) < 0) return -EIO;
    ic_valid = 1;
    ic_inum = inum;
    ic_dirty = 0;
    return 0;
}

/* Helper functions for indirect pointers */

/* Get block number for logical block index (supports indirect pointers)
 * Returns: block number or 0 if not allocated
 */
static int get_block_num(struct fs_inode *inode, int block_idx) {
    if (block_idx < 0) return 0;

    /* Direct pointers */
    if (block_idx < NUM_DIRECT) {
        return inode->ptrs[block_idx];
    }

    /* Indirect pointers */
    block_idx -= NUM_DIRECT;
    int indirect_idx = block_idx / PTRS_PER_BLOCK;
    int offset_in_indirect = block_idx % PTRS_PER_BLOCK;

    if (indirect_idx >= NUM_INDIRECT) {
        return 0;  /* Beyond max file size */
    }

    int indirect_block = inode->ptrs[NUM_DIRECT + indirect_idx];
    if (indirect_block == 0) {
        return 0;  /* Indirect block not allocated */
    }

    /* Read the indirect block */
    uint32_t indirect_ptrs[PTRS_PER_BLOCK];
    if (block_read(indirect_ptrs, indirect_block, 1) < 0) {
        return -EIO;
    }

    return indirect_ptrs[offset_in_indirect];
}

/* Set block number for logical block index (supports indirect pointers)
 * Returns: 0 on success, negative on error
 */
static int set_block_num(struct fs_inode *inode, int block_idx, int block_num) {
    if (block_idx < 0) return -EINVAL;

    /* Direct pointers */
    if (block_idx < NUM_DIRECT) {
        inode->ptrs[block_idx] = block_num;
        return 0;
    }

    /* Indirect pointers */
    block_idx -= NUM_DIRECT;
    int indirect_idx = block_idx / PTRS_PER_BLOCK;
    int offset_in_indirect = block_idx % PTRS_PER_BLOCK;

    if (indirect_idx >= NUM_INDIRECT) {
        return -ENOSPC;  /* Beyond max file size */
    }

    int indirect_block = inode->ptrs[NUM_DIRECT + indirect_idx];

    /* Allocate indirect block if needed */
    if (indirect_block == 0) {
        indirect_block = alloc_blk();
        if (indirect_block < 0) return indirect_block;

        inode->ptrs[NUM_DIRECT + indirect_idx] = indirect_block;

        /* Initialize indirect block with zeros */
        uint32_t indirect_ptrs[PTRS_PER_BLOCK];
        memset(indirect_ptrs, 0, sizeof(indirect_ptrs));
        if (block_write(indirect_ptrs, indirect_block, 1) < 0) {
            free_blk(indirect_block);
            inode->ptrs[NUM_DIRECT + indirect_idx] = 0;
            return -EIO;
        }
    }

    /* Read indirect block, update pointer, write back */
    uint32_t indirect_ptrs[PTRS_PER_BLOCK];
    if (block_read(indirect_ptrs, indirect_block, 1) < 0) {
        return -EIO;
    }

    indirect_ptrs[offset_in_indirect] = block_num;

    if (block_write(indirect_ptrs, indirect_block, 1) < 0) {
        return -EIO;
    }

    return 0;
}

/* Free all data blocks associated with a file (direct and indirect)
 */
static void free_all_blocks(struct fs_inode *inode) {
    /* Free direct blocks */
    for (int i = 0; i < NUM_DIRECT; i++) {
        if (inode->ptrs[i] != 0) {
            free_blk(inode->ptrs[i]);
            inode->ptrs[i] = 0;
        }
    }

    /* Free indirect blocks */
    for (int i = 0; i < NUM_INDIRECT; i++) {
        int indirect_block = inode->ptrs[NUM_DIRECT + i];
        if (indirect_block != 0) {
            /* Read the indirect block */
            uint32_t indirect_ptrs[PTRS_PER_BLOCK];
            if (block_read(indirect_ptrs, indirect_block, 1) >= 0) {
                /* Free all data blocks pointed to by this indirect block */
                for (int j = 0; j < PTRS_PER_BLOCK; j++) {
                    if (indirect_ptrs[j] != 0) {
                        free_blk(indirect_ptrs[j]);
                    }
                }
            }
            /* Free the indirect block itself */
            free_blk(indirect_block);
            inode->ptrs[NUM_DIRECT + i] = 0;
        }
    }
}


// === FS helper functions ===


/* Two notes on path translation:
 *
 * (1) translation errors:
 *
 *   In addition to the method-specific errors listed below, almost
 *   every method can return one of the following errors if it fails to
 *   locate a file or directory corresponding to a specified path.
 *
 *   ENOENT - a component of the path doesn't exist.
 *   ENOTDIR - an intermediate component of the path (e.g. 'b' in
 *             /a/b/c) is not a directory
 *
 * (2) note on splitting the 'path' variable:
 *
 *   the value passed in by the FUSE framework is declared as 'const',
 *   which means you can't modify it. The standard mechanisms for
 *   splitting strings in C (strtok, strsep) modify the string in place,
 *   so you have to copy the string and then free the copy when you're
 *   done. One way of doing this:
 *
 *      char *_path = strdup(path);
 *      int inum = ... // translate _path to inode number
 *      free(_path);
 */


/* EXERCISE 2:
 * convert path into inode number.
 *
 * how?
 *  - first split the path into directory and file names
 *  - then, start from the root inode (which inode/block number is that?)
 *  - then, walk the dirs to find the final file or dir.
 *    when walking:
 *      -- how do I know if an inode is a dir or file? (hint: mode)
 *      -- what should I do if something goes wrong? (hint: read the above note about errors)
 *      -- how many dir entries in one inode? (hint: read Lab4 instructions about directory inode)
 *
 * hints:
 *  - you can safely assume the max depth of nested dirs is 10
 *  - a bunch of string functions may be useful (e.g., "strtok", "strsep", "strcmp")
 *  - "block_read" may be useful.
 *  - "S_ISDIR" may be useful. (what is this? read Lab4 instructions or "man inode")
 *
 * programing hints:
 *  - there are several functionalities that you will reuse; it's better to
 *  implement them in other functions.
 */

int path2inum(const char *path) {
    if (strcmp(path, "/") == 0) {
        return 2;
    }

    char *_path = strdup(path);
    char *tokens[10];
    int token_count = 0;

    char *token = strtok(_path, "/");
    while (token != NULL && token_count < 10) {
        tokens[token_count++] = token;
        token = strtok(NULL, "/");
    }

    int current_inum = 2;
    struct fs_inode current_inode;

    for (int i = 0; i < token_count; i++) {
        if (block_read(&current_inode, current_inum, 1) < 0) {
            free(_path);
            return -EIO;
        }

        if (!S_ISDIR(current_inode.mode)) {
            free(_path);
            return -ENOTDIR;
        }

        struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
        if (block_read(dir_entries, current_inode.ptrs[0], 1) < 0) {
            free(_path);
            return -EIO;
        }

        int found = 0;
        for (int j = 0; j < NUM_DIRENT_BLOCK; j++) {
            if (dir_entries[j].valid && strcmp(dir_entries[j].name, tokens[i]) == 0) {
                current_inum = dir_entries[j].inode;
                found = 1;
                break;
            }
        }

        if (!found) {
            free(_path);
            return -ENOENT;
        }
    }

    free(_path);
    return current_inum;
}


/* EXERCISE 2:
 * Helper function:
 *   copy the information in an inode to struct stat
 *   (see its definition below, and the full Linux definition in "man lstat".)
 *
 *  struct stat {
 *        ino_t     st_ino;         // Inode number
 *        mode_t    st_mode;        // File type and mode
 *        nlink_t   st_nlink;       // Number of hard links
 *        uid_t     st_uid;         // User ID of owner
 *        gid_t     st_gid;         // Group ID of owner
 *        off_t     st_size;        // Total size, in bytes
 *        blkcnt_t  st_blocks;      // Number of blocks allocated
 *                                  // (note: block size is FS_BLOCK_SIZE;
 *                                  // and this number is an int which should be round up)
 *
 *        struct timespec st_atim;  // Time of last access
 *        struct timespec st_mtim;  // Time of last modification
 *        struct timespec st_ctim;  // Time of last status change
 *    };
 *
 *  [hints:
 *
 *    - what you should do is mostly copy.
 *
 *    - read fs_inode in fs5600.h and compare with struct stat.
 *
 *    - you can safely treat the types "ino_t", "mode_t", "nlink_t", "uid_t"
 *      "gid_t", "off_t", "blkcnt_t" as "unit32_t" in this lab.
 *
 *    - read "man clock_gettime" to see "struct timespec" definition
 *
 *    - the above "struct stat" does not show all attributes, but we don't care
 *      the rest attributes.
 *
 *    - for several fields in 'struct stat' there is no corresponding
 *    information in our file system:
 *      -- st_nlink - always set it to 1  (recall that fs5600 doesn't support links)
 *      -- st_atime - set to same value as st_mtime
 *  ]
 */

void inode2stat(struct stat *sb, struct fs_inode *in, uint32_t inode_num)
{
    memset(sb, 0, sizeof(*sb));

    sb->st_ino = inode_num;
    sb->st_mode = in->mode;
    sb->st_nlink = 1;
    sb->st_uid = in->uid;
    sb->st_gid = in->gid;
    sb->st_size = in->size;
    sb->st_blocks = DIV_ROUND_UP(in->size, FS_BLOCK_SIZE);

    sb->st_atim.tv_sec = in->mtime;
    sb->st_atim.tv_nsec = 0;
    sb->st_mtim.tv_sec = in->mtime;
    sb->st_mtim.tv_nsec = 0;
    sb->st_ctim.tv_sec = in->ctime;
    sb->st_ctim.tv_nsec = 0;
}




// ====== FUSE APIs ========

/* EXERCISE 1:
 * init - this is called once by the FUSE framework at startup.
 *
 * The function should:
 *   - read superblock
 *   - check if the magic number matches FS_MAGIC
 *   - initialize whatever in-memory data structure your fs5600 needs
 *     (you may come back later when requiring new data structures)
 *
 * notes:
 *   - ignore the 'conn' argument.
 *   - use "block_read" to read data (if you don't know how it works, read its
 *     implementation in misc.c)
 *   - if there is an error, exit(1)
 */
void* fs_init(struct fuse_conn_info *conn)
{
    /* RESET ALL GLOBAL STATE - critical for mounting multiple images */
    next_free_blk = 3;

    cache_valid = 0;
    cache_lba = -1;
    cache_dirty = 0;

    ic_valid = 0;
    ic_inum = -1;
    ic_dirty = 0;

    if (block_read(&superblock, 0, 1) < 0) {
        fprintf(stderr, "Failed to read superblock\n");
        exit(1);
    }

    if (superblock.magic != FS_MAGIC) {
        fprintf(stderr, "Invalid magic number: 0x%x (expected 0x%x)\n",
                superblock.magic, FS_MAGIC);
        exit(1);
    }

    if (block_read(block_bitmap, 1, 1) < 0) {
        fprintf(stderr, "Failed to read block bitmap\n");
        exit(1);
    }

    /* Initialize next_free_blk correctly */
    next_free_blk = 0;
    for (int i = 0; i < superblock.disk_size; i++) {
        if (!bit_test(block_bitmap, i)) {
            next_free_blk = i;
            break;
        }
    }

    return NULL;
}


/* EXERCISE 1:
 * statfs - get file system statistics
 * see 'man 2 statfs' for description of 'struct statvfs'.
 * Errors - none. Needs to work.
 */
int fs_statfs(const char *path, struct statvfs *st)
{
    /* needs to return the following fields (ignore others):
     *   [DONE] f_bsize = FS_BLOCK_SIZE
     *   [DONE] f_namemax = <whatever your max namelength is>
     *   [TODO] f_blocks = total image - (superblock + block map)
     *   [TODO] f_bfree = f_blocks - blocks used
     *   [TODO] f_bavail = f_bfree
     *
     * it's okay to calculate this dynamically on the rare occasions
     * when this function is called.
     */

    st->f_bsize = FS_BLOCK_SIZE;
    st->f_namemax = 27;  // why? see fs5600.h

    st->f_blocks = superblock.disk_size - 2;

    int used_blocks = 0;
    for (int i = 2; i < superblock.disk_size; i++) {
        if (bit_test(block_bitmap, i)) {
            used_blocks++;
        }
    }

    st->f_bfree = st->f_blocks - used_blocks;
    st->f_bavail = st->f_bfree;

    return 0;
}


/* EXERCISE 2:
 * getattr - get file or directory attributes. For a description of
 *  the fields in 'struct stat', read 'man 2 stat'.
 *
 * You should:
 *  1. parse the path given by "const char * path",
 *     find the inode of the specified file,
 *       [note: you should implement the helfer function "path2inum"
 *       and use it.]
 *  2. copy inode's information to "struct stat",
 *       [note: you should implement the helper function "inode2stat"
 *       and use it.]
 *  3. and return:
 *     ** success - return 0
 *     ** errors - path translation, ENOENT
 */


int fs_getattr(const char *path, struct stat *sb)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    struct fs_inode inode;
    struct fs_inode *inode_ptr;

    /* Check inode cache first */
    if (ic_valid && ic_inum == inum) {
        inode_ptr = &ic_inode;
    } else {
        if (block_read(&inode, inum, 1) < 0) {
            return -EIO;
        }
        inode_ptr = &inode;
    }

    inode2stat(sb, inode_ptr, inum);
    return 0;
}

/* EXERCISE 2:
 * readdir - get directory contents.
 *
 * call the 'filler' function for *each valid entry* in the
 * directory, as follows:
 *     filler(ptr, <name>, <statbuf>, 0)
 * where
 *   ** "ptr" is the second argument
 *   ** <name> is the name of the file/dir (the name in the direntry)
 *   ** <statbuf> is a pointer to the struct stat (of the file/dir)
 *
 * success - return 0
 * errors - path resolution, ENOTDIR, ENOENT
 *
 * hints:
 *   - this process is similar to the fs_getattr:
 *     -- you will walk file system to find the dir pointed by "path",
 *     -- then you need to call "filler" for each of
 *        the *valid* entry in this dir
 *   - you can ignore "struct fuse_file_info *fi" (also apply to later Exercises)
 */
int fs_readdir(const char *path, void *ptr, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    struct fs_inode dir_inode;
    if (block_read(&dir_inode, inum, 1) < 0) {
        return -EIO;
    }

    if (!S_ISDIR(dir_inode.mode)) {
        return -ENOTDIR;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, dir_inode.ptrs[0], 1) < 0) {
        return -EIO;
    }

    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (dir_entries[i].valid) {
            struct fs_inode entry_inode;
            if (block_read(&entry_inode, dir_entries[i].inode, 1) < 0) {
                return -EIO;
            }

            struct stat sb;
            inode2stat(&sb, &entry_inode, dir_entries[i].inode);
            filler(ptr, dir_entries[i].name, &sb, 0);
        }
    }

    return 0;
}


/* EXERCISE 3:
 * read - read data from an open file.
 * success: should return exactly the number of bytes requested, except:
 *   - if offset >= file len, return 0
 *   - if offset+len > file len, return #bytes from offset to end
 *   - on error, return <0
 * Errors - path resolution, ENOENT, EISDIR
 */
int fs_read(const char *path, char *buf, size_t len, off_t offset,
        struct fuse_file_info *fi)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    struct fs_inode inode;
    struct fs_inode *inode_ptr;

    /* Check inode cache first */
    if (ic_valid && ic_inum == inum) {
        inode_ptr = &ic_inode;
    } else {
        if (block_read(&inode, inum, 1) < 0) {
            return -EIO;
        }
        inode_ptr = &inode;
    }

    if (S_ISDIR(inode_ptr->mode)) {
        return -EISDIR;
    }

    if (offset >= inode_ptr->size) {
        return 0;
    }

    if (offset + len > inode_ptr->size) {
        len = inode_ptr->size - offset;
    }

    int bytes_read = 0;
    while (bytes_read < len) {
        int block_idx = (offset + bytes_read) / FS_BLOCK_SIZE;
        int block_offset = (offset + bytes_read) % FS_BLOCK_SIZE;
        int bytes_to_read = FS_BLOCK_SIZE - block_offset;

        if (bytes_to_read > len - bytes_read) {
            bytes_to_read = len - bytes_read;
        }

        int block_lba = get_block_num(inode_ptr, block_idx);
        if (block_lba < 0) {
            return block_lba;  /* Error reading indirect block */
        }

        if (block_lba == 0) {
            memset(buf + bytes_read, 0, bytes_to_read);
        } else {
            /* Check if block is in cache */
            if (cache_valid && cache_lba == block_lba) {
                memcpy(buf + bytes_read, cache_buf + block_offset, bytes_to_read);
            } else {
                char block_buf[FS_BLOCK_SIZE];
                if (block_read(block_buf, block_lba, 1) < 0) {
                    return -EIO;
                }
                memcpy(buf + bytes_read, block_buf + block_offset, bytes_to_read);
            }
        }

        bytes_read += bytes_to_read;
    }

    return bytes_read;
}



/* EXERCISE 3:
 * rename - rename a file or directory
 * success - return 0
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
int fs_rename(const char *src_path, const char *dst_path)
{
    int src_inum = path2inum(src_path);
    if (src_inum < 0) {
        return src_inum;
    }

    char *src_copy = strdup(src_path);
    char *dst_copy = strdup(dst_path);
    char *src_parent = strdup(src_path);
    char *dst_parent = strdup(dst_path);

    char *src_name = strrchr(src_copy, '/');
    char *dst_name = strrchr(dst_copy, '/');

    if (src_name == src_copy) {
        strcpy(src_parent, "/");
    } else {
        src_name[0] = '\0';
        strcpy(src_parent, src_copy);
    }
    src_name = strrchr(src_path, '/') + 1;

    if (dst_name == dst_copy) {
        strcpy(dst_parent, "/");
    } else {
        dst_name[0] = '\0';
        strcpy(dst_parent, dst_copy);
    }
    dst_name = strrchr(dst_path, '/') + 1;

    if (strcmp(src_parent, dst_parent) != 0) {
        free(src_copy);
        free(dst_copy);
        free(src_parent);
        free(dst_parent);
        return -EINVAL;
    }

    int dst_inum = path2inum(dst_path);
    if (dst_inum >= 0) {
        free(src_copy);
        free(dst_copy);
        free(src_parent);
        free(dst_parent);
        return -EEXIST;
    }

    int dir_inum = path2inum(src_parent);
    if (dir_inum < 0) {
        free(src_copy);
        free(dst_copy);
        free(src_parent);
        free(dst_parent);
        return dir_inum;
    }

    struct fs_inode dir_inode;
    if (block_read(&dir_inode, dir_inum, 1) < 0) {
        free(src_copy);
        free(dst_copy);
        free(src_parent);
        free(dst_parent);
        return -EIO;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, dir_inode.ptrs[0], 1) < 0) {
        free(src_copy);
        free(dst_copy);
        free(src_parent);
        free(dst_parent);
        return -EIO;
    }

    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (dir_entries[i].valid && strcmp(dir_entries[i].name, src_name) == 0) {
            strncpy(dir_entries[i].name, dst_name, 27);
            dir_entries[i].name[27] = '\0';

            if (block_write(dir_entries, dir_inode.ptrs[0], 1) < 0) {
                free(src_copy);
                free(dst_copy);
                free(src_parent);
                free(dst_parent);
                return -EIO;
            }

            free(src_copy);
            free(dst_copy);
            free(src_parent);
            free(dst_parent);
            return 0;
        }
    }

    free(src_copy);
    free(dst_copy);
    free(src_parent);
    free(dst_parent);
    return -ENOENT;
}

/* EXERCISE 3:
 * chmod - change file permissions
 *
 * success - return 0
 * Errors - path resolution, ENOENT.
 *
 * hints:
 *   - You can safely assume the "mode" is valid.
 *   - notice that "mode" only contains permissions
 *     (blindly assign it to inode mode doesn't work;
 *      why? check out Lab4 instructions about mode)
 *   - S_IFMT might be useful.
 */
int fs_chmod(const char *path, mode_t mode)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    int rv = load_inode_cache(inum);
    if (rv < 0) return rv;
    struct fs_inode *inode = &ic_inode;

    inode->mode = (inode->mode & S_IFMT) | (mode & ~S_IFMT);
    ic_dirty = 1;

    return 0;
}


/* EXERCISE 4:
 * create - create a new file with specified permissions
 *
 * success - return 0
 * errors - path resolution, EEXIST
 *          in particular, for create("/a/b/c") to succeed,
 *          "/a/b" must exist, and "/a/b/c" must not.
 *
 * If a file or directory of this name already exists, return -EEXIST.
 * If there are already 128 entries in the directory (i.e. it's filled an
 * entire block), you are free to return -ENOSPC instead of expanding it.
 * If the name is too long (longer than 27 letters), return -EINVAL.
 *
 * notes:
 *   - that 'mode' only has the permission bits. You have to OR it with S_IFREG
 *     before setting the inode 'mode' field.
 *   - Ignore the third parameter.
 *   - you will have to implement the helper funciont "alloc_blk" first
 *   - when creating a file, remember to initialize the inode ptrs.
 */
int fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    uint32_t cur_time = time(NULL);
    struct fuse_context *ctx = fuse_get_context();
    uint16_t uid = ctx->uid;
    uint16_t gid = ctx->gid;

    char *path_copy = strdup(path);
    char *filename = strrchr(path_copy, '/') + 1;

    if (strlen(filename) > 27) {
        free(path_copy);
        return -EINVAL;
    }

    if (path2inum(path) >= 0) {
        free(path_copy);
        return -EEXIST;
    }

    char *parent_path = strdup(path);
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash == parent_path) {
        strcpy(parent_path, "/");
    } else {
        *last_slash = '\0';
    }

    int parent_inum = path2inum(parent_path);
    free(parent_path);

    if (parent_inum < 0) {
        free(path_copy);
        return parent_inum;
    }

    struct fs_inode parent_inode;
    if (block_read(&parent_inode, parent_inum, 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        free(path_copy);
        return -ENOTDIR;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, parent_inode.ptrs[0], 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    int free_entry = -1;
    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (!dir_entries[i].valid) {
            free_entry = i;
            break;
        }
    }

    if (free_entry == -1) {
        free(path_copy);
        return -ENOSPC;
    }

    int new_inum = alloc_blk();
    if (new_inum < 0) {
        free(path_copy);
        return new_inum;
    }

    struct fs_inode new_inode;
    memset(&new_inode, 0, sizeof(new_inode));
    new_inode.uid = uid;
    new_inode.gid = gid;
    new_inode.mode = S_IFREG | mode;
    new_inode.ctime = cur_time;
    new_inode.mtime = cur_time;
    new_inode.size = 0;

    if (block_write(&new_inode, new_inum, 1) < 0) {
        free_blk(new_inum);
        free(path_copy);
        return -EIO;
    }

    dir_entries[free_entry].valid = 1;
    dir_entries[free_entry].inode = new_inum;
    strncpy(dir_entries[free_entry].name, filename, 27);
    dir_entries[free_entry].name[27] = '\0';

    if (block_write(dir_entries, parent_inode.ptrs[0], 1) < 0) {
        free_blk(new_inum);
        free(path_copy);
        return -EIO;
    }

    free(path_copy);
    return 0;
}



/* EXERCISE 4:
 * mkdir - create a directory with the given mode.
 *
 * Note that 'mode' only has the permission bits. You
 * have to OR it with S_IFDIR before setting the inode 'mode' field.
 *
 * success - return 0
 * Errors - path resolution, EEXIST
 * Conditions for EEXIST are the same as for create.
 *
 * hint:
 *   - there is a lot of similaries between fs_mkdir and fs_create.
 *     you may want to reuse many parts (note: reuse is not copy-paste!)
 */
int fs_mkdir(const char *path, mode_t mode)
{
    uint32_t cur_time = time(NULL);
    struct fuse_context *ctx = fuse_get_context();
    uint16_t uid = ctx->uid;
    uint16_t gid = ctx->gid;

    char *path_copy = strdup(path);
    char *dirname = strrchr(path_copy, '/') + 1;

    if (strlen(dirname) > 27) {
        free(path_copy);
        return -EINVAL;
    }

    if (path2inum(path) >= 0) {
        free(path_copy);
        return -EEXIST;
    }

    char *parent_path = strdup(path);
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash == parent_path) {
        strcpy(parent_path, "/");
    } else {
        *last_slash = '\0';
    }

    int parent_inum = path2inum(parent_path);
    free(parent_path);

    if (parent_inum < 0) {
        free(path_copy);
        return parent_inum;
    }

    struct fs_inode parent_inode;
    if (block_read(&parent_inode, parent_inum, 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    if (!S_ISDIR(parent_inode.mode)) {
        free(path_copy);
        return -ENOTDIR;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, parent_inode.ptrs[0], 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    int free_entry = -1;
    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (!dir_entries[i].valid) {
            free_entry = i;
            break;
        }
    }

    if (free_entry == -1) {
        free(path_copy);
        return -ENOSPC;
    }

    int new_inum = alloc_blk();
    if (new_inum < 0) {
        free(path_copy);
        return new_inum;
    }

    int data_blk = alloc_blk();
    if (data_blk < 0) {
        free_blk(new_inum);
        free(path_copy);
        return data_blk;
    }

    struct fs_dirent new_dir_entries[NUM_DIRENT_BLOCK];
    memset(new_dir_entries, 0, sizeof(new_dir_entries));
    if (block_write(new_dir_entries, data_blk, 1) < 0) {
        free_blk(new_inum);
        free_blk(data_blk);
        free(path_copy);
        return -EIO;
    }

    struct fs_inode new_inode;
    memset(&new_inode, 0, sizeof(new_inode));
    new_inode.uid = uid;
    new_inode.gid = gid;
    new_inode.mode = S_IFDIR | mode;
    new_inode.ctime = cur_time;
    new_inode.mtime = cur_time;
    new_inode.size = FS_BLOCK_SIZE;
    new_inode.ptrs[0] = data_blk;

    if (block_write(&new_inode, new_inum, 1) < 0) {
        free_blk(new_inum);
        free_blk(data_blk);
        free(path_copy);
        return -EIO;
    }

    dir_entries[free_entry].valid = 1;
    dir_entries[free_entry].inode = new_inum;
    strncpy(dir_entries[free_entry].name, dirname, 27);
    dir_entries[free_entry].name[27] = '\0';

    if (block_write(dir_entries, parent_inode.ptrs[0], 1) < 0) {
        free_blk(new_inum);
        free_blk(data_blk);
        free(path_copy);
        return -EIO;
    }

    free(path_copy);
    return 0;
}


/* EXERCISE 5:
 * unlink - delete a file
 *  success - return 0
 *  errors - path resolution, ENOENT, EISDIR
 *
 * hint:
 *   - you will have to implement the helper funciont "free_blk" first
 *   - remember to delete all data blocks as well
 *   - remember to update "mtime"
 */
int fs_unlink(const char *path)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    struct fs_inode inode;
    struct fs_inode *inode_ptr;

    /* Check inode cache first */
    if (ic_valid && ic_inum == inum) {
        inode_ptr = &ic_inode;
    } else {
        if (block_read(&inode, inum, 1) < 0) {
            return -EIO;
        }
        inode_ptr = &inode;
    }

    if (S_ISDIR(inode_ptr->mode)) {
        return -EISDIR;
    }

    char *path_copy = strdup(path);
    char *filename = strrchr(path_copy, '/') + 1;
    char *parent_path = strdup(path);
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash == parent_path) {
        strcpy(parent_path, "/");
    } else {
        *last_slash = '\0';
    }

    int parent_inum = path2inum(parent_path);
    free(parent_path);

    if (parent_inum < 0) {
        free(path_copy);
        return parent_inum;
    }

    struct fs_inode parent_inode;
    if (block_read(&parent_inode, parent_inum, 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, parent_inode.ptrs[0], 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (dir_entries[i].valid && strcmp(dir_entries[i].name, filename) == 0) {
            dir_entries[i].valid = 0;

            parent_inode.mtime = time(NULL);
            if (block_write(&parent_inode, parent_inum, 1) < 0) {
                free(path_copy);
                return -EIO;
            }

            if (block_write(dir_entries, parent_inode.ptrs[0], 1) < 0) {
                free(path_copy);
                return -EIO;
            }

            /* Free all data blocks (direct and indirect) */
            free_all_blocks(inode_ptr);

            free_blk(inum);
            free(path_copy);
            return 0;
        }
    }

    free(path_copy);
    return -ENOENT;
}

/* EXERCISE 5:
 * rmdir - remove a directory
 *  success - return 0
 *  Errors - path resolution, ENOENT, ENOTDIR, ENOTEMPTY
 *
 * hint:
 *   - fs_rmdir and fs_unlink have a lot in common; think of reuse the code
 */
int fs_rmdir(const char *path)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    struct fs_inode inode;
    if (block_read(&inode, inum, 1) < 0) {
        return -EIO;
    }

    if (!S_ISDIR(inode.mode)) {
        return -ENOTDIR;
    }

    struct fs_dirent dir_entries[NUM_DIRENT_BLOCK];
    if (block_read(dir_entries, inode.ptrs[0], 1) < 0) {
        return -EIO;
    }

    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (dir_entries[i].valid) {
            return -ENOTEMPTY;
        }
    }

    char *path_copy = strdup(path);
    char *dirname = strrchr(path_copy, '/') + 1;
    char *parent_path = strdup(path);
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash == parent_path) {
        strcpy(parent_path, "/");
    } else {
        *last_slash = '\0';
    }

    int parent_inum = path2inum(parent_path);
    free(parent_path);

    if (parent_inum < 0) {
        free(path_copy);
        return parent_inum;
    }

    struct fs_inode parent_inode;
    if (block_read(&parent_inode, parent_inum, 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    struct fs_dirent parent_entries[NUM_DIRENT_BLOCK];
    if (block_read(parent_entries, parent_inode.ptrs[0], 1) < 0) {
        free(path_copy);
        return -EIO;
    }

    for (int i = 0; i < NUM_DIRENT_BLOCK; i++) {
        if (parent_entries[i].valid && strcmp(parent_entries[i].name, dirname) == 0) {
            parent_entries[i].valid = 0;

            parent_inode.mtime = time(NULL);
            if (block_write(&parent_inode, parent_inum, 1) < 0) {
                free(path_copy);
                return -EIO;
            }

            if (block_write(parent_entries, parent_inode.ptrs[0], 1) < 0) {
                free(path_copy);
                return -EIO;
            }

            if (inode.ptrs[0] != 0) {
                free_blk(inode.ptrs[0]);
            }

            free_blk(inum);
            free(path_copy);
            return 0;
        }
    }

    free(path_copy);
    return -ENOENT;
}

/* EXERCISE 6:
 * write - write data to a file
 * success - return number of bytes written. (this will be the same as
 *           the number requested, or else it's an error)
 *
 * Errors - path resolution, ENOENT, EISDIR, ENOSPC
 *  return EINVAL if 'offset' is greater than current file length.
 *  (POSIX semantics support the creation of files with "holes" in them,
 *   but we don't)
 *  return ENOSPC when the data exceed the maximum size of a file.
 */
int fs_write(const char *path, const char *buf, size_t len,
         off_t offset, struct fuse_file_info *fi)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    int rv = load_inode_cache(inum);
    if (rv < 0) return rv;
    struct fs_inode *inode = &ic_inode;

    if (S_ISDIR(inode->mode)) {
        return -EISDIR;
    }

    if (offset > inode->size) {
        return -EINVAL;
    }

    /* Calculate max file size with indirect pointers */
    int max_file_size = (NUM_DIRECT + NUM_INDIRECT * PTRS_PER_BLOCK) * FS_BLOCK_SIZE;
    if (offset + len > max_file_size) {
        return -ENOSPC;
    }

    size_t bytes_written = 0;
    while (bytes_written < len) {
        int block_idx = (offset + bytes_written) / FS_BLOCK_SIZE;
        int block_offset = (offset + bytes_written) % FS_BLOCK_SIZE;
        size_t bytes_to_write = FS_BLOCK_SIZE - block_offset;

        if (bytes_to_write > len - bytes_written) {
            bytes_to_write = len - bytes_written;
        }

        int lba = get_block_num(inode, block_idx);
        if (lba < 0) {
            return lba;  /* Error */
        }

        if (lba == 0) {
            int nb = alloc_blk();
            if (nb < 0) return nb;

            int rv = set_block_num(inode, block_idx, nb);
            if (rv < 0) {
                free_blk(nb);
                return rv;
            }
            ic_dirty = 1;

            int frv = flush_block_cache();
            if (frv < 0) return frv;

            cache_valid = 1;
            cache_lba = nb;
            memset(cache_buf, 0, FS_BLOCK_SIZE);
            cache_dirty = 1;
        } else {
            int brv = load_block_cache(lba);
            if (brv < 0) return brv;
        }

        memcpy(cache_buf + block_offset, buf + bytes_written, bytes_to_write);
        cache_dirty = 1;

        bytes_written += bytes_to_write;
    }

    if (offset + len > inode->size) {
        inode->size = offset + len;
    }

    inode->mtime = time(NULL);
    ic_dirty = 1;

    return len;
}

/* EXERCISE 6:
 * truncate - truncate file to exactly 'len' bytes
 * note that CS5600 fs only allows len=0, meaning discard all data in this file.
 *
 * success - return 0
 * Errors - path resolution, ENOENT, EISDIR, EINVAL
 *    return EINVAL if len > 0.
 */
int fs_truncate(const char *path, off_t len)
{
    /* you can cheat by only implementing this for the case of len==0,
     * and an error otherwise.
     */
    if (len != 0) {
        return -EINVAL;        /* invalid argument */
    }

    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    int rv = load_inode_cache(inum);
    if (rv < 0) return rv;
    struct fs_inode *inode = &ic_inode;

    if (S_ISDIR(inode->mode)) {
        return -EISDIR;
    }

    /* Free all data blocks (direct and indirect) */
    free_all_blocks(inode);

    inode->size = 0;
    inode->mtime = time(NULL);
    ic_dirty = 1;

    return 0;
}

/* EXERCISE 6:
 * Change file's last modification time.
 *
 * notes:
 *  - read "man 2 utime" to know more.
 *  - when "ut" is NULL, update the time to now (i.e., time(NULL))
 *  - you only need to use the "modtime" in "struct utimbuf" (ignore "actime")
 *    and you only have to update "mtime" in inode.
 *
 * success - return 0
 * Errors - path resolution, ENOENT
 */
int fs_utime(const char *path, struct utimbuf *ut)
{
    int inum = path2inum(path);
    if (inum < 0) {
        return inum;
    }

    int rv = load_inode_cache(inum);
    if (rv < 0) return rv;
    struct fs_inode *inode = &ic_inode;

    if (ut == NULL) {
        inode->mtime = time(NULL);
    } else {
        inode->mtime = ut->modtime;
    }

    ic_dirty = 1;

    return 0;
}

/* EXERCISE 7:
 * fsync - synchronize file data to disk
 * Forces any cached writes to be written to disk.
 *
 * success - return 0
 * errors - EIO on write failure
 *
 * Critical: Must flush in correct order for crash consistency:
 *   1. Data blocks first (so inode never points to garbage)
 *   2. Inode metadata (size, pointers)
 *   3. Allocation bitmap (so blocks are marked as used)
 */
int fs_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    /* 1. Flush dirty data block */
    if (flush_block_cache() < 0)
        return -EIO;

    /* 2. Flush inode metadata */
    if (flush_inode_cache() < 0)
        return -EIO;

    /* 3. Flush allocation bitmap */
    if (block_write(block_bitmap, 1, 1) < 0)
        return -EIO;

    return 0;
}

/* EXERCISE 7:
 * destroy - called when filesystem is unmounted
 * Ensures all cached data is written to disk before shutdown.
 *
 * Must flush everything in the correct order:
 *   1. Data blocks
 *   2. Inode metadata
 *   3. Allocation bitmap
 */
void fs_destroy(void *private_data)
{
    /* Flush all caches before unmounting (same order as fsync) */
    flush_block_cache();
    flush_inode_cache();
    block_write(block_bitmap, 1, 1);
}


/* operations vector. Please don't rename it, or else you'll break things
 */
struct fuse_operations fs_ops = {
    .init = fs_init,            /* read-mostly operations */
    .statfs = fs_statfs,
    .getattr = fs_getattr,
    .readdir = fs_readdir,
    .read = fs_read,
    .rename = fs_rename,
    .chmod = fs_chmod,

    .create = fs_create,        /* write operations */
    .mkdir = fs_mkdir,
    .unlink = fs_unlink,
    .rmdir = fs_rmdir,
    .write = fs_write,
    .truncate = fs_truncate,
    .utime = fs_utime,

    .fsync = fs_fsync,          /* cache management */
    .destroy = fs_destroy,
};

