#include "inode_manager.h"

#include <cmath>

time_t gettime_core() {
    struct timespec time1 = {0, 0};
    clock_gettime(CLOCK_REALTIME, &time1);
    return time1.tv_sec;
}

uint32_t compare(const char *str1, const char *str2, uint32_t size, uint32_t start = 0) {
    uint32_t i;
    for (i = 0; i < size; ++i) {
        if (i >= start && *str1 != *str2) {
            break;
        }
        str1++;
        str2++;
    }
    return i;
}

// disk layer -----------------------------------------

disk::disk() {
    memset(blocks, 0, BLOCK_NUM * BLOCK_SIZE);
}

void disk::read_block(blockid_t id, char *buf) {
    memcpy(buf, (char *) blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
    memcpy((char *) blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */

    uint32_t bitBlocks = (sb.nblocks) / BPB;
    char buf[BLOCK_SIZE], full[BLOCK_SIZE];
    uint32_t i, j, k;
    blockid_t id = 0;

    memset(full, 0xFF, BLOCK_SIZE);

    // Find Vacancy Block
    for (i = 0; i < bitBlocks; ++i) {
        d->read_block(i + 2, buf);
        // Find Vacancy Byte
        j = compare(buf, full, BLOCK_SIZE);
        if (j != BLOCK_SIZE) {
            break;
        }
    }

    if (i == bitBlocks) {
        printf("\tim: blocks run out\n");
        return 0;
    }

    char ch0 = buf[j];
    // Find Vacancy Bit
    for (k = 0; k <= 7; ++k) {
        if ((~ch0) & 1 << (7 - k)) {
            break;
        }
    }

    buf[j] |= 1 << (7 - k);
    d->write_block(i + 2, buf);

    uint32_t startBlock = IBLOCK(INODE_NUM, sb.nblocks) + 1;
    id = i * BPB + j * 8 + k + startBlock;

    memset(buf, 0, BLOCK_SIZE);
    write_block(id, buf); // Clear Block

    return id;
}

void block_manager::free_block(uint32_t id) {
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */

    uint32_t i, j, k;
    uint32_t startBlock = IBLOCK(INODE_NUM, sb.nblocks) + 1;
    id -= startBlock;

    i = id / BPB;           // Block
    j = (id - i * BPB) / 8; // Byte
    k = id % 8;             // Bit

    char buf[BLOCK_SIZE];
    d->read_block(i + 2, buf);
    buf[j] &= ~(1 << (7 - k));
    d->write_block(i + 2, buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf) {
    d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf) {
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
    /*
     * your code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */

    struct inode *ino_cache;
    char buf[BLOCK_SIZE];
    bool find = false;
    uint32_t inum = 1;

    for (; inum < INODE_NUM; ++inum) {
        bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
        ino_cache = (struct inode *) buf + inum % IPB;
        if (ino_cache->type == 0) {
            find = true;
            break;
        }
    }

    if (!find) {
        printf("\tim: cannot create more inode\n");
        return 0;
    }

    memset(ino_cache, 0, sizeof(inode));
    ino_cache->type = type;

    time_t time = gettime_core();
    ino_cache->atime = time;
    ino_cache->ctime = time;
    ino_cache->mtime = time;

    // Write back
    put_inode(inum, ino_cache);

    return inum;
}

void inode_manager::free_inode(uint32_t inum) {
    /*
     * your code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */

    struct inode *ino_cache;
    char buf[BLOCK_SIZE];

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_cache = (struct inode *) buf + inum % IPB;
    if (ino_cache->type == 0) {
        printf("\tim: inode not exist\n");
        return;
    }

    ino_cache->type = 0;

    // Write back
    put_inode(inum, ino_cache);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    //    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode *) buf + inum % IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode *) malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    //    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */

    char buf0[BLOCK_SIZE];
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf0);
    inode *target = (struct inode *) buf0 + inum % IPB;
    if (target->type == 0) {
        return;
    }

    // Modify Atime
    target->atime = gettime_core();

    int total = target->size, len = 0;
    char *ptr = new char[total];
    char buf1[BLOCK_SIZE];

    *buf_out = ptr;

    // Read Direct Blocks
    for (uint32_t i = 0; i < NDIRECT && len < total; ++i) {
        blockid_t id = target->blocks[i];

        if (total - len < BLOCK_SIZE) {
            memset(buf1, 0, BLOCK_SIZE);
            bm->read_block(id, buf1);
            memcpy(ptr, buf1, total - len);

            ptr += total - len;
            len += total - len;
        } else {
            bm->read_block(id, ptr);

            ptr += BLOCK_SIZE;
            len += BLOCK_SIZE;
        }
    }

    // Read Indirect Blocks
    blockid_t id2 = target->blocks[NDIRECT];
    if (id2 != 0) {
        uint32_t block2[NINDIRECT];
        bm->read_block(id2, (char *) block2);

        for (uint32_t i = 0; i < NINDIRECT; ++i) {
            blockid_t id = block2[i];
            if (id == 0) {
                break;
            }

            if (total - len < BLOCK_SIZE) {
                memset(buf1, 0, BLOCK_SIZE);
                bm->read_block(id, buf1);
                memcpy(ptr, buf1, total - len);

                ptr += total - len;
                len += total - len;
            } else {
                bm->read_block(id, ptr);

                ptr += BLOCK_SIZE;
                len += BLOCK_SIZE;
            }
        }
    }

    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf0);
    *size = len;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */

    char buf0[BLOCK_SIZE];
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf0);
    inode *target = (struct inode *) buf0 + inum % IPB;
    if (target->type == 0) {
        return;
    }

    // Modify AMtime
    time_t time = gettime_core();
    target->atime = time;
    target->mtime = time;
    target->ctime = time;
    target->size = size;

    int len = 0;
    const char *ptr = buf;
    char buf1[BLOCK_SIZE];

    // Write Direct Blocks
    for (uint32_t i = 0; i < NDIRECT && len < size; ++i) {
        blockid_t id = target->blocks[i];
        if (id == 0) {
            id = bm->alloc_block();
            if (id == 0) {
                return;
            }
            target->blocks[i] = id;
        }

        if (size - len < BLOCK_SIZE) {
            memset(buf1, 0, BLOCK_SIZE);
            memcpy(buf1, ptr, size - len);
            bm->write_block(id, buf1);

            ptr += size - len;
            len += size - len;
        } else {
            bm->write_block(id, ptr);
            ptr += BLOCK_SIZE;
            len += BLOCK_SIZE;
        }
    }

    blockid_t id2 = target->blocks[NDIRECT];
    if (len == size) {
        // Free Direct Blocks
        for (uint32_t i = size; i < NDIRECT; ++i) {
            blockid_t id = target->blocks[i];
            if (id == 0) {
                break;
            }
            bm->free_block(id);
            target->blocks[i] = 0;
        }
        // Free Indirect Blocks
        if (id2 != 0) {
            uint32_t block2[NINDIRECT];
            bm->read_block(id2, (char *) block2);

            for (uint32_t i = 0; i < NINDIRECT; ++i) {
                blockid_t id = block2[i];
                if (id == 0) {
                    break;
                }
                bm->free_block(id);
            }

            bm->free_block(id2);
            target->blocks[NDIRECT] = 0;
        }
    } else {
        // Need Indirect
        uint32_t block2[NINDIRECT];
        if (id2 == 0) {
            // Create Indirect
            id2 = bm->alloc_block();
            if (id2 == 0) {
                return;
            }
            target->blocks[NDIRECT] = id2;

            memset(block2, 0, BLOCK_SIZE);
        } else {
            bm->read_block(id2, (char *) block2);
        }

        uint32_t i = 0;
        while (len < size && i < NINDIRECT) {
            blockid_t id = block2[i];
            if (id == 0) {
                id = bm->alloc_block();
                if (id == 0) {
                    return;
                }
                block2[i] = id;
            }

            if (size - len < BLOCK_SIZE) {
                memset(buf1, 0, BLOCK_SIZE);
                memcpy(buf1, ptr, size - len);
                bm->write_block(id, buf1);

                ptr += size - len;
                len += size - len;
            } else {
                bm->write_block(id, ptr);
                ptr += BLOCK_SIZE;
                len += BLOCK_SIZE;
            }
            ++i;
        }
        while (i < NINDIRECT && block2[i] != 0) {
            bm->free_block(block2[i]);
            block2[i] = 0;
            ++i;
        }
        bm->write_block(id2, (const char *) block2);
    }

    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf0);
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */

    inode *target = get_inode(inum);
    if (!target) {
        return;
    }

    a.type = target->type;
    a.atime = target->atime;
    a.ctime = target->ctime;
    a.mtime = target->mtime;
    a.size = target->size;

    free(target);

    return;
}

void inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */

    char buf0[BLOCK_SIZE];
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf0);
    inode *target = (struct inode *) buf0 + inum % IPB;
    if (target->type == 0) {
        return;
    }

    // Write Direct Blocks
    for (uint32_t i = 0; i < NDIRECT; ++i) {
        blockid_t id = target->blocks[i];
        if (id == 0) {
            break;
        }
        bm->free_block(id);
    }

    blockid_t id2 = target->blocks[NDIRECT];
    // Free Indirect Blocks
    if (id2 != 0) {
        uint32_t block2[NINDIRECT];
        bm->read_block(id2, (char *) block2);
        for (uint32_t i = 0; i < NINDIRECT; ++i) {
            blockid_t id = block2[i];
            if (id == 0) {
                break;
            }
            bm->free_block(id);
        }
        bm->free_block(id2);
    }
    memset(target->blocks, 0, (NDIRECT + 1) * sizeof(blockid_t));
    target->type = 0;

    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf0);
}
