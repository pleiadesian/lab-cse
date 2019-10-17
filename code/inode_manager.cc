#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */

  int bitmap_pos = 2;
  int blockid = -1;
  char buf[BLOCK_SIZE];

  // scan bitmap and set the first 0
  while (blockid == -1 && bitmap_pos < BLOCK_NUM/BPB + 2) {
    read_block(bitmap_pos, buf);
    for (int i = 0 ; i < BLOCK_SIZE ; i++) {
      for (int j = 0 ; j < 8 ; j++) {
        if (!(buf[i] & (1<<j))) {
          blockid = 8 * i + j + (bitmap_pos - 2) * BPB;
          buf[i] = buf[i] | (1<<j);
          write_block(bitmap_pos, buf);
          return blockid;
        }
      }
    }
    bitmap_pos++;
  }

  exit(-1);
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (id < 0 || id > BLOCK_NUM) {
    return;
  }

  // get block position in bitmap
  char buf[BLOCK_SIZE];
  int bitmap_pos = BBLOCK(id);
  int bit_pos = id % BPB;
  int byte_pos = bit_pos / 8;

  // reset the bit
  read_block(bitmap_pos, buf);
  buf[byte_pos] = buf[byte_pos] & ~(1 << (bit_pos % 8));
  write_block(bitmap_pos, buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  // super block
  char super_buf[BLOCK_SIZE];
  bzero(super_buf, BLOCK_SIZE);
  memcpy(super_buf, &sb, sizeof(superblock_t));
  write_block(1, super_buf);

  // mark meta data block as allocated
  char buf[BLOCK_SIZE];
  int tot_alloced = 2 + BLOCK_NUM / BPB + (INODE_NUM + IPB - 1) / IPB;
  int bitmap_pos = BBLOCK(tot_alloced);
  int byte_pos = tot_alloced % BPB / 8;
  int bit_pos = tot_alloced % BPB % 8;

  memset(buf, 0xff, BLOCK_SIZE);
  for(int i = 2 ; i < bitmap_pos ; i++) {
    write_block(i, buf);
  }

  read_block(bitmap_pos, buf);
  memset(buf, 0xff, byte_pos);
  buf[byte_pos] = buf[byte_pos] | ((1<<bit_pos) - 1);
  write_block(bitmap_pos, buf);
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  int inode_pos = 0;
  int iblock_pos;
  char buf[BLOCK_SIZE];
  inode_t *inode;

  // get first free inode
  do{
    inode_pos++;
    iblock_pos = IBLOCK(inode_pos, bm->sb.nblocks);
    bm->read_block(iblock_pos, buf);
    inode = (inode_t *)buf + (inode_pos - 1) % IPB;
  }while (inode->type && inode_pos < INODE_NUM);

  if (inode_pos == INODE_NUM) {
    exit(-1);
  }

  // set inode
  bzero(inode, sizeof(inode_t));
  inode->type = type;
  inode->size = 0;
  inode->atime = (unsigned)time(NULL);
  inode->mtime = (unsigned)time(NULL);
  inode->ctime = (unsigned)time(NULL);
  bm->write_block(iblock_pos, buf);

  return inode_pos;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  if (inum < 1 || inum > INODE_NUM) {
    return;
  }

  char buf[BLOCK_SIZE];
  int inode_pos = IBLOCK(inum, BLOCK_NUM);

  bm->read_block(inode_pos, buf);
  inode_t *inode = (inode_t *)buf + (inum - 1) % IPB;
  inode->type = 0;
  bm->write_block(inode_pos, buf);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */

  blockid_t blockids[MAXFILE];
  int whole_block_num, data_block_num, last_block_size;

  inode_t *inode = get_inode(inum);

  if (inode == NULL) {
    printf("\tim: cannot find inode\n");
    return;
  }

  *size = inode->size;
  *buf_out = (char *)malloc(inode->size);

  if (*buf_out == NULL) {
    exit(-1);
  }

  // get meta data from inode
  whole_block_num = inode->size / BLOCK_SIZE;
  data_block_num = (inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  last_block_size = inode->size % BLOCK_SIZE;

  // read data block ids from only direct blocks?
  if (data_block_num <= NDIRECT) {
    memcpy(blockids, inode->blocks, data_block_num * sizeof(blockid_t));
  } else {
    char buf[BLOCK_SIZE];
    memcpy(blockids, inode->blocks, NDIRECT * sizeof(blockid_t));
    bm->read_block(inode->blocks[NDIRECT], buf);
    memcpy(blockids + NDIRECT, buf, (data_block_num - NDIRECT) * sizeof(blockid_t));
  }


  // read whole data blocks
  for (int i = 0 ; i < whole_block_num ; i++) {
    bm->read_block(blockids[i], *buf_out + i * BLOCK_SIZE);
  }

  // read last block
  if (last_block_size > 0) {
    char buf[BLOCK_SIZE];
    bm->read_block(blockids[whole_block_num], buf);
    memcpy(*buf_out + whole_block_num * BLOCK_SIZE, buf, last_block_size);
  }

  // set atime
  inode->atime = (unsigned int)time(NULL);
  put_inode(inum, inode);

  free(inode);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  if (buf == NULL) {
    return;
  }

  blockid_t blockids[MAXFILE];
  int whole_block_num, old_block_num, new_block_num, last_block_size;

  inode_t *inode = get_inode(inum);

  if (inode == NULL) {
    return;
  }

  // get meta data from inode
  old_block_num = (inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  whole_block_num = size / BLOCK_SIZE;
  last_block_size = size % BLOCK_SIZE;

  // get inode
  if (old_block_num <= NDIRECT) {
      memcpy(blockids, inode->blocks, old_block_num * sizeof(blockid_t));
  } else {
      char temp_buf[BLOCK_SIZE];
      memcpy(blockids, inode->blocks, NDIRECT * sizeof(blockid_t));
      bm->read_block(inode->blocks[NDIRECT], temp_buf);
      memcpy(blockids + NDIRECT, temp_buf, (old_block_num - NDIRECT) * sizeof(blockid_t));
  }

  // adjust blocks
  if (old_block_num < new_block_num) {
    // need to alloc new block
    int diff = new_block_num - old_block_num;
    for (int i = 0 ; i < diff ; i++) {
      blockids[old_block_num + i] = bm->alloc_block();
    }

    // need to alloc indirect block
    if (old_block_num <= NDIRECT && new_block_num > NDIRECT) {
      inode->blocks[NDIRECT] = bm->alloc_block();
    }

  } else if (old_block_num > new_block_num) {
    // need to free old block
    int diff = old_block_num - new_block_num;
    for (int i = 0; i < diff ; i++) {
      bm->free_block(blockids[old_block_num-i-1]);
    }

    // need to free indirect block
    if (old_block_num > NDIRECT && new_block_num <= NDIRECT) {
      bm->free_block(inode->blocks[NDIRECT]);
    }
  }

  // write data to whole blocks
  for (int i = 0 ; i < whole_block_num ; i++) {
    bm->write_block(blockids[i], buf + i * BLOCK_SIZE);
  }

  // write data to last block
  if (last_block_size > 0) {
    char temp_buf[BLOCK_SIZE];
    memcpy(temp_buf, buf + whole_block_num * BLOCK_SIZE, last_block_size);
    bm->write_block(blockids[whole_block_num], temp_buf);
  }

  // set inode
  if (new_block_num <= NDIRECT) {
    memcpy(inode->blocks, blockids, new_block_num * sizeof(blockid_t));
  } else {
    char temp_buf[BLOCK_SIZE];
    memcpy(inode->blocks, blockids, NDIRECT * sizeof(blockid_t));
    memcpy(temp_buf, blockids + NDIRECT, (new_block_num - NDIRECT) * sizeof(blockid_t));
    bm->write_block(inode->blocks[NDIRECT], temp_buf);
  }
  inode->size = size;
  inode->mtime = (unsigned int)time(NULL);
  inode->ctime = (unsigned int)time(NULL);
  put_inode(inum, inode);

  free(inode);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *inode = get_inode(inum);

  if (inode == NULL) {
    return;
  }

  a.type = inode->type;
  a.atime = inode->atime;
  a.mtime = inode->mtime;
  a.ctime = inode->ctime;
  a.size = inode->size;

  free(inode);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  inode_t *inode = get_inode(inum);

  if (inode == NULL) {
    return;
  }

  blockid_t blockids[MAXFILE];

  // find data block ids
  char buf[MAXFILE];
  int data_block_num = (inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (data_block_num < NDIRECT) {
    memcpy(blockids, inode->blocks, data_block_num * sizeof(blockid_t));
  } else {
    memcpy(blockids, inode->blocks, NDIRECT * sizeof(blockid_t));
    bm->read_block(inode->blocks[NDIRECT], buf);
    memcpy(blockids + NDIRECT, buf, (data_block_num - NDIRECT));
  }

  // free blocks
  for (int i = 0 ; i < data_block_num ; i++) {
    bm->free_block(blockids[i]);
  }

  if (data_block_num > NDIRECT) {
    bm->free_block(inode->blocks[NDIRECT]);
  }

  free_inode(inum);
  free(inode);
}
