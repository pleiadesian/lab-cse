// this is the extent server

#ifndef extent_server_cache_h
#define extent_server_cache_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "inode_manager.h"
#include <pthread.h>

class extent_server_cache {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;

  pthread_mutex_t lock;
  enum inode_status { SHARED, EXCLUSIVE };

  struct inode_entry {
    enum inode_status stat;
    std::list<std::string> reader_queue;
    std::string writer;
  };
  std::map<extent_protocol::extentid_t, struct inode_entry *> inode_list;

 public:
  extent_server_cache();

  int create(uint32_t type, std::string cid, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string cid, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string cid, std::string &);
  int getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, std::string cid, int &);
};

#endif 
