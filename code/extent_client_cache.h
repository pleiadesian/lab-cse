// extent client interface.

#ifndef extent_client_cache_h
#define extent_client_cache_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"
#include <pthread.h>
#include <map>
#include "extent_client.h"

class extent_client_cache : public extent_client {
 private:

  int rextent_port;
  std::string hostname;
  std::string id;

  pthread_mutex_t lock;

  enum inode_status { SHARED, EXCLUSIVE };

  struct inode_cache {
    enum inode_status stat;
    int valid;
    int retry;
    int attr_cached;
    unsigned int atime;
    unsigned int mtime;
    unsigned int ctime;
    std::string buf;
    extent_protocol::attr attr;
    pthread_cond_t retry_reader;

    inode_cache() {
      stat = SHARED;
      valid = 0;
      retry = 0;
      attr_cached = 0;
      atime = 0;
      mtime = 0;
      ctime = 0;
    }
  };

  std::map<extent_protocol::extentid_t, struct inode_cache *> inode_list;

 public:
  static int last_port;

  extent_client_cache(std::string dst);
  virtual ~extent_client_cache() {};

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  rextent_protocol::status invalid_handler(extent_protocol::extentid_t,
                                          int &);
  rextent_protocol::status revoke_handler(extent_protocol::extentid_t,
                                          int &);
  rextent_protocol::status retry_handler(extent_protocol::extentid_t,
                                          std::string, extent_protocol::attr, int &);
};

#endif 

