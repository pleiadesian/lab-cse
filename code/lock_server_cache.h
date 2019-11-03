#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <pthread.h>


class lock_server_cache {
 private:
  int nacquire;

  pthread_mutex_t lock;
  enum lock_status { NONE, LOCKED, REVOKING, RETRYING };

  struct lock_entry {
    enum lock_status stat;
    std::list<std::string> wait_queue;
    std::string owner;
    std::string retry_client;
  };

  std::map<lock_protocol::lockid_t, struct lock_entry *> lock_list;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
