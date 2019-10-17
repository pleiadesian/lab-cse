// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
    pthread_mutex_init(&lock, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

	// acquire mutex lock
  pthread_mutex_lock(&lock);

  if (lock_stat.find(lid) == lock_stat.end()) {
    // init lock
    pthread_cond_t *cond_init = new pthread_cond_t;
    pthread_cond_init(cond_init, NULL);
    lock_cond[lid] = cond_init;
  } else {
    // waiting for lock to release
    while (lock_stat[lid] == 1)
      pthread_cond_wait(lock_cond[lid], &lock);
  }

  // acquire lock
  lock_stat[lid] = 1;

  // release mutex lock
  pthread_mutex_unlock(&lock);

  r = ret;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  // acquire mutex lock
  pthread_mutex_lock(&lock);

  // release lock
  lock_stat[lid] = 0;

  // signal lock to release
  pthread_cond_signal(lock_cond[lid]);

  // release mutex lock
  pthread_mutex_unlock(&lock);

  r = ret;
  return ret;
}
