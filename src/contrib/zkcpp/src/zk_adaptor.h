/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ZK_ADAPTOR_H_
#define ZK_ADAPTOR_H_
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/ptr_container/ptr_list.hpp>
#include "zookeeper.h"
#include "zookeeper.jute.h"
#include "recordio.h"
#include "zk_hashtable.h"

using namespace org::apache::zookeeper;

/* predefined xid's values recognized as special by the server */
#define WATCHER_EVENT_XID -1 
#define PING_XID -2
#define AUTH_XID -4
#define SET_WATCHES_XID -8

/* zookeeper state constants */
#define EXPIRED_SESSION_STATE_DEF -112
#define AUTH_FAILED_STATE_DEF -113
#define CONNECTING_STATE_DEF 1
#define ASSOCIATING_STATE_DEF 2
#define CONNECTED_STATE_DEF 3
#define NOTCONNECTED_STATE_DEF 999

/* zookeeper event type constants */
#define CREATED_EVENT_DEF 1
#define DELETED_EVENT_DEF 2
#define CHANGED_EVENT_DEF 3
#define CHILD_EVENT_DEF 4
#define SESSION_EVENT_DEF -1
#define NOTWATCHING_EVENT_DEF -2

#ifdef __cplusplus
extern "C" {
#endif

class completion_list_t;

/**
 * This structure represents a packet being read or written.
 */
class buffer_t {
  public:
    buffer_t() {
      len = 0;
      curr_offset = 0;
      buffer = NULL;
    }
    buffer_t(char* buff, int length) {
      len = length;
      curr_offset = 0;
      buffer = buff;
    }
    ~buffer_t() {
      if (buffer) {
        free(buffer);
      }
    }

    char *buffer;
    int len; /* This represents the length of sizeof(header) + length of buffer */
    int curr_offset; /* This is the offset into the header followed by offset into the buffer */
};

class buffer_list_t {
  public:
    boost::ptr_list<buffer_t> bufferList_;
    boost::recursive_mutex mutex_;
};

class completion_head_t {
  public:
    completion_list_t* volatile head;
    completion_list_t* last;
    boost::shared_ptr<boost::condition_variable> cond;
    boost::shared_ptr<boost::mutex> lock;
};

void lock_completion_list(completion_head_t *l);
void unlock_completion_list(completion_head_t *l);

class auth_info {
  public:
    auth_info() : scheme("") {}
    ~auth_info() {
      deallocate_Buffer(&auth);
    }
    int state; /* 0=>inactive, >0 => active */
    std::string scheme;
    struct buffer auth;
    void_completion_t completion;
    const char* data;
};

/* the size of connect request */
#define HANDSHAKE_REQ_SIZE 44
/* connect request */
struct connect_req {
    int32_t protocolVersion;
    int64_t lastZxidSeen;
    int32_t timeOut;
    int64_t sessionId;
    int32_t passwd_len;
    char passwd[16];
};

/* the connect response */
struct prime_struct {
    int32_t len;
    int32_t protocolVersion;
    int32_t timeOut;
    int64_t sessionId;
    int32_t passwd_len;
    char passwd[16];
}; 

/* this is used by mt_adaptor internally for thread management */
struct adaptor_threads {
     boost::thread io;
     boost::thread completion;
     int threadsToWait;         // barrier
     boost::condition_variable cond;  // barrier's conditional   
     boost::mutex lock;               // ... and a lock
     boost::mutex zh_lock;            // critical section lock
     int self_pipe[2];
};

/** the auth list for adding auth */
class auth_list_head_t {
  public:
    auth_list_head_t() : authList_(boost::ptr_list<auth_info>()) {
    }
    boost::ptr_list<auth_info> authList_;
    boost::mutex mutex_;
};

/**
 * This structure represents the connection to zookeeper.
 */

struct _zhandle {
    int fd; /* the descriptor used to talk to zookeeper */
    char *hostname; /* the hostname of zookeeper */
    struct sockaddr_storage *addrs; /* the addresses that correspond to the hostname */
    int addrs_count; /* The number of addresses in the addrs array */
    watcher_fn watcher; /* the registered watcher */
    struct timeval last_recv; /* The time that the last message was received */
    struct timeval last_send; /* The time that the last message was sent */
    struct timeval last_ping; /* The time that the last PING was sent */
    struct timeval next_deadline; /* The time of the next deadline */
    int recv_timeout; /* The maximum amount of time that can go by without 
     receiving anything from the zookeeper server */
    buffer_t* input_buffer; /* the current buffer being read in */
    boost::scoped_ptr<buffer_list_t> to_process; /* The buffers that have been read and are ready to be processed. */
    boost::scoped_ptr<buffer_list_t> to_send; /* The packets queued to send */
    completion_head_t sent_requests; /* The outstanding requests */
    completion_head_t completions_to_process; /* completions that are ready to run */
    int connect_index; /* The index of the address to connect to */
    clientid_t client_id;
    long long last_zxid;
    boost::scoped_ptr<proto::ConnectResponse> connectResponse;
    buffer_t primer_buffer; /* The buffer used for the handshake at the start of a connection */
    volatile int state;
    void *context;
    boost::scoped_ptr<auth_list_head_t> auth_h; /* authentication data list */
    /* zookeeper_close is not reentrant because it de-allocates the zhandler. 
     * This guard variable is used to defer the destruction of zhandle till 
     * right before top-level API call returns to the caller */
    uint32_t ref_counter;
    volatile int close_requested;
    void *adaptor_priv;
    /* Used for debugging only: non-zero value indicates the time when the zookeeper_process
     * call returned while there was at least one unprocessed server response 
     * available in the socket recv buffer */
    struct timeval socket_readable;
    zk_hashtable* active_node_watchers;
    zk_hashtable* active_exist_watchers;
    zk_hashtable* active_child_watchers;
    /** used for chroot path at the client side **/
    boost::scoped_ptr<std::string> chroot;
};


int adaptor_init(zhandle_t *zh);
void adaptor_finish(zhandle_t *zh);
void adaptor_destroy(zhandle_t *zh);
struct sync_completion *alloc_sync_completion(void);
int wait_sync_completion(struct sync_completion *sc);
void free_sync_completion(struct sync_completion *sc);
void notify_sync_completion(struct sync_completion *sc);
int adaptor_send_queue(zhandle_t *zh, int timeout);
int process_async(int outstanding_sync);
void process_completions(zhandle_t *zh);
int flush_send_queue(zhandle_t*zh, int timeout);
std::string stripChroot(const std::string& path, const std::string& chroot);
void free_duplicate_path(const char* free_path, const char* path);

// critical section guards
void enter_critical(zhandle_t* zh);
void leave_critical(zhandle_t* zh);
// zhandle object reference counting
void api_prolog(zhandle_t* zh);
int api_epilog(zhandle_t *zh, int rc);
int32_t get_xid();
// returns the new value of the ref counter
uint32_t inc_ref_counter(zhandle_t* zh);
uint32_t dec_ref_counter(zhandle_t* zh);
uint32_t get_ref_counter(zhandle_t* zh);

// in mt mode process session event asynchronously by the completion thread
#define PROCESS_SESSION_EVENT(zh,newstate) queue_session_event(zh,newstate)

#ifdef __cplusplus
}
#endif

#endif /*ZK_ADAPTOR_H_*/


