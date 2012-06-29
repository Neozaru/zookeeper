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
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/ptr_container/ptr_list.hpp>
#include <queue>
#include <zookeeper/zookeeper_const.hh>
#include "zookeeper.h"
#include "zk_hashtable.h"

using namespace org::apache::zookeeper;

/* predefined xid's values recognized as special by the server */
#define WATCHER_EVENT_XID -1
#define PING_XID -2
#define AUTH_XID -4
#define SET_WATCHES_XID -8

#ifdef __cplusplus
extern "C" {
#endif

class completion_list_t;

/**
 * This structure represents a packet being read or written.
 */
class buffer_t {
  public:
    buffer_t() : buffer(""), length(0), offset(0) {
    }
    std::string buffer;
    int32_t length;
    int32_t offset;
};

class buffer_list_t {
  public:
    boost::ptr_list<buffer_t> bufferList_;
    boost::recursive_mutex mutex_;
};

class completion_t {
  public:
    int type; /* one of COMPLETION_* values above */
    union {
        void_completion_t void_result;
        stat_completion_t stat_result;
        data_completion_t data_result;
        strings_completion_t strings_result;
        strings_stat_completion_t strings_stat_result;
        acl_completion_t acl_result;
        string_completion_t string_result;
        multi_completion_t multi_result;
    };
    std::list<watcher_object_t*> watcher_result;
    boost::scoped_ptr<boost::ptr_vector<OpResult> > results; /* For multi-op */
    bool isSynchronous;
};

class completion_list_t {
  public:
    int xid;
    completion_t c;
    const void *data;
    buffer_t *buffer;
    completion_list_t* next;
    watcher_registration_t* watcher;
};

class completion_head_t {
  public:
    std::queue<completion_list_t*> completions;
    boost::shared_ptr<boost::condition_variable> cond;
    boost::shared_ptr<boost::mutex> lock;
};

void lock_completion_list(completion_head_t *l);
void unlock_completion_list(completion_head_t *l);

class auth_info {
  public:
    int state; /* 0=>inactive, >0 => active */
    std::string scheme;
    std::string auth;
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

/* this is used by mt_adaptor internally for thread management */
class adaptor_threads {
  public:
     boost::thread io;
     boost::thread completion;
     int threadsToWait;         // barrier
     boost::condition_variable cond;  // barrier's conditional   
     boost::mutex lock;               // ... and a lock
     int self_pipe[2];
};

/**
 * This structure represents the connection to zookeeper.
 */

class zhandle_t {
  public:
    ~zhandle_t();
    int fd; /* the descriptor used to talk to zookeeper */
    char *hostname; /* the hostname of zookeeper */
    struct sockaddr_storage *addrs; /* the addresses that correspond to the hostname */
    int addrs_count; /* The number of addresses in the addrs array */
    boost::shared_ptr<Watch> watch; /* the registered watcher */
    struct timeval last_recv; /* The time that the last message was received */
    struct timeval last_send; /* The time that the last message was sent */
    struct timeval last_ping; /* The time that the last PING was sent */
    struct timeval next_deadline; /* The time of the next deadline */
    int recv_timeout; /* The maximum amount of time that can go by without 
     receiving anything from the zookeeper server */
    buffer_t* input_buffer; /* the current buffer being read in */
    buffer_list_t to_process; /* The buffers that have been read and are ready to be processed. */
    buffer_list_t to_send; /* The packets queued to send */
    completion_head_t sent_requests; /* The outstanding requests */
    completion_head_t completions_to_process; /* completions that are ready to run */
    int connect_index; /* The index of the address to connect to */
    clientid_t client_id;
    long long last_zxid;
    proto::ConnectResponse connectResponse;
    SessionState::type state;
    void *context;
    boost::ptr_list<auth_info> authList_; /* authentication data list */
    /* zookeeper_close is not reentrant because it de-allocates the zhandler. 
     * This guard variable is used to defer the destruction of zhandle till 
     * right before top-level API call returns to the caller */
    uint32_t ref_counter;
    volatile int close_requested;
    adaptor_threads threads;
    /* Used for debugging only: non-zero value indicates the time when the zookeeper_process
     * call returned while there was at least one unprocessed server response 
     * available in the socket recv buffer */
    struct timeval socket_readable;
    zk_hashtable active_node_watchers;
    zk_hashtable active_exist_watchers;
    zk_hashtable active_child_watchers;
    /** used for chroot path at the client side **/
    std::string chroot;
    boost::mutex mutex; // critical section lock
    static completion_list_t completionOfDeath;
};

int adaptor_init(zhandle_t *zh);
int adaptor_send_queue(zhandle_t *zh, int timeout);
int process_completions(zhandle_t *zh);
int flush_send_queue(zhandle_t*zh, int timeout);
std::string stripChroot(const std::string& path, const std::string& chroot);
void free_duplicate_path(const char* free_path, const char* path);
int32_t get_xid();
int wakeup_io_thread(zhandle_t *zh);
void free_completions(zhandle_t *zh, int reason);

#ifdef __cplusplus
}
#endif

#endif /*ZK_ADAPTOR_H_*/


