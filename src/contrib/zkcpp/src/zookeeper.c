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

#ifndef DLL_EXPORT
#  define USE_STATIC_LIB
#endif

#include <boost/algorithm/string/predicate.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/locks.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <boost/random/mersenne_twister.hpp> // mt19937
#include <boost/random/normal_distribution.hpp>
#include <string>
#include <zookeeper.h>
#include <memory_in_stream.hh>
#include <string_out_stream.hh>
#include <recordio.hh>
#include <binarchive.hh>
#include <proto.h>
#include "zk_adaptor.h"
#include "zookeeper/logging.hh"
ENABLE_LOGGING;
#include "zk_hashtable.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <stdarg.h>
#include <limits.h>

#include <sys/time.h>
#include <sys/socket.h>
#include <poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include "config.h"

#ifdef HAVE_SYS_UTSNAME_H
#include <sys/utsname.h>
#endif

#ifdef HAVE_GETPWUID_R
#include <pwd.h>
#endif

#define IF_DEBUG(x) if(logLevel==ZOO_LOG_LEVEL_DEBUG) {x;}

const int ZOOKEEPER_WRITE = 1 << 0;
const int ZOOKEEPER_READ = 1 << 1;

const int ZOO_EPHEMERAL = 1 << 0;
const int ZOO_SEQUENCE = 1 << 1;

const int ZOO_EXPIRED_SESSION_STATE = EXPIRED_SESSION_STATE_DEF;
const int ZOO_AUTH_FAILED_STATE = AUTH_FAILED_STATE_DEF;
const int ZOO_CONNECTING_STATE = CONNECTING_STATE_DEF;
const int ZOO_ASSOCIATING_STATE = ASSOCIATING_STATE_DEF;
const int ZOO_CONNECTED_STATE = CONNECTED_STATE_DEF;
static __attribute__ ((unused)) const char* state2String(int state){
    switch(state){
    case 0:
        return "ZOO_CLOSED_STATE";
    case CONNECTING_STATE_DEF:
        return "ZOO_CONNECTING_STATE";
    case ASSOCIATING_STATE_DEF:
        return "ZOO_ASSOCIATING_STATE";
    case CONNECTED_STATE_DEF:
        return "ZOO_CONNECTED_STATE";
    case EXPIRED_SESSION_STATE_DEF:
        return "ZOO_EXPIRED_SESSION_STATE";
    case AUTH_FAILED_STATE_DEF:
        return "ZOO_AUTH_FAILED_STATE";
    }
    return "INVALID_STATE";
}

const int ZOO_CREATED_EVENT = CREATED_EVENT_DEF;
const int ZOO_DELETED_EVENT = DELETED_EVENT_DEF;
const int ZOO_CHANGED_EVENT = CHANGED_EVENT_DEF;
const int ZOO_CHILD_EVENT = CHILD_EVENT_DEF;
const int ZOO_SESSION_EVENT = SESSION_EVENT_DEF;
const int ZOO_NOTWATCHING_EVENT = NOTWATCHING_EVENT_DEF;
static __attribute__ ((unused)) const char* watcherEvent2String(int ev){
    switch(ev){
    case 0:
        return "ZOO_ERROR_EVENT";
    case CREATED_EVENT_DEF:
        return "ZOO_CREATED_EVENT";
    case DELETED_EVENT_DEF:
        return "ZOO_DELETED_EVENT";
    case CHANGED_EVENT_DEF:
        return "ZOO_CHANGED_EVENT";
    case CHILD_EVENT_DEF:
        return "ZOO_CHILD_EVENT";
    case SESSION_EVENT_DEF:
        return "ZOO_SESSION_EVENT";
    case NOTWATCHING_EVENT_DEF:
        return "ZOO_NOTWATCHING_EVENT";
    }
    return "INVALID_EVENT";
}

#define COMPLETION_WATCH -1
#define COMPLETION_VOID 0
#define COMPLETION_STAT 1
#define COMPLETION_DATA 2
#define COMPLETION_STRINGLIST 3
#define COMPLETION_STRINGLIST_STAT 4
#define COMPLETION_ACLLIST 5
#define COMPLETION_STRING 6
#define COMPLETION_MULTI 7

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
        struct watcher_object_list *watcher_result;
        multi_completion_t multi_result;
    };
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

const char*err2string(int err);
static int queue_session_event(zhandle_t *zh, int state);
static const char* format_endpoint_info(const struct sockaddr_storage* ep);
static const char* format_current_endpoint_info(zhandle_t* zh);

/* deserialize forward declarations */
static void deserialize_response(int type, int xid, int failed, int rc,
    completion_list_t *cptr, hadoop::IBinArchive& iarchive,
     const std::string& chroot);
static int deserialize_multi(int xid, completion_list_t *cptr,
                             hadoop::IBinArchive& iarchive,
                             boost::ptr_vector<OpResult>& results);

/* completion routine forward declarations */
static int add_completion(zhandle_t *zh, int xid, int completion_type,
        const void *dc, const void *data, watcher_registration_t* wo,
        boost::ptr_vector<OpResult>* results, bool isSynchronous);
static completion_list_t* create_completion_entry(int xid, int completion_type,
        const void *dc, const void *data, watcher_registration_t* wo,
        boost::ptr_vector<OpResult>* results, bool isSynchronous);
static void destroy_completion_entry(completion_list_t* c);
static void queue_completion_nolock(completion_head_t *list, completion_list_t *c);
static void queue_completion(completion_head_t *list, completion_list_t *c);
static int handle_socket_error_msg(zhandle_t *zh, int line, int rc,
                                  const std::string& message);
static void cleanup_bufs(zhandle_t *zh, int rc);

static int disable_conn_permute=0; // permute enabled by default

static int isValidPath(const char* path, const int flags);

static ssize_t zookeeper_send(int s, const void* buf, size_t len)
{
#ifdef __linux__
  return send(s, buf, len, MSG_NOSIGNAL);
#else
  return send(s, buf, len, 0);
#endif
}

const void *zoo_get_context(zhandle_t *zh)
{
    return zh->context;
}

void zoo_set_context(zhandle_t *zh, void *context)
{
    if (zh != NULL) {
        zh->context = context;
    }
}

int zoo_recv_timeout(zhandle_t *zh)
{
    return zh->recv_timeout;
}

int is_unrecoverable(zhandle_t *zh)
{
    return (zh->state<0)? ZINVALIDSTATE: ZOK;
}

zk_hashtable *exists_result_checker(zhandle_t *zh, int rc)
{
    if (rc == ZOK) {
        return &zh->active_node_watchers;
    } else if (rc == ZNONODE) {
        return &zh->active_exist_watchers;
    }
    return 0;
}

zk_hashtable *data_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? &zh->active_node_watchers : 0;
}

zk_hashtable *child_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? &zh->active_child_watchers : 0;
}

/**
 * Frees and closes everything associated with a handle,
 * including the handle itself.
 */
static void destroy(zhandle_t *zh)
{
    if (zh == NULL) {
        return;
    }
    /* call any outstanding completions with a special error code */
    cleanup_bufs(zh, ZCLOSING);
    if (zh->hostname != 0) {
        free(zh->hostname);
        zh->hostname = NULL;
    }
    if (zh->fd != -1) {
        close(zh->fd);
        zh->fd = -1;
        zh->state = 0;
    }
    if (zh->addrs != 0) {
        free(zh->addrs);
        zh->addrs = NULL;
    }

    destroy_zk_hashtable(&zh->active_node_watchers);
    destroy_zk_hashtable(&zh->active_exist_watchers);
    destroy_zk_hashtable(&zh->active_child_watchers);
}

/**
 * get the errno from the return code of get addrinfo. Errno is not set with the
 * call to getaddrinfo, so thats why we have to do this.
 */
static int getaddrinfo_errno(int rc) {
    switch(rc) {
    case EAI_NONAME:
// ZOOKEEPER-1323 EAI_NODATA and EAI_ADDRFAMILY are deprecated in FreeBSD.
#if defined EAI_NODATA && EAI_NODATA != EAI_NONAME
    case EAI_NODATA:
#endif
        return ENOENT;
    case EAI_MEMORY:
        return ENOMEM;
    default:
        return EINVAL;
    }
}

/**
 * fill in the addrs array of the zookeeper servers in the zhandle. after filling
 * them in, we will permute them for load balancing.
 */
int getaddrs(zhandle_t *zh) {
  char *hosts = strdup(zh->hostname);
  char *host;
  char *strtok_last;
  struct sockaddr_storage *addr;
  int i;
  int rc;
  int alen = 0; /* the allocated length of the addrs array */

  zh->addrs_count = 0;
  if (zh->addrs) {
    free(zh->addrs);
    zh->addrs = 0;
  }
  if (!hosts) {
    LOG_ERROR("out of memory");
    errno=ENOMEM;
    return ZSYSTEMERROR;
  }
  zh->addrs = 0;
  host=strtok_r(hosts, ",", &strtok_last);
  while(host) {
    char *port_spec = strrchr(host, ':');
    char *end_port_spec;
    int port;
    if (!port_spec) {
      LOG_ERROR("no port in " << host);
      errno=EINVAL;
      rc=ZBADARGUMENTS;
      goto fail;
    }
    *port_spec = '\0';
    port_spec++;
    port = strtol(port_spec, &end_port_spec, 0);
    if (!*port_spec || *end_port_spec || port == 0) {
      LOG_ERROR("invalid port in " << host);
      errno=EINVAL;
      rc=ZBADARGUMENTS;
      goto fail;
    }
    {
      struct addrinfo hints, *res, *res0;

      memset(&hints, 0, sizeof(hints));
#ifdef AI_ADDRCONFIG
      hints.ai_flags = AI_ADDRCONFIG;
#else
      hints.ai_flags = 0;
#endif
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_protocol = IPPROTO_TCP;

      while(isspace(*host) && host != strtok_last)
        host++;

      if ((rc = getaddrinfo(host, port_spec, &hints, &res0)) != 0) {
        //bug in getaddrinfo implementation when it returns
        //EAI_BADFLAGS or EAI_ADDRFAMILY with AF_UNSPEC and 
        // ai_flags as AI_ADDRCONFIG
#ifdef AI_ADDRCONFIG
        if ((hints.ai_flags == AI_ADDRCONFIG) && 
            // ZOOKEEPER-1323 EAI_NODATA and EAI_ADDRFAMILY are deprecated in FreeBSD.
#ifdef EAI_ADDRFAMILY
            ((rc ==EAI_BADFLAGS) || (rc == EAI_ADDRFAMILY))) {
#else
          (rc == EAI_BADFLAGS)) {
#endif
            //reset ai_flags to null
            hints.ai_flags = 0;
            //retry getaddrinfo
            rc = getaddrinfo(host, port_spec, &hints, &res0);
          }
          if (rc != 0) {
            errno = getaddrinfo_errno(rc);
#if __linux__ && __GNUC__
            LOG_ERROR("getaddrinfo: " << gai_strerror(rc));
#else
            LOG_ERROR("getaddrinfo: " << strerror(errno));
#endif
            rc=ZSYSTEMERROR;
            goto fail;
          }
        }

        for (res = res0; res; res = res->ai_next) {
          // Expand address list if needed
          if (zh->addrs_count == alen) {
            void *tmpaddr;
            alen += 16;
            tmpaddr = realloc(zh->addrs, sizeof(*zh->addrs)*alen);
            if (tmpaddr == 0) {
              LOG_ERROR("out of memory");
              errno=ENOMEM;
              rc=ZSYSTEMERROR;
              freeaddrinfo(res0);
              goto fail;
            }
            zh->addrs=(sockaddr_storage*)tmpaddr;
          }

          // Copy addrinfo into address list
          addr = &zh->addrs[zh->addrs_count];
          switch (res->ai_family) {
            case AF_INET:
#if defined(AF_INET6)
            case AF_INET6:
#endif
              memcpy(addr, res->ai_addr, res->ai_addrlen);
              ++zh->addrs_count;
              break;
            default:
              LOG_WARN(
                  boost::format("skipping unknown address family %x for %s") %
                  res->ai_family % zh->hostname);
              break;
          }
        }

        freeaddrinfo(res0);

        host = strtok_r(0, ",", &strtok_last);
      }
#endif
    }
    free(hosts);

    if(!disable_conn_permute){
      srandom(time(NULL));
      /* Permute */
      for (i = zh->addrs_count - 1; i > 0; --i) {
        long int j = random()%(i+1);
        if (i != j) {
          struct sockaddr_storage t = zh->addrs[i];
          zh->addrs[i] = zh->addrs[j];
          zh->addrs[j] = t;
        }
      }
    }
    return ZOK;
fail:
    if (zh->addrs) {
      free(zh->addrs);
      zh->addrs=0;
    }
    if (hosts) {
      free(hosts);
    }
    return rc;
  }

const clientid_t *zoo_client_id(zhandle_t *zh)
{
    return &zh->client_id;
}

static void null_watcher_fn(zhandle_t* p1, int p2, int p3,const char* p4,void*p5){}

watcher_fn zoo_set_watcher(zhandle_t *zh,watcher_fn newFn)
{
    watcher_fn oldWatcher=zh->watcher;
    if (newFn) {
       zh->watcher = newFn;
    } else {
       zh->watcher = null_watcher_fn;
    }
    return oldWatcher;
}

struct sockaddr* zookeeper_get_connected_host(zhandle_t *zh,
                 struct sockaddr *addr, socklen_t *addr_len)
{
    if (zh->state!=ZOO_CONNECTED_STATE) {
        return NULL;
    }
    if (getpeername(zh->fd, addr, addr_len)==-1) {
        return NULL;
    }
    return addr;
}

// TODO Fix these macros
static void log_env() {
  char buf[2048];
#ifdef HAVE_SYS_UTSNAME_H
  struct utsname utsname;
#endif

#if defined(HAVE_GETUID) && defined(HAVE_GETPWUID_R)
  struct passwd pw;
  struct passwd *pwp = NULL;
  uid_t uid = 0;
#endif

  LOG_INFO("Client environment:zookeeper.version=" << PACKAGE_STRING);

#ifdef HAVE_GETHOSTNAME
  gethostname(buf, sizeof(buf));
  LOG_INFO("Client environment:host.name=" << buf);
#else
  LOG_INFO("Client environment:host.name=<not implemented>");
#endif

#ifdef HAVE_SYS_UTSNAME_H
  uname(&utsname);
  LOG_INFO("Client environment:os.name=" << utsname.sysname);
  LOG_INFO("Client environment:os.arch=" << utsname.release);
  LOG_INFO("Client environment:os.version=" << utsname.version);
#else
  LOG_INFO("Client environment:os.name=<not implemented>");
  LOG_INFO("Client environment:os.arch=<not implemented>");
  LOG_INFO("Client environment:os.version=<not implemented>");
#endif

#ifdef HAVE_GETLOGIN
  LOG_INFO("Client environment:user.name=" << getlogin());
#else
  LOG_INFO("Client environment:user.name=<not implemented>");
#endif

#if defined(HAVE_GETUID) && defined(HAVE_GETPWUID_R)
  uid = getuid();
  if (!getpwuid_r(uid, &pw, buf, sizeof(buf), &pwp) && pwp) {
    LOG_INFO("Client environment:user.home=" << pw.pw_dir);
  } else {
    LOG_INFO("Client environment:user.home=<NA>");
  }
#else
  LOG_INFO("Client environment:user.home=<not implemented>");
#endif

#ifdef HAVE_GETCWD
  if (!getcwd(buf, sizeof(buf))) {
    LOG_INFO("Client environment:user.dir=<toolong>");
  } else {
    LOG_INFO("Client environment:user.dir=" << buf);
  }
#else
  LOG_INFO("Client environment:user.dir=<not implemented>");
#endif
}

/**
 * Create a zookeeper handle associated with the given host and port.
 */
zhandle_t *zookeeper_init(const char *host, watcher_fn watcher,
  int recv_timeout, const clientid_t *clientid, void *context, int flags)
{
    int errnosave = 0;
    zhandle_t *zh = NULL;
    char *index_chroot = NULL;

    log_env();
    LOG_INFO(
      boost::format("Initiating client connection, host=%s sessionTimeout=%d "
                    "watcher=%p sessionId=%#llx sessionPasswd=%s context=%p flags=%d") %
                    host % recv_timeout % watcher %
                    (clientid == 0 ? 0 : clientid->client_id) %
                    ((clientid == 0) || (clientid->passwd[0] == 0) ?
                     "<null>" : "<hidden>") % context % flags);

    zh = new zhandle_t();
    zh->sent_requests.lock.reset(new boost::mutex());
    zh->sent_requests.cond.reset(new boost::condition_variable());
    zh->completions_to_process.lock.reset(new boost::mutex());
    zh->completions_to_process.cond.reset(new boost::condition_variable());

    zh->ref_counter = 0;
    zh->fd = -1;
    zh->state = NOTCONNECTED_STATE_DEF;
    zh->context = context;
    zh->recv_timeout = recv_timeout;
    if (watcher) {
       zh->watcher = watcher;
    } else {
       zh->watcher = null_watcher_fn;
    }
    if (host == 0 || *host == 0) { // what we shouldn't dup
        errno=EINVAL;
        goto abort;
    }
    //parse the host to get the chroot if
    //available
    index_chroot = (char*)strchr(host, '/');
    if (index_chroot) {
        zh->chroot = index_chroot;
        // if chroot is just / set it to null
        if (zh->chroot == "/") {
          zh->chroot = "";
        }
        // cannot use strndup so allocate and strcpy
        zh->hostname = (char *) malloc(index_chroot - host + 1);
        zh->hostname = strncpy(zh->hostname, host, (index_chroot - host));
        //strncpy does not null terminate
        *(zh->hostname + (index_chroot - host)) = '\0';

    } else {
        zh->hostname = strdup(host);
    }
    if (!zh->chroot.empty() && !isValidPath(zh->chroot.c_str(), 0)) {
        errno = EINVAL;
        goto abort;
    }
    if (zh->hostname == 0) {
        goto abort;
    }
    if(getaddrs(zh)!=0) {
        goto abort;
    }
    zh->connect_index = 0;
    if (clientid) {
        memcpy(&zh->client_id, clientid, sizeof(zh->client_id));
    } else {
        memset(&zh->client_id, 0, sizeof(zh->client_id));
    }
    zh->last_zxid = 0;
    zh->next_deadline.tv_sec=zh->next_deadline.tv_usec=0;
    zh->socket_readable.tv_sec=zh->socket_readable.tv_usec=0;

    if (adaptor_init(zh) == -1) {
        goto abort;
    }

    LOG_DEBUG("Init completed. ZooKeeper state="<< state2String(zh->state));
    return zh;
abort:
    errnosave=errno;
    destroy(zh);
    delete zh;
    errno=errnosave;
    return 0;
}

/**
 * deallocated the free_path only its beeen allocated
 * and not equal to path
 */
void free_duplicate_path(const char *free_path, const char* path) {
    if (free_path != path) {
        free((void*)free_path);
    }
}

/**
  prepend the chroot path if available else return the path
*/
static char* prepend_string(zhandle_t *zh, const char* client_path) {
    char *ret_str;
    if (zh == NULL || zh->chroot.empty()) {
        return (char *) client_path;
    }
    // handle the chroot itself, client_path = "/"
    if (strlen(client_path) == 1) {
        return strdup(zh->chroot.c_str());
    }
    ret_str = (char *) malloc(zh->chroot.size() + strlen(client_path) + 1);
    strcpy(ret_str, zh->chroot.c_str());
    return strcat(ret_str, client_path);
}

/**
   strip off the chroot string from the server path
   if there is one else return the exact path
 */
std::string stripChroot(const std::string& path, const std::string& chroot) {
  //ZOOKEEPER-1027
  if (!boost::starts_with(path, chroot)) {
    LOG_ERROR(boost::format("server path %s does not include chroot path %s") %
              path % chroot);
    return path;
  }
  if (path == chroot) {
    return "/";
  }
  return path.substr(chroot.size());
}

static buffer_t *dequeue_buffer(buffer_list_t *list) {
  boost::lock_guard<boost::recursive_mutex> lock(list->mutex_);
  if (list->bufferList_.empty()) {
    return NULL;
  }
  return list->bufferList_.release(list->bufferList_.begin()).release();
}

static int remove_buffer(buffer_list_t *list)
{
    buffer_t *b = dequeue_buffer(list);
    if (!b) {
        return 0;
    }
    delete b;
    return 1;
}

static int queue_buffer_bytes(buffer_list_t *list, char *buff, int len) {
  buffer_t *b  = new buffer_t(buff,len);
  boost::lock_guard<boost::recursive_mutex> lock(list->mutex_);
  list->bufferList_.push_back(b);
  return ZOK;
}

/* returns:
 * -1 if send failed,
 * 0 if send would block while sending the buffer (or a send was incomplete),
 * 1 if success
 */
static int send_buffer(int fd, buffer_t *buff)
{
    int len = buff->len;
    int off = buff->curr_offset;
    int rc = -1;

    if (off < 4) {
        /* we need to send the length at the beginning */
        int nlen = htonl(len);
        char *b = (char*)&nlen;
        rc = zookeeper_send(fd, b + off, sizeof(nlen) - off);
        if (rc == -1) {
            if (errno != EAGAIN) {
                return -1;
            } else {
                return 0;
            }
        } else {
            buff->curr_offset  += rc;
        }
        off = buff->curr_offset;
    }
    if (off >= 4) {
        /* want off to now represent the offset into the buffer */
        off -= sizeof(buff->len);
        rc = zookeeper_send(fd, buff->buffer + off, len - off);
        if (rc == -1) {
            if (errno != EAGAIN) {
                return -1;
            }
        } else {
            buff->curr_offset += rc;
        }
    }
    return buff->curr_offset == len + (int)sizeof(buff->len);
}

/* returns:
 * -1 if recv call failed,
 * 0 if recv would block,
 * 1 if success
 */
static int recv_buffer(int fd, buffer_t *buff) {
    int off = buff->curr_offset;
    int rc = 0;
    //fprintf(LOGSTREAM, "rc = %d, off = %d, line %d\n", rc, off, __LINE__);

    /* if buffer is less than 4, we are reading in the length */
    if (off < 4) {
        char *buffer = (char*)&(buff->len);
        rc = recv(fd, buffer+off, sizeof(int)-off, 0);
        //fprintf(LOGSTREAM, "rc = %d, off = %d, line %d\n", rc, off, __LINE__);
        switch(rc) {
        case 0:
            errno = EHOSTDOWN;
        case -1:
            if (errno == EAGAIN) {
                return 0;
            }
            return -1;
        default:
            buff->curr_offset += rc;
        }
        off = buff->curr_offset;
        if (buff->curr_offset == sizeof(buff->len)) {
            buff->len = ntohl(buff->len);
            buff->buffer = new char[buff->len];
        }
    }
    if (buff->buffer) {
        /* want off to now represent the offset into the buffer */
        off -= sizeof(buff->len);

        rc = recv(fd, buff->buffer+off, buff->len-off, 0);
        switch(rc) {
        case 0:
            errno = EHOSTDOWN;
        case -1:
            if (errno == EAGAIN) {
                break;
            }
            return -1;
        default:
            buff->curr_offset += rc;
        }
    }
    return buff->curr_offset == buff->len + (int)sizeof(buff->len);
}

void free_buffers(buffer_list_t *list)
{
    while (remove_buffer(list))
        ;
}

void free_completions(zhandle_t *zh, int reason) {
  completion_head_t tmp_list;
  {
    boost::lock_guard<boost::mutex> lock(*(zh->sent_requests.lock));
    tmp_list = zh->sent_requests;
    zh->sent_requests.head = 0;
    zh->sent_requests.last = 0;
    (*(zh->sent_requests.cond)).notify_all();
  }
  while (tmp_list.head) {
    completion_list_t *cptr = tmp_list.head;

    tmp_list.head = cptr->next;
    if(cptr->xid == PING_XID){
      // Nothing to do with a ping response
      destroy_completion_entry(cptr);
    } else if (cptr->c.isSynchronous) {
      buffer_t *bptr = cptr->buffer;
      MemoryInStream stream(bptr->buffer, bptr->len);
      hadoop::IBinArchive iarchive(stream);
      deserialize_response(cptr->c.type, cptr->xid, true,
          reason, cptr, iarchive, zh->chroot);
    } else {
      // Fake the response
      std::string serialized;
      StringOutStream stream(serialized);
      hadoop::OBinArchive oarchive(stream);
      proto::ReplyHeader header;
      header.setxid(cptr->xid);
      header.setzxid(-1);
      header.seterr(reason);
      header.serialize(oarchive, "header");
      char* data = new char[serialized.size()];
      memmove(data, serialized.c_str(), serialized.size());

      buffer_t *bptr;
      bptr = new buffer_t();
      bptr->buffer = data;
      bptr->len = serialized.size();
      cptr->buffer = bptr;
      queue_completion(&zh->completions_to_process, cptr);
    }
  }
  {
    boost::lock_guard<boost::mutex> lock(zh->auth_h.mutex_);
    BOOST_FOREACH(auth_info& info, zh->auth_h.authList_) {
      if (info.completion) {
        info.completion(reason, info.data);
        info.data = NULL;
        info.completion = NULL;
      }
    }
  }
}

static void cleanup_bufs(zhandle_t *zh, int rc)
{
    enter_critical(zh);
    free_buffers(&zh->to_send);
    free_buffers(&zh->to_process);
    free_completions(zh, rc);
    leave_critical(zh);
    if (zh->input_buffer != NULL) {
        delete zh->input_buffer;
        zh->input_buffer = 0;
    }
}

static void handle_error(zhandle_t *zh,int rc)
{
    close(zh->fd);
    if (is_unrecoverable(zh)) {
        LOG_DEBUG("Calling a watcher for a ZOO_SESSION_EVENT and the state=" <<
                  state2String(zh->state));
        PROCESS_SESSION_EVENT(zh, zh->state);
    } else if (zh->state == ZOO_CONNECTED_STATE) {
        LOG_DEBUG("Calling a watcher for a ZOO_SESSION_EVENT and the state=CONNECTING_STATE");
        PROCESS_SESSION_EVENT(zh, ZOO_CONNECTING_STATE);
    }
    cleanup_bufs(zh, rc);
    zh->fd = -1;
    zh->connect_index++;
    if (!is_unrecoverable(zh)) {
        zh->state = 0;
    }
}

static int handle_socket_error_msg(zhandle_t *zh, int line, int rc,
        const std::string& message)
{
    LOG_ERROR(boost::format("%s:%d Socket [%s] zk retcode=%d, errno=%d(%s): %s") %
            __func__ % line % format_current_endpoint_info(zh) %
            rc % errno % strerror(errno) % message);
    handle_error(zh,rc);
    return rc;
}

static void auth_completion_func(int rc, zhandle_t* zh) {
  if (zh == NULL) {
    return;
  }

  boost::lock_guard<boost::mutex> lock(zh->auth_h.mutex_);
  if (rc != 0) {
    LOG_ERROR("Authentication scheme " << zh->auth_h.authList_.front().scheme <<
        " failed. Connection closed.");
    zh->state=ZOO_AUTH_FAILED_STATE;
  }else{
    LOG_INFO("Authentication scheme " << zh->auth_h.authList_.front().scheme <<
        " succeeded.");
    //change state for all auths
    BOOST_FOREACH(auth_info& info, zh->auth_h.authList_) {
      info.state = 1;
      if (info.completion) {
        info.completion(rc, info.data);
        info.completion = NULL;
        info.data = NULL;
      }
    }
  }
}

static int send_info_packet(zhandle_t *zh, auth_info* auth) {
  int rc = 0;
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(AUTH_XID);
  header.settype(ZOO_SETAUTH_OP);
  header.serialize(oarchive, "header");

  proto::AuthPacket req;
  req.settype(0); // ignored by the server
  req.getscheme() = auth->scheme;
  req.getauth() = auth->auth;
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* data = new char[serialized.size()];
  memmove(data, serialized.c_str(), serialized.size());
  rc = queue_buffer_bytes(&zh->to_send,
        data, serialized.size());
  adaptor_send_queue(zh, 0);
  return rc;
}

/** send all auths, not just the last one **/
static int send_auth_info(zhandle_t *zh) {
    int rc = 0;
    {
        boost::lock_guard<boost::mutex> lock(zh->auth_h.mutex_);
        BOOST_FOREACH(auth_info& info, zh->auth_h.authList_) {
          rc = send_info_packet(zh, &info);
        }
    }
    LOG_DEBUG("Sending all auth info request to " << format_current_endpoint_info(zh));
    return (rc <0) ? ZMARSHALLINGERROR:ZOK;
}

static int send_last_auth_info(zhandle_t *zh) {
  int rc = 0;
  {
    boost::lock_guard<boost::mutex> lock(zh->auth_h.mutex_);
    if (zh->auth_h.authList_.empty()) {
      return ZOK; // there is nothing to send
    }
    rc = send_info_packet(zh, &(zh->auth_h.authList_.back()));
  }
  LOG_DEBUG("Sending auth info request to " << format_current_endpoint_info(zh));
  return (rc < 0) ? ZMARSHALLINGERROR : ZOK;
}

static int send_set_watches(zhandle_t *zh) {
  int rc = 0;
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(SET_WATCHES_XID);
  header.settype(ZOO_SETWATCHES_OP);

  proto::SetWatches req;
  std::vector<std::string> paths;
  req.setrelativeZxid(zh->last_zxid);
  collectKeys(&zh->active_node_watchers, req.getdataWatches());
  collectKeys(&zh->active_exist_watchers, req.getexistWatches());
  collectKeys(&zh->active_child_watchers, req.getchildWatches());

  // return if there are no pending watches
  if (req.getdataWatches().empty() && req.getexistWatches().empty() &&
      req.getchildWatches().empty()) {
    return ZOK;
  }

  header.serialize(oarchive, "header");
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* data = new char[serialized.size()];
  memmove(data, serialized.c_str(), serialized.size());
  rc = queue_buffer_bytes(&zh->to_send,
        data, serialized.size());

  enter_critical(zh);
  adaptor_send_queue(zh, 0);
  leave_critical(zh);

  LOG_DEBUG("Sending SetWatches request to " << format_current_endpoint_info(zh));
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

static int sendConnectRequest(zhandle_t *zh) {
  int rc;
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::ConnectRequest request;
  request.setprotocolVersion(0);
  request.settimeOut(zh->recv_timeout);
  request.setlastZxidSeen(zh->last_zxid);
  request.setsessionId(zh->client_id.client_id);
  request.getpasswd() = std::string(zh->client_id.passwd,
      sizeof(zh->client_id.passwd));
  request.serialize(oarchive, "connect");
  uint32_t len = htonl(serialized.size());
  rc=zookeeper_send(zh->fd, &len, sizeof(len));
  rc=rc<0 ? rc : zookeeper_send(zh->fd, serialized.data(), serialized.size());
  if (rc<0) {
    return handle_socket_error_msg(zh, __LINE__, ZCONNECTIONLOSS, "");
  }
  zh->state = ZOO_ASSOCIATING_STATE;
  return ZOK;
}

static inline int calculate_interval(const struct timeval *start,
        const struct timeval *end)
{
    int interval;
    struct timeval i = *end;
    i.tv_sec -= start->tv_sec;
    i.tv_usec -= start->tv_usec;
    interval = i.tv_sec * 1000 + (i.tv_usec/1000);
    return interval;
}

static struct timeval get_timeval(int interval)
{
    struct timeval tv;
    if (interval < 0) {
        interval = 0;
    }
    tv.tv_sec = interval/1000;
    tv.tv_usec = (interval%1000)*1000;
    return tv;
}

 static int add_void_completion(zhandle_t *zh, int xid, void_completion_t dc,
     const void *data, bool isSynchronous);
 static int add_string_completion(zhandle_t *zh, int xid,
     string_completion_t dc, const void *data, bool isSynchronous);

 int
 send_ping(zhandle_t* zh) {
  int rc = 0;
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(PING_XID);
  header.settype(ZOO_PING_OP);
  header.serialize(oarchive, "header");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* data = new char[serialized.size()];
  memmove(data, serialized.c_str(), serialized.size());

  enter_critical(zh);
  gettimeofday(&zh->last_ping, 0);
  rc = rc < 0 ? rc : add_void_completion(zh, header.getxid(), 0, 0, false);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, data, serialized.size());
  leave_critical(zh);
  return rc<0 ? rc : adaptor_send_queue(zh, 0);
 }

int zookeeper_interest(zhandle_t *zh, int *fd, int *interest,
     struct timeval *tv) {
    struct timeval now;
    if(zh==0 || fd==0 ||interest==0 || tv==0)
        return ZBADARGUMENTS;
    if (is_unrecoverable(zh))
        return ZINVALIDSTATE;
    gettimeofday(&now, 0);
    if(zh->next_deadline.tv_sec!=0 || zh->next_deadline.tv_usec!=0){
        int time_left = calculate_interval(&zh->next_deadline, &now);
        int max_exceed = zh->recv_timeout / 10 > 200 ? 200 : 
                         (zh->recv_timeout / 10);
        if (time_left > max_exceed)
            LOG_WARN("Exceeded deadline by " << time_left << "ms");
    }
    api_prolog(zh);
    *fd = zh->fd;
    *interest = 0;
    tv->tv_sec = 0;
    tv->tv_usec = 0;
    if (*fd == -1) {
        if (zh->connect_index == zh->addrs_count) {
            /* Wait a bit before trying again so that we don't spin */
            zh->connect_index = 0;
        }else {
            int rc;
            int enable_tcp_nodelay = 1;
            int ssoresult;

            zh->fd = socket(zh->addrs[zh->connect_index].ss_family, SOCK_STREAM, 0);
            if (zh->fd < 0) {
                return api_epilog(zh,handle_socket_error_msg(zh,__LINE__,
                                                             ZSYSTEMERROR, "socket() call failed"));
            }
            ssoresult = setsockopt(zh->fd, IPPROTO_TCP, TCP_NODELAY, &enable_tcp_nodelay, sizeof(enable_tcp_nodelay));
            if (ssoresult != 0) {
                LOG_WARN("Unable to set TCP_NODELAY, operation latency may be effected");
            }
            fcntl(zh->fd, F_SETFL, O_NONBLOCK|fcntl(zh->fd, F_GETFL, 0));
#if defined(AF_INET6)
            if (zh->addrs[zh->connect_index].ss_family == AF_INET6) {
                rc = connect(zh->fd, (struct sockaddr*) &zh->addrs[zh->connect_index], sizeof(struct sockaddr_in6));
            } else {
#else
               LOG_DEBUG("[zk] connect()");
            {
#endif
                rc = connect(zh->fd, (struct sockaddr*) &zh->addrs[zh->connect_index], sizeof(struct sockaddr_in));
            }
            if (rc == -1) {
                /* we are handling the non-blocking connect according to
                 * the description in section 16.3 "Non-blocking connect"
                 * in UNIX Network Programming vol 1, 3rd edition */
                if (errno == EWOULDBLOCK || errno == EINPROGRESS)
                    zh->state = ZOO_CONNECTING_STATE;
                else
                    return api_epilog(zh,handle_socket_error_msg(zh,__LINE__,
                            ZCONNECTIONLOSS,"connect() call failed"));
            } else {
                if((rc=sendConnectRequest(zh))!=0)
                    return api_epilog(zh,rc);

                LOG_INFO("Initiated connection to server: " <<
                        format_endpoint_info(&zh->addrs[zh->connect_index]));
            }
        }
        *fd = zh->fd;
        *tv = get_timeval(zh->recv_timeout/3);
        zh->last_recv = now;
        zh->last_send = now;
        zh->last_ping = now;
    }
    if (zh->fd != -1) {
        int idle_recv = calculate_interval(&zh->last_recv, &now);
        int idle_send = calculate_interval(&zh->last_send, &now);
        int recv_to = zh->recv_timeout*2/3 - idle_recv;
        int send_to = zh->recv_timeout/3;
        // have we exceeded the receive timeout threshold?
        if (recv_to <= 0) {
            // We gotta cut our losses and connect to someone else
            errno = ETIMEDOUT;
            *fd=-1;
            *interest=0;
            *tv = get_timeval(0);
            return api_epilog(zh,handle_socket_error_msg(zh,
                  __LINE__,ZOPERATIONTIMEOUT, ""));
        }
        // We only allow 1/3 of our timeout time to expire before sending
        // a PING
        if (zh->state==ZOO_CONNECTED_STATE) {
            send_to = zh->recv_timeout/3 - idle_send;
            if (send_to <= 0 && zh->sent_requests.head==0) {
//                LOG_DEBUG(("Sending PING to %s (exceeded idle by %dms)",
//                                format_current_endpoint_info(zh),-send_to));
                int rc=send_ping(zh);
                if (rc < 0){
                    //LOG_ERROR("failed to send PING request (zk retcode=" << rc << ")");
                    return api_epilog(zh,rc);
                }
                send_to = zh->recv_timeout/3;
            }
        }
        // choose the lesser value as the timeout
        *tv = get_timeval(recv_to < send_to? recv_to:send_to);
        zh->next_deadline.tv_sec = now.tv_sec + tv->tv_sec;
        zh->next_deadline.tv_usec = now.tv_usec + tv->tv_usec;
        if (zh->next_deadline.tv_usec > 1000000) {
            zh->next_deadline.tv_sec += zh->next_deadline.tv_usec / 1000000;
            zh->next_deadline.tv_usec = zh->next_deadline.tv_usec % 1000000;
        }
        *interest = ZOOKEEPER_READ;
        /* we are interested in a write if we are connected and have something
         * to send, or we are waiting for a connect to finish. */
        if ((!zh->to_send.bufferList_.empty() &&
            zh->state == ZOO_CONNECTED_STATE) ||
            zh->state == ZOO_CONNECTING_STATE) {
            *interest |= ZOOKEEPER_WRITE;
        }
    }
    return api_epilog(zh,ZOK);
}

static int check_events(zhandle_t *zh, int events)
{
    if (zh->fd == -1)
        return ZINVALIDSTATE;
    if ((events&ZOOKEEPER_WRITE)&&(zh->state == ZOO_CONNECTING_STATE)) {
        int rc, error;
        socklen_t len = sizeof(error);
        rc = getsockopt(zh->fd, SOL_SOCKET, SO_ERROR, &error, &len);
        /* the description in section 16.4 "Non-blocking connect"
         * in UNIX Network Programming vol 1, 3rd edition, points out
         * that sometimes the error is in errno and sometimes in error */
        if (rc < 0 || error) {
            if (rc == 0)
                errno = error;
            return handle_socket_error_msg(zh, __LINE__,ZCONNECTIONLOSS,
                "server refused to accept the client");
        }
        if((rc=sendConnectRequest(zh))!=0)
            return rc;
        LOG_INFO("initiated connection to server: " <<
                format_endpoint_info(&zh->addrs[zh->connect_index]));
        return ZOK;
    }
    if (!(zh->to_send.bufferList_.empty()) && (events&ZOOKEEPER_WRITE)) {
        /* make the flush call non-blocking by specifying a 0 timeout */
        int rc=flush_send_queue(zh,0);
        if (rc < 0)
            return handle_socket_error_msg(zh,__LINE__,ZCONNECTIONLOSS,
                "failed while flushing send queue");
    }
    if (events&ZOOKEEPER_READ) {
        int rc;
        if (zh->input_buffer == 0) {
            zh->input_buffer = new buffer_t(0,0);
        }

        rc = recv_buffer(zh->fd, zh->input_buffer);
        if (rc < 0) {
            delete zh->input_buffer;
            zh->input_buffer = NULL;
            return handle_socket_error_msg(zh, __LINE__,ZCONNECTIONLOSS,
                "failed while receiving a server response");
        }
        if (rc > 0) {
            gettimeofday(&zh->last_recv, 0);
            if (zh->state != ZOO_ASSOCIATING_STATE) {
                boost::lock_guard<boost::recursive_mutex> lock(zh->to_process.mutex_);
                zh->to_process.bufferList_.push_back(zh->input_buffer);
            } else  {
                // Process connect response.
                int64_t oldid,newid;
                MemoryInStream istream((zh->input_buffer->buffer), zh->input_buffer->len);
                hadoop::IBinArchive iarchive(istream);
                zh->connectResponse.deserialize(iarchive,"connect");

                /* We are processing the connect response , so we need to finish
                 * the connection handshake */
                oldid = zh->client_id.client_id;
                newid = zh->connectResponse.getsessionId();
                delete zh->input_buffer;
                zh->input_buffer = NULL;
                if (oldid != 0 && oldid != newid) {
                    zh->state = ZOO_EXPIRED_SESSION_STATE;
                    errno = ESTALE;
                    return handle_socket_error_msg(zh,__LINE__,ZSESSIONEXPIRED,
                    "");
                } else {
                    zh->recv_timeout = zh->connectResponse.gettimeOut();
                    zh->client_id.client_id = newid;

                    memcpy(zh->client_id.passwd, zh->connectResponse.getpasswd().c_str(),
                           sizeof(zh->client_id.passwd));
                    zh->state = ZOO_CONNECTED_STATE;
                    LOG_INFO(
                      boost::format("session establishment complete on server [%s], sessionId=%#llx, negotiated timeout=%d") %
                              format_endpoint_info(&zh->addrs[zh->connect_index]) % newid % zh->recv_timeout);
                    /* we want the auth to be sent for, but since both call push to front
                       we need to call send_watch_set first */
                    send_set_watches(zh);
                    /* send the authentication packet now */
                    send_auth_info(zh);
                    LOG_DEBUG("Calling a watcher for a ZOO_SESSION_EVENT and the state=ZOO_CONNECTED_STATE");
                    PROCESS_SESSION_EVENT(zh, ZOO_CONNECTED_STATE);
                }
            }
            zh->input_buffer = 0;
        } else {
            // zookeeper_process was called but there was nothing to read
            // from the socket
            return ZNOTHING;
        }
    }
    return ZOK;
}

void api_prolog(zhandle_t* zh)
{
    inc_ref_counter(zh);
}

int api_epilog(zhandle_t *zh,int rc)
{
    if(dec_ref_counter(zh)==0 && zh->close_requested!=0)
        zookeeper_close(zh);
    return rc;
}

// IO thread queues session events to be processed by the completion thread
static int queue_session_event(zhandle_t *zh, int state) {
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);
  completion_list_t *cptr;
  proto::ReplyHeader header;
  header.setxid(WATCHER_EVENT_XID);
  header.setzxid(0);
  header.seterr(0);
  proto::WatcherEvent event;
  event.settype(ZOO_SESSION_EVENT);
  event.setstate(state);
  event.getpath() = "";

  header.serialize(oarchive, "header");
  event.serialize(oarchive, "event");
  cptr = create_completion_entry(WATCHER_EVENT_XID,-1,0,0,0,0, false);

  // TODO(michim) avoid copy
  char* data = new char[serialized.size()];
  memmove(data, serialized.c_str(), serialized.size());

  cptr->buffer = new buffer_t(data, serialized.size());
  cptr->buffer->curr_offset = serialized.size();
  cptr->c.watcher_result = collectWatchers(zh, ZOO_SESSION_EVENT, "");
  queue_completion(&zh->completions_to_process, cptr);
  return ZOK;
}

completion_list_t *dequeue_completion(completion_head_t *list)
{
    completion_list_t *cptr;
    {
        boost::lock_guard<boost::mutex> lock(*(list->lock));
        cptr = list->head;
        if (cptr) {
            assert(list);
            assert(cptr);
            list->head = cptr->next;
            if (!list->head) {
                assert(list->last == cptr);
                list->last = 0;
            }
        }
        list->cond->notify_all();
    }
    return cptr;
}

static int
deserialize_multi(int xid, completion_list_t *cptr,
                  hadoop::IBinArchive& iarchive,
                  boost::ptr_vector<OpResult>& results) {

  boost::ptr_vector<OpResult> temp;
  int rc = 0;
  boost::ptr_vector<OpResult>* clist = cptr->c.results.get();
  assert(clist);
  proto::MultiHeader mheader;
  mheader.deserialize(iarchive, "multiheader");
  while (!mheader.getdone()) {
    if (mheader.gettype() == -1) {
      proto::ErrorResponse errorResponse;
      errorResponse.deserialize(iarchive, "error");
      ReturnCode::type error = (ReturnCode::type)errorResponse.geterr();
      LOG_DEBUG("got error response for: " << ReturnCode::toString(error));
      mheader.seterr(error);
      OpResult* result = clist->release(clist->begin()).release();
      result->setReturnCode(error);
      results.push_back(result);
      if (rc == 0 && (int)error != 0 && (int)error != ZRUNTIMEINCONSISTENCY) {
        rc = (int)error;
      }
    } else {
      switch((clist->begin())->getType()) {
        case OpCode::Remove: {
          ReturnCode::type zrc = (ReturnCode::type) mheader.geterr();
          LOG_DEBUG("got delete response for: " << ReturnCode::toString(zrc));
          OpResult* result = clist->release(clist->begin()).release();
          results.push_back(result);
          break;
          }
        case OpCode::Create: {
          proto::CreateResponse res;
          res.deserialize(iarchive, "reply");
          ReturnCode::type zrc = (ReturnCode::type) mheader.geterr();
          LOG_DEBUG("got create response for: " << res.getpath() << ": " <<
                    ReturnCode::toString(zrc));
          OpResult* result = clist->release(clist->begin()).release();
          dynamic_cast<OpResult::Create*>(result)->setPathCreated(res.getpath());
          results.push_back(result);
          break;
        }
        case OpCode::SetData: {
          proto::SetDataResponse res;
          res.deserialize(iarchive, "reply");
          ReturnCode::type zrc = (ReturnCode::type) mheader.geterr();
          LOG_DEBUG("got setData response: " << ReturnCode::toString(zrc));
          OpResult* result = clist->release(clist->begin()).release();
          dynamic_cast<OpResult::SetData*>(result)->setStat(res.getstat());
          results.push_back(result);
          break;
        }
        case OpCode::Check: {
          ReturnCode::type zrc = (ReturnCode::type) mheader.geterr();
          LOG_DEBUG("got check response for: " << ReturnCode::toString(zrc));
          OpResult* result = clist->release(clist->begin()).release();
          results.push_back(result);
          break;
        }
      }
    }
    mheader.deserialize(iarchive, "multiheader");
  }
  LOG_DEBUG("returning: " << ReturnCode::toString((ReturnCode::type)rc));
  return rc;
}

static void deserialize_response(int type, int xid, int failed, int rc,
    completion_list_t *cptr, hadoop::IBinArchive& iarchive,
    const std::string& chroot) {
  switch (type) {
    case COMPLETION_DATA:
      LOG_DEBUG(boost::format("Calling COMPLETION_DATA for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      if (failed) {
        data::Stat stat;
        cptr->c.data_result(rc, "", stat, cptr->data);
      } else {
        proto::GetDataResponse res;
        res.deserialize(iarchive, "reply");
        cptr->c.data_result(rc, res.getdata(), res.getstat(), cptr->data);
      }
      break;
    case COMPLETION_STAT:
      LOG_DEBUG(boost::format("Calling COMPLETION_STAT for xid=%#08x failed=%d rc=%d") % 
          cptr->xid % failed % rc);
      if (failed) {
        data::Stat stat;
        cptr->c.stat_result(rc, stat, cptr->data);
      } else {
        proto::SetDataResponse res;
        res.deserialize(iarchive, "reply");
        cptr->c.stat_result(rc, res.getstat(), cptr->data);
      }
      break;
    case COMPLETION_STRINGLIST:
      LOG_DEBUG(boost::format("Calling COMPLETION_STRINGLIST for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      if (failed) {
        std::vector<std::string> res;
        cptr->c.strings_result(rc, res, cptr->data);
      } else {
        proto::GetChildrenResponse res;
        res.deserialize(iarchive, "reply");
        cptr->c.strings_result(rc, res.getchildren(), cptr->data);
      }
      break;
    case COMPLETION_STRINGLIST_STAT:
      LOG_DEBUG(boost::format("Calling COMPLETION_STRINGLIST_STAT for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      if (failed) {
        std::vector<std::string> children;
        data::Stat stat;
        cptr->c.strings_stat_result(rc, children, stat, cptr->data);
      } else {
        proto::GetChildren2Response res;
        res.deserialize(iarchive, "reply");
        cptr->c.strings_stat_result(rc, res.getchildren(), res.getstat(), cptr->data);
      }
      break;
    case COMPLETION_STRING:
      LOG_DEBUG(boost::format("Calling COMPLETION_STRING for xid=%#08x failed=%d, rc=%d") %
          cptr->xid % failed % rc);
      if (failed) {
        cptr->c.string_result(rc, "", cptr->data);
      } else {
        proto::CreateResponse res;
        res.deserialize(iarchive, "reply");

        //ZOOKEEPER-1027
        cptr->c.string_result(rc, stripChroot(res.getpath(), chroot), cptr->data);
      }
      break;
    case COMPLETION_ACLLIST:
      LOG_DEBUG(boost::format("Calling COMPLETION_ACLLIST for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      if (failed) {
        std::vector<data::ACL> acl;
        data::Stat stat;
        cptr->c.acl_result(rc, acl, stat, cptr->data);
      } else {
        proto::GetACLResponse res;
        res.deserialize(iarchive, "reply");
        cptr->c.acl_result(rc, res.getacl(), res.getstat(), cptr->data);
      }
      break;
    case COMPLETION_VOID:
      LOG_DEBUG(boost::format("Calling COMPLETION_VOID for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      if (xid != PING_XID && cptr->c.void_result) {
        cptr->c.void_result(rc, cptr->data);
      }
      break;
    case COMPLETION_MULTI: {
      assert(cptr);
      LOG_DEBUG(boost::format("Calling COMPLETION_MULTI for xid=%#08x failed=%d rc=%d") %
          cptr->xid % failed % rc);
      boost::ptr_vector<OpResult> results;
      rc = deserialize_multi(xid, cptr, iarchive, results);
      if (cptr->c.multi_result) {
        cptr->c.multi_result(rc, results, cptr->data);
      }
      break;
    }
    default:
      LOG_DEBUG("Unsupported completion type=" << cptr->c.type);
  }
}


/* handles async completion (both single- and multithreaded) */
void process_completions(zhandle_t *zh) {
  completion_list_t *cptr;
  while ((cptr = dequeue_completion(&zh->completions_to_process)) != 0) {
    buffer_t *bptr = cptr->buffer;
    MemoryInStream stream(bptr->buffer, bptr->len);
    hadoop::IBinArchive iarchive(stream);
    proto::ReplyHeader header;
    header.deserialize(iarchive, "header");

    if (header.getxid() == WATCHER_EVENT_XID) {
      /* We are doing a notification, so there is no pending request */
      int type, state;
      proto::WatcherEvent event;
      event.deserialize(iarchive, "event");
      type = event.gettype();
      state = event.getstate();
      LOG_DEBUG(boost::format("Calling a watcher for node [%s], type = %d event=%s") %
          event.getpath() % cptr->c.type % watcherEvent2String(type));
      deliverWatchers(zh,type,state,event.getpath().c_str(), &cptr->c.watcher_result);
    } else {
      deserialize_response(cptr->c.type, header.getxid(), header.geterr() != 0,
                           header.geterr(), cptr, iarchive, zh->chroot);
    }
    destroy_completion_entry(cptr);
  }
}

int
zookeeper_process(zhandle_t *zh, int events) {
  buffer_t *bptr;
  int rc;

  if (zh==NULL)
    return ZBADARGUMENTS;
  if (is_unrecoverable(zh))
    return ZINVALIDSTATE;
  api_prolog(zh);
  rc = check_events(zh, events);
  if (rc!=ZOK)
    return api_epilog(zh, rc);
  while (rc >= 0 && (bptr=dequeue_buffer(&zh->to_process))) {
    MemoryInStream stream(bptr->buffer, bptr->curr_offset);
    hadoop::IBinArchive iarchive(stream);
    proto::ReplyHeader header;
    header.deserialize(iarchive, "header");

    if (header.getzxid() > 0) {
      zh->last_zxid = header.getzxid();
    }

    if (header.getxid() == WATCHER_EVENT_XID) {
      LOG_DEBUG("Processing WATCHER_EVENT");
      proto::WatcherEvent event;
      event.deserialize(iarchive, "event");
      completion_list_t* c =
        create_completion_entry(WATCHER_EVENT_XID,-1,0,0,0,0, false);
      c->buffer = bptr;
      c->c.watcher_result = collectWatchers(zh, event.gettype(),
                                            event.getpath());
      queue_completion(&zh->completions_to_process, c);
    } else if (header.getxid() == SET_WATCHES_XID) {
      LOG_DEBUG("Processing SET_WATCHES");
      delete bptr;
    } else if (header.getxid() == AUTH_XID) {
      LOG_DEBUG("Processing AUTH_XID");
      // special handling for the AUTH response as it may come back out-of-band
      auth_completion_func(header.geterr(), zh);
      delete bptr;
      // auth completion may change the connection state to unrecoverable
      if(is_unrecoverable(zh)){
        handle_error(zh, ZAUTHFAILED);
        return api_epilog(zh, ZAUTHFAILED);
      }
    } else {
      /* Find the request corresponding to the response */
      completion_list_t *cptr = dequeue_completion(&zh->sent_requests);

      /* [ZOOKEEPER-804] Don't assert if zookeeper_close has been called. */
      if (zh->close_requested == 1 && !cptr) {
        return api_epilog(zh,ZINVALIDSTATE);
      }
      assert(cptr);
      /* The requests are going to come back in order */
      if (cptr->xid != header.getxid()) {
        // received unexpected (or out-of-order) response
        LOG_DEBUG("Processing unexpected or out-of-order response!");
        delete bptr;
        // put the completion back on the queue (so it gets properly
        // signaled and deallocated) and disconnect from the server
        // TODO destroy completion
        queue_completion(&zh->sent_requests,cptr);
        return handle_socket_error_msg(zh, __LINE__,ZRUNTIMEINCONSISTENCY,
            "");
      }

      activateWatcher(zh, cptr->watcher, rc);
      if (header.getxid() == PING_XID) {
        int elapsed = 0;
        struct timeval now;
        gettimeofday(&now, 0);
        elapsed = calculate_interval(&zh->last_ping, &now);
        LOG_DEBUG("Got ping response in " << elapsed << "ms");
        delete bptr;
        destroy_completion_entry(cptr);
      } else {
        if (cptr->c.isSynchronous) {
          LOG_DEBUG(boost::format("Processing synchronous request "
                "from the IO thread: xid=%#08x") % header.getxid());
          deserialize_response(cptr->c.type, header.getxid(), header.geterr() != 0,
                               header.geterr(), cptr, iarchive, zh->chroot);
          delete bptr;
          destroy_completion_entry(cptr);
        } else {
          cptr->buffer = bptr;
          queue_completion(&zh->completions_to_process, cptr);
        }
      }
    }
  }
  return api_epilog(zh,ZOK);
}

int zoo_state(zhandle_t *zh)
{
    if(zh!=0)
        return zh->state;
    return 0;
}

static watcher_registration_t* create_watcher_registration(
    const std::string& path, result_checker_fn checker,
    watcher_fn watcher, void* ctx) {
  watcher_registration_t* wo;
  if(watcher==0)
    return 0;
  wo= new watcher_registration_t();
  wo->path = path;
  wo->watcher=watcher;
  wo->context=ctx;
  wo->checker=checker;
  return wo;
}

static void destroy_watcher_registration(watcher_registration_t* wo){
    if(wo!=0){
        free(wo);
    }
}

static completion_list_t* create_completion_entry(int xid, int completion_type,
    const void *dc, const void *data,watcher_registration_t* wo,
    boost::ptr_vector<OpResult>* results, bool isSynchronous) {
  completion_list_t *c = new completion_list_t();
  c->c.type = completion_type;
  c->data = data;
  switch(c->c.type) {
    case COMPLETION_VOID:
      c->c.void_result = (void_completion_t)dc;
      break;
    case COMPLETION_STRING:
      c->c.string_result = (string_completion_t)dc;
      break;
    case COMPLETION_DATA:
      c->c.data_result = (data_completion_t)dc;
      break;
    case COMPLETION_STAT:
      c->c.stat_result = (stat_completion_t)dc;
      break;
    case COMPLETION_STRINGLIST:
      c->c.strings_result = (strings_completion_t)dc;
      break;
    case COMPLETION_STRINGLIST_STAT:
      c->c.strings_stat_result = (strings_stat_completion_t)dc;
      break;
    case COMPLETION_ACLLIST:
      c->c.acl_result = (acl_completion_t)dc;
      break;
    case COMPLETION_MULTI:
      assert(results);
      c->c.multi_result = (multi_completion_t)dc;
      c->c.results.reset(results);
      break;
  }
  c->xid = xid;
  c->watcher = wo;
  c->c.isSynchronous = isSynchronous;
  return c;
}

static void destroy_completion_entry(completion_list_t* c) {
  if(c != NULL) {
    destroy_watcher_registration(c->watcher);
    if(c->buffer != NULL)
      delete c->buffer;
    delete c;
  }
}

static void queue_completion_nolock(completion_head_t *list, 
                                    completion_list_t *c)
{
    c->next = 0;
    /* appending a new entry to the back of the list */
    if (list->last) {
        assert(list->head);
        // List is not empty
        list->last->next = c;
        list->last = c;
    } else {
        // List is empty
        assert(!list->head);
        list->head = c;
        list->last = c;
    }
}

static void queue_completion(completion_head_t *list, completion_list_t *c)
{

    boost::lock_guard<boost::mutex> lock(*(list->lock));
    queue_completion_nolock(list, c);
    list->cond->notify_all();
}

static int add_completion(zhandle_t *zh, int xid, int completion_type,
        const void *dc, const void *data,
        watcher_registration_t* wo, boost::ptr_vector<OpResult>* results,
        bool isSynchronous)
{
    completion_list_t *c =create_completion_entry(xid, completion_type, dc,
            data, wo, results, isSynchronous);
    int rc = 0;
    if (!c)
        return ZSYSTEMERROR;
    {
        boost::lock_guard<boost::mutex> lock(*(zh->sent_requests.lock));
        if (zh->close_requested != 1) {
            queue_completion_nolock(&zh->sent_requests, c);
            rc = ZOK;
        } else {
            free(c);
            rc = ZINVALIDSTATE;
        }
        (*(zh->sent_requests.cond)).notify_all();
    }
    return rc;
}

static int add_data_completion(zhandle_t *zh, int xid, data_completion_t dc,
        const void *data,watcher_registration_t* wo, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_DATA, (const void*)dc, data, wo, 0, isSynchronous);
}

static int add_stat_completion(zhandle_t *zh, int xid, stat_completion_t dc,
        const void *data,watcher_registration_t* wo, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_STAT, (const void*)dc, data, wo, 0, isSynchronous);
}

static int add_strings_stat_completion(zhandle_t *zh, int xid,
        strings_stat_completion_t dc, const void *data,watcher_registration_t* wo, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_STRINGLIST_STAT, (const void*)dc, data, wo, 0, isSynchronous);
}

static int add_acl_completion(zhandle_t *zh, int xid, acl_completion_t dc,
        const void *data, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_ACLLIST, (const void*)dc, data, 0, 0, isSynchronous);
}

static int add_void_completion(zhandle_t *zh, int xid, void_completion_t dc,
        const void *data, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_VOID, (const void*)dc, data, 0, 0, isSynchronous);
}

static int add_string_completion(zhandle_t *zh, int xid,
        string_completion_t dc, const void *data, bool isSynchronous)
{
    return add_completion(zh, xid, COMPLETION_STRING, (const void*)dc, data, 0, 0, isSynchronous);
}

static int add_multi_completion(zhandle_t *zh, int xid, multi_completion_t dc,
        const void *data, boost::ptr_vector<OpResult>* results, bool isSynchronous) {
    return add_completion(zh, xid, COMPLETION_MULTI, (const void*)dc, data, 0, results, isSynchronous);
}

int
zookeeper_close(zhandle_t *zh) {
  LOG_DEBUG("Entering zookeeper_close()");
  int rc = ZOK;
  if (zh == 0)
    return ZBADARGUMENTS;

  zh->close_requested=1;
  if (inc_ref_counter(zh)>1) {
    /* We have incremented the ref counter to prevent the
     * completions from calling zookeeper_close before we have
     * completed the adaptor_finish call below. */

    /* Signal any syncronous completions before joining the threads */
    enter_critical(zh);
    free_completions(zh, ZCLOSING);
    leave_critical(zh);

    adaptor_finish(zh);
    /* Now we can allow the handle to be cleaned up, if the completion
     * threads finished during the adaptor_finish call. */
    api_epilog(zh, 0);
    return ZOK;
  }
  /* No need to decrement the counter since we're just going to
   * destroy the handle later. */
  if(zh->state==ZOO_CONNECTED_STATE){
    int rc = 0;
    std::string serialized;
    StringOutStream stream(serialized);
    hadoop::OBinArchive oarchive(stream);

    proto::RequestHeader header;
    header.setxid(get_xid());
    header.settype(ZOO_CLOSE_OP);
    header.serialize(oarchive, "header");
    LOG_INFO(boost::format("Closing zookeeper sessionId=%#llx to [%s]\n") %
        zh->client_id.client_id % format_current_endpoint_info(zh));

    /* add this buffer to the head of the send queue */
    // TODO(michim) avoid copy
    char* data  = new char[serialized.size()];
    memmove(data, serialized.c_str(), serialized.size());

    enter_critical(zh);
    rc = queue_buffer_bytes(&zh->to_send, data, serialized.size());
    leave_critical(zh);

    if (rc < 0) {
      rc = ZMARSHALLINGERROR;
      goto finish;
    }
    /* make sure the close request is sent; we set timeout to an arbitrary
     * (but reasonable) number of milliseconds since we want the call to block*/
    rc = adaptor_send_queue(zh, 3000);
  } else {
    LOG_INFO(boost::format("Freeing zookeeper resources for sessionId=%#llx") %
        zh->client_id.client_id);
    rc = ZOK;
  }
finish:
  destroy(zh);
  adaptor_destroy(zh);
  delete zh;
  return rc;
}

static int isValidPath(const char* path, const int flags) {
    int len = 0;
    char lastc = '/';
    char c;
    int i = 0;

  if (path == 0)
    return 0;
  len = strlen(path);
  if (len == 0)
    return 0;
  if (path[0] != '/')
    return 0;
  if (len == 1) // done checking - it's the root
    return 1;
  if (path[len - 1] == '/' && !(flags & ZOO_SEQUENCE))
    return 0;

  i = 1;
  for (; i < len; lastc = path[i], i++) {
    c = path[i];

    if (c == 0) {
      return 0;
    } else if (c == '/' && lastc == '/') {
      return 0;
    } else if (c == '.' && lastc == '.') {
      if (path[i-2] == '/' && (((i + 1 == len) && !(flags & ZOO_SEQUENCE))
                               || path[i+1] == '/')) {
        return 0;
      }
    } else if (c == '.') {
      if ((path[i-1] == '/') && (((i + 1 == len) && !(flags & ZOO_SEQUENCE))
                                 || path[i+1] == '/')) {
        return 0;
      }
    } else if (c > 0x00 && c < 0x1f) {
      return 0;
    }
  }

  return 1;
}

/*---------------------------------------------------------------------------*
 * REQUEST INIT HELPERS
 *---------------------------------------------------------------------------*/
/* Common Request init helper functions to reduce code duplication */
static int Request_path_init(zhandle_t *zh, int flags, 
        char **path_out, const char *path)
{
    assert(path_out);
    
    *path_out = prepend_string(zh, path);
    if (zh == NULL || !isValidPath(*path_out, flags)) {
        free_duplicate_path(*path_out, path);
        return ZBADARGUMENTS;
    }
    if (is_unrecoverable(zh)) {
        free_duplicate_path(*path_out, path);
        return ZINVALIDSTATE;
    }

    return ZOK;
}

/**
 * TODO:michim Avoid extra copies.
 */
static int getRealString(zhandle_t *zh, int flags, const char* path,
                         std::string& pathStr) {
  char* tempPath = NULL;;
  int rc = Request_path_init(zh, flags, &tempPath, path);
  if (rc != ZOK) {
    return rc;
  }
  pathStr.assign(tempPath, strlen(tempPath));
  if (path != tempPath) {
    free(tempPath);
  }
  return rc;
}

/*---------------------------------------------------------------------------*
 * ASYNC API
 *---------------------------------------------------------------------------*/
int zoo_awget(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        data_completion_t dc, const void *data, bool isSynchronous)
{
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_GETDATA_OP);
  header.serialize(oarchive, "header");

  proto::GetDataRequest req;
  req.getpath() = pathStr;
  req.setwatch(watcher != NULL);
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_data_completion(zh, header.getxid(), dc, data,
      create_watcher_registration(pathStr,data_result_checker,watcher,watcherCtx),
      isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer,
    serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aset(zhandle_t *zh, const char *path, const char *buf, int buflen,
        int version, stat_completion_t dc, const void *data, bool isSynchronous)
{
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_SETDATA_OP);
  header.serialize(oarchive, "header");

  proto::SetDataRequest req;
  req.getpath() = pathStr;
  if (buf != NULL && buflen >= 0) {
    req.getdata() = std::string(buf, buflen);
  } else {
    req.getdata() = "";
  }
  req.setversion(version);
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_stat_completion(zh, header.getxid(), dc, data,0,
                                         isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer,
    serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending set request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_acreate(zhandle_t *zh, const char *path, const char *value,
        int valuelen, const std::vector<org::apache::zookeeper::data::ACL>& acl,
        int flags, string_completion_t completion, const void *data,
        bool isSynchronous) {
  LOG_DEBUG("Entering zoo_acreate()");
  std::string pathStr;
  int rc = getRealString(zh, flags, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_CREATE_OP);
  header.serialize(oarchive, "header");

  proto::CreateRequest req;
  req.getpath() = pathStr;
  if (value != NULL && valuelen >= 0) {
    req.getdata() = std::string(value, valuelen);
  } else {
    req.getdata() = "";
  }
  req.getacl() = acl;
  req.setflags(flags);
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_string_completion(zh, header.getxid(), completion,
                                           data, isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer,
                                        serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_adelete(zhandle_t *zh, const char *path, int version,
        void_completion_t completion, const void *data, bool isSynchronous) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_DELETE_OP);
  header.serialize(oarchive, "header");

  proto::DeleteRequest req;
  req.getpath() = pathStr;
  req.setversion(version);
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_void_completion(zh, header.getxid(), completion, data,
                                         isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_awexists(zhandle_t *zh, const char *path, watcher_fn watcher,
                 void* watcherCtx, stat_completion_t completion,
                 const void *data, bool isSynchronous) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_EXISTS_OP);
  header.serialize(oarchive, "header");

  proto::ExistsRequest req;
  req.getpath() = pathStr;
  req.setwatch(watcher != NULL);
  req.serialize(oarchive, "req");

  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_stat_completion(zh, header.getxid(), completion, data,
      create_watcher_registration(req.getpath(), exists_result_checker,
        watcher,watcherCtx), isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

static int zoo_awget_children2_(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_stat_completion_t ssc, const void *data, bool isSynchronous) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_GETCHILDREN2_OP);
  header.serialize(oarchive, "header");

  proto::GetChildren2Request req;
  req.getpath() = pathStr;
  req.setwatch(watcher != NULL);
  req.serialize(oarchive, "req");

  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_strings_stat_completion(zh, header.getxid(), ssc, data,
      create_watcher_registration(req.getpath(),child_result_checker,watcher,watcherCtx), false);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_awget_children2(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_stat_completion_t dc,
         const void *data, bool isSynchronous)
{
    return zoo_awget_children2_(zh,path,watcher,watcherCtx,dc,data,
                                isSynchronous);
}

int zoo_async(zhandle_t *zh, const char *path,
              string_completion_t completion, const void *data) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_SYNC_OP);
  header.serialize(oarchive, "header");

  proto::SyncRequest req;
  req.getpath() = pathStr;
  req.serialize(oarchive, "req");

  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_string_completion(zh, header.getxid(), completion, data, false) ;
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;

}

int zoo_aget_acl(zhandle_t *zh, const char *path, acl_completion_t completion,
        const void *data, bool isSynchronous) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_GETACL_OP);
  header.serialize(oarchive, "header");

  proto::GetACLRequest req;
  req.getpath() = pathStr;
  req.serialize(oarchive, "req");

  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_acl_completion(zh, header.getxid(), completion, data,
                                        isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aset_acl(zhandle_t *zh, const char *path, int version,
        const std::vector<data::ACL>& acl, void_completion_t completion,
        const void *data, bool isSynchronous) {
  std::string pathStr;
  int rc = getRealString(zh, 0, path, pathStr);
  if (rc != ZOK) {
    return rc;
  }

  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_SETACL_OP);
  header.serialize(oarchive, "header");

  proto::SetACLRequest req;
  req.getpath() = pathStr;
  req.setversion(version);
  req.getacl() = acl;
  req.serialize(oarchive, "req");

  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  enter_critical(zh);
  rc = rc < 0 ? rc : add_void_completion(zh, header.getxid(), completion, data,
                                         isSynchronous);
  rc = rc < 0 ? rc : queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
      header.getxid() % path % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_amulti2(zhandle_t *zh,
    const boost::ptr_vector<org::apache::zookeeper::Op>& ops,
    multi_completion_t completion, const void *data, bool isSynchronous) {
  std::string serialized;
  StringOutStream stream(serialized);
  hadoop::OBinArchive oarchive(stream);

  proto::RequestHeader header;
  header.setxid(get_xid());
  header.settype(ZOO_MULTI_OP);
  header.serialize(oarchive, "header");
  boost::ptr_vector<OpResult>* results = new boost::ptr_vector<OpResult>();

  size_t index = 0;
  for (index = 0; index < ops.size(); index++) {
    std::string pathStr;
    proto::MultiHeader mheader;
    mheader.settype(ops[index].getType());
    mheader.setdone(0);
    mheader.seterr(-1);
    mheader.serialize(oarchive, "req");

    int rc = getRealString(zh, 0, ops[index].getPath().c_str(), pathStr);
    if (rc != ZOK) {
      return rc;
    }

    switch(ops[index].getType()) {
      case ZOO_CREATE_OP: {
        const Op::Create* createOp = dynamic_cast<const Op::Create*>(&(ops[index]));
        assert(createOp != NULL);
        proto::CreateRequest createReq;
        createReq.getpath() = pathStr;
        createReq.getdata() = createOp->getData();
        createReq.getacl() = createOp->getAcl();
        createReq.setflags(createOp->getMode());
        createReq.serialize(oarchive, "req");
        results->push_back(new OpResult::Create(ReturnCode::Ok, ""));
        break;
      }
      case ZOO_DELETE_OP: {
        const Op::Remove* removeOp = dynamic_cast<const Op::Remove*>(&(ops[index]));
        assert(removeOp != NULL);
        proto::DeleteRequest removeReq;
        removeReq.getpath() = pathStr;
        removeReq.setversion(removeOp->getVersion());
        removeReq.serialize(oarchive, "req");
        results->push_back(new OpResult::Remove(ReturnCode::Ok));
        break;
      }

      case ZOO_SETDATA_OP: {
        const Op::SetData* setDataOp = dynamic_cast<const Op::SetData*>(&(ops[index]));
        assert(setDataOp != NULL);
        proto::SetDataRequest setDataReq;
        setDataReq.getpath() = pathStr;
        setDataReq.getdata() = setDataOp->getData();
        setDataReq.setversion(setDataOp->getVersion());
        setDataReq.serialize(oarchive, "req");
        results->push_back(new OpResult::SetData());
        break;
     }

      case ZOO_CHECK_OP: {
        const Op::Check* checkOp = dynamic_cast<const Op::Check*>(&(ops[index]));
        assert(checkOp != NULL);
        proto::CheckVersionRequest checkReq;
        checkReq.getpath() = pathStr;
        checkReq.setversion(checkOp->getVersion());
        checkReq.serialize(oarchive, "req");
        results->push_back(new OpResult::Check());
        break;
      }
      default:
        LOG_ERROR("Unimplemented sub-op type=" << ops[index].getType()
                                               << " in multi-op.");
        return ZUNIMPLEMENTED;
    }
  }

  // End of multi request.
  proto::MultiHeader mheader;
  mheader.settype(-1);
  mheader.setdone(1);
  mheader.seterr(-1);
  mheader.serialize(oarchive, "req");
  // TODO(michim) avoid copy
  char* buffer = new char[serialized.size()];
  memmove(buffer, serialized.c_str(), serialized.size());

  /* BEGIN: CRTICIAL SECTION */
  enter_critical(zh);
  add_multi_completion(zh, header.getxid(), completion, data, results, isSynchronous);
  queue_buffer_bytes(&zh->to_send, buffer, serialized.size());
  leave_critical(zh);

  LOG_DEBUG(boost::format("Sending multi request xid=%#08x with %d subrequests to %s") %
      header.getxid() % index % format_current_endpoint_info(zh));
  /* make a best (non-blocking) effort to send the requests asap */
  adaptor_send_queue(zh, 0);
  return ZOK;
}

/* specify timeout of 0 to make the function non-blocking */
/* timeout is in milliseconds */
int flush_send_queue(zhandle_t*zh, int timeout)
{
    int rc= ZOK;
    struct timeval started;
    gettimeofday(&started,0);
    // we can't use dequeue_buffer() here because if (non-blocking) send_buffer()
    // returns EWOULDBLOCK we'd have to put the buffer back on the queue.
    // we use a recursive lock instead and only dequeue the buffer if a send was
    // successful
    {
        boost::lock_guard<boost::recursive_mutex>
          lock(zh->to_send.mutex_);
        while (!(zh->to_send.bufferList_.empty()) &&
               zh->state == ZOO_CONNECTED_STATE) {
            if(timeout!=0){
                int elapsed;
                struct timeval now;
                gettimeofday(&now,0);
                elapsed=calculate_interval(&started,&now);
                if (elapsed>timeout) {
                    rc = ZOPERATIONTIMEOUT;
                    break;
                }

                struct pollfd fds;
                fds.fd = zh->fd;
                fds.events = POLLOUT;
                fds.revents = 0;
                rc = poll(&fds, 1, timeout-elapsed);
                if (rc<=0) {
                    /* timed out or an error or POLLERR */
                    rc = rc==0 ? ZOPERATIONTIMEOUT : ZSYSTEMERROR;
                    break;
                }
            }

            rc = send_buffer(zh->fd, &(zh->to_send.bufferList_.front()));
            if(rc==0 && timeout==0){
                /* send_buffer would block while sending this buffer */
                rc = ZOK;
                break;
            }
            if (rc < 0) {
                rc = ZCONNECTIONLOSS;
                break;
            }
            // if the buffer has been sent successfully, remove it from the queue
            if (rc > 0)
                remove_buffer(&zh->to_send);
            gettimeofday(&zh->last_send, 0);
            rc = ZOK;
        }
    }
    return rc;
}

// TODO(michim) handle synchronous
int zoo_add_auth(zhandle_t *zh,const char* scheme,const char* cert,
        int certLen,void_completion_t completion, const void *data,
        bool isSynchronous)
{
    auth_info *authinfo;
    if(scheme==NULL || zh==NULL)
        return ZBADARGUMENTS;

    if (is_unrecoverable(zh))
        return ZINVALIDSTATE;

    // [ZOOKEEPER-800] zoo_add_auth should return ZINVALIDSTATE if
    // the connection is closed. 
    if (zoo_state(zh) == 0) {
        return ZINVALIDSTATE;
    }

    assert(cert != NULL);
    assert(certLen >= 0);
    {
        boost::lock_guard<boost::mutex> lock(zh->auth_h.mutex_);
        authinfo = new auth_info();
        authinfo->scheme = scheme;
        authinfo->auth = std::string(cert, certLen);
        authinfo->completion=completion;
        authinfo->data=(const char*)data;
        zh->auth_h.authList_.push_back(authinfo);
    }

    if(zh->state == ZOO_CONNECTED_STATE || zh->state == ZOO_ASSOCIATING_STATE)
        return send_last_auth_info(zh);

    return ZOK;
}

static const char* format_endpoint_info(const struct sockaddr_storage* ep)
{
    static char buf[128];
    char addrstr[128];
    void *inaddr;
    int port;
    if(ep==0)
        return "null";

#if defined(AF_INET6)
    if(ep->ss_family==AF_INET6){
        inaddr=&((struct sockaddr_in6*)ep)->sin6_addr;
        port=((struct sockaddr_in6*)ep)->sin6_port;
    } else {
#endif
        inaddr=&((struct sockaddr_in*)ep)->sin_addr;
        port=((struct sockaddr_in*)ep)->sin_port;
#if defined(AF_INET6)
    }
#endif
    inet_ntop(ep->ss_family,inaddr,addrstr,sizeof(addrstr)-1);
    sprintf(buf,"%s:%d",addrstr,ntohs(port));
    return buf;
}

static const char* format_current_endpoint_info(zhandle_t* zh)
{
    return format_endpoint_info(&zh->addrs[zh->connect_index]);
}

void zoo_deterministic_conn_order(int yesOrNo)
{
    disable_conn_permute=yesOrNo;
}
