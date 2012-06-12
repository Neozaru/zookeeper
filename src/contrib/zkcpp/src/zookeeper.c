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

#include <boost/thread/locks.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <boost/random/mersenne_twister.hpp> // mt19937
#include <boost/random/normal_distribution.hpp>
#include <string>
#include <zookeeper.h>
#include <zookeeper.jute.h>
#include <memory_in_stream.hh>
#include <string_out_stream.hh>
#include <zookeeper.jute.hh>
#include <recordio.hh>
#include <binarchive.hh>
#include <proto.h>
#include "zk_adaptor.h"
#include "logging.hh"
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

const int ZOO_PERM_READ = 1 << 0;
const int ZOO_PERM_WRITE = 1 << 1;
const int ZOO_PERM_CREATE = 1 << 2;
const int ZOO_PERM_DELETE = 1 << 3;
const int ZOO_PERM_ADMIN = 1 << 4;
const int ZOO_PERM_ALL = 0x1f;
struct Id ZOO_ANYONE_ID_UNSAFE = {(char*)"world", (char*)"anyone"};
struct Id ZOO_AUTH_IDS = {(char*)"auth", (char*)""};
static struct ACL _OPEN_ACL_UNSAFE_ACL[] = {{0x1f, {(char*)"world", (char*)"anyone"}}};
static struct ACL _READ_ACL_UNSAFE_ACL[] = {{0x01, {(char*)"world", (char*)"anyone"}}};
static struct ACL _CREATOR_ALL_ACL_ACL[] = {{0x1f, {(char*)"auth", (char*)""}}};
struct ACL_vector ZOO_OPEN_ACL_UNSAFE = { 1, _OPEN_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_READ_ACL_UNSAFE = { 1, _READ_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_CREATOR_ALL_ACL = { 1, _CREATOR_ALL_ACL_ACL};

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
    };
    completion_head_t clist; /* For multi-op */
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
static void deserialize_response(int type, int xid, int failed, int rc, completion_list_t *cptr, struct iarchive *ia);
static int deserialize_multi(int xid, completion_list_t *cptr, struct iarchive *ia);

/* completion routine forward declarations */
static int add_completion(zhandle_t *zh, int xid, int completion_type,
        const void *dc, const void *data,
        watcher_registration_t* wo, completion_head_t *clist);
static completion_list_t* create_completion_entry(int xid, int completion_type,
        const void *dc, const void *data, watcher_registration_t* wo, 
        completion_head_t *clist);
static void destroy_completion_entry(completion_list_t* c);
static void queue_completion_nolock(completion_head_t *list, completion_list_t *c);
static void queue_completion(completion_head_t *list, completion_list_t *c);
static int handle_socket_error_msg(zhandle_t *zh, int line, int rc,
                                  const std::string& message);
static void cleanup_bufs(zhandle_t *zh,int callCompletion,int rc);

static int disable_conn_permute=0; // permute enabled by default

static void *SYNCHRONOUS_MARKER = (void*)&SYNCHRONOUS_MARKER;
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
        return zh->active_node_watchers;
    } else if (rc == ZNONODE) {
        return zh->active_exist_watchers;
    }
    return 0;
}

zk_hashtable *data_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? zh->active_node_watchers : 0;
}

zk_hashtable *child_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? zh->active_child_watchers : 0;
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
    cleanup_bufs(zh,1,ZCLOSING);
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

    if (zh->chroot != 0) {
        free(zh->chroot);
        zh->chroot = NULL;
    }

    zh->auth_h.get()->authList_.clear();
    destroy_zk_hashtable(zh->active_node_watchers);
    destroy_zk_hashtable(zh->active_exist_watchers);
    destroy_zk_hashtable(zh->active_child_watchers);
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

    zh = (zhandle_t*)calloc(1, sizeof(*zh));
    if (!zh) {
        return 0;
    }
    zh->connectResponse.reset(new proto::ConnectResponse());
    zh->to_process.reset(new buffer_list_t());
    zh->to_send.reset(new buffer_list_t());
    zh->sent_requests.lock.reset(new boost::mutex());
    zh->sent_requests.cond.reset(new boost::condition_variable());
    zh->completions_to_process.lock.reset(new boost::mutex());
    zh->completions_to_process.cond.reset(new boost::condition_variable());

    zh->ref_counter = 0;
    zh->fd = -1;
    zh->state = NOTCONNECTED_STATE_DEF;
    zh->context = context;
    zh->recv_timeout = recv_timeout;
    zh->auth_h.reset(new auth_list_head_t());
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
        zh->chroot = strdup(index_chroot);
        if (zh->chroot == NULL) {
            goto abort;
        }
        // if chroot is just / set it to null
        if (strlen(zh->chroot) == 1) {
            free(zh->chroot);
            zh->chroot = NULL;
        }
        // cannot use strndup so allocate and strcpy
        zh->hostname = (char *) malloc(index_chroot - host + 1);
        zh->hostname = strncpy(zh->hostname, host, (index_chroot - host));
        //strncpy does not null terminate
        *(zh->hostname + (index_chroot - host)) = '\0';

    } else {
        zh->chroot = NULL;
        zh->hostname = strdup(host);
    }
    if (zh->chroot && !isValidPath(zh->chroot, 0)) {
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
    // TODO(michim) use vector<char> as buffer. don't hardcode buffer size.
    zh->primer_buffer.buffer = (char*)malloc(40);
    zh->primer_buffer.curr_offset = 0;
    zh->primer_buffer.len = 40;
    zh->last_zxid = 0;
    zh->next_deadline.tv_sec=zh->next_deadline.tv_usec=0;
    zh->socket_readable.tv_sec=zh->socket_readable.tv_usec=0;
    zh->active_node_watchers=create_zk_hashtable();
    zh->active_exist_watchers=create_zk_hashtable();
    zh->active_child_watchers=create_zk_hashtable();

    if (adaptor_init(zh) == -1) {
        goto abort;
    }

    return zh;
abort:
    errnosave=errno;
    destroy(zh);
    free(zh);
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
    if (zh == NULL || zh->chroot == NULL)
        return (char *) client_path;
    // handle the chroot itself, client_path = "/"
    if (strlen(client_path) == 1) {
        return strdup(zh->chroot);
    }
    ret_str = (char *) malloc(strlen(zh->chroot) + strlen(client_path) + 1);
    strcpy(ret_str, zh->chroot);
    return strcat(ret_str, client_path);
}

/**
   strip off the chroot string from the server path
   if there is one else return the exact path
 */
char* sub_string(zhandle_t *zh, const char* server_path) {
    char *ret_str;
    if (zh->chroot == NULL)
        return (char *) server_path;
    //ZOOKEEPER-1027
    if (strncmp(server_path, zh->chroot, strlen(zh->chroot)) != 0) {
        LOG_ERROR(
          boost::format("server path %s does not include chroot path %s") %
                        server_path % zh->chroot);
        return (char *) server_path;
    }
    if (strlen(server_path) == strlen(zh->chroot)) {
        //return "/"
        ret_str = strdup("/");
        return ret_str;
    }
    ret_str = strdup(server_path + strlen(zh->chroot));
    return ret_str;
}

static buffer_t *allocate_buffer(char *buff, int len)
{
    buffer_t* buffer = new buffer_t();
    buffer->len = len==0?sizeof(*buffer):len;
    buffer->curr_offset = 0;
    buffer->buffer = buff;
    return buffer;
}

static void free_buffer(buffer_t *b)
{
    if (!b) {
        return;
    }
    if (b->buffer) {
        free(b->buffer);
    }
    delete b;
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
    free_buffer(b);
    return 1;
}

static int queue_buffer_bytes(buffer_list_t *list, char *buff, int len)
{
    buffer_t *b  = allocate_buffer(buff,len);
    if (!b)
        return ZSYSTEMERROR;
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
            buff->buffer = (char*)calloc(1, buff->len);
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

void free_completions(zhandle_t *zh,int callCompletion,int reason)
{
    completion_head_t tmp_list;
    struct oarchive *oa;
    struct ReplyHeader h;

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
        if (cptr->c.data_result == SYNCHRONOUS_MARKER) {
            struct sync_completion
                        *sc = (struct sync_completion*)cptr->data;
            sc->rc = reason;
            notify_sync_completion(sc);
            zh->outstanding_sync--;
            destroy_completion_entry(cptr);
        } else if (callCompletion) {
            if(cptr->xid == PING_XID){
                // Nothing to do with a ping response
                destroy_completion_entry(cptr);
            } else {
                // Fake the response
                buffer_t *bptr;
                h.xid = cptr->xid;
                h.zxid = -1;
                h.err = reason;
                oa = create_buffer_oarchive();
                serialize_ReplyHeader(oa, "header", &h);
                bptr = (buffer_t*)calloc(sizeof(*bptr), 1);
                assert(bptr);
                bptr->len = get_buffer_len(oa);
                bptr->buffer = get_buffer(oa);
                close_buffer_oarchive(&oa, 0);
                cptr->buffer = bptr;
                queue_completion(&zh->completions_to_process, cptr);
            }
        }
    }
    {
        boost::lock_guard<boost::mutex> lock(zh->auth_h.get()->mutex_);
        BOOST_FOREACH(auth_info& info, zh->auth_h.get()->authList_) {
          if (info.completion) {
            info.completion(reason, info.data);
            info.data = NULL;
            info.completion = NULL;
          }
        }
    }
}

static void cleanup_bufs(zhandle_t *zh,int callCompletion,int rc)
{
    enter_critical(zh);
    free_buffers(zh->to_send.get());
    free_buffers(zh->to_process.get());
    free_completions(zh,callCompletion,rc);
    leave_critical(zh);
    if (zh->input_buffer && zh->input_buffer != &zh->primer_buffer) {
        free_buffer(zh->input_buffer);
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
    cleanup_bufs(zh,1,rc);
    zh->fd = -1;
    zh->connect_index++;
    if (!is_unrecoverable(zh)) {
        zh->state = 0;
    }
    if (process_async(zh->outstanding_sync)) {
        process_completions(zh);
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

  boost::lock_guard<boost::mutex> lock(zh->auth_h.get()->mutex_);
  if (rc != 0) {
    LOG_ERROR("Authentication scheme " << zh->auth_h.get()->authList_.front().scheme <<
        " failed. Connection closed.");
    zh->state=ZOO_AUTH_FAILED_STATE;
  }else{
    LOG_INFO("Authentication scheme " << zh->auth_h.get()->authList_.front().scheme <<
        " succeeded.");
    //change state for all auths
    BOOST_FOREACH(auth_info& info, zh->auth_h.get()->authList_) {
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
  req.getauth() = std::string(auth->auth.buff, auth->auth.len);
  req.serialize(oarchive, "req");

  /* add this buffer to the head of the send queue */
  // TODO(michim) avoid copy
  char* data  = (char*)malloc(serialized.size());
  memmove(data, serialized.c_str(), serialized.size());
  rc = queue_buffer_bytes(zh->to_send.get(),
        data, serialized.size());
  adaptor_send_queue(zh, 0);
  return rc;
}

/** send all auths, not just the last one **/
static int send_auth_info(zhandle_t *zh) {
    int rc = 0;
    {
        boost::lock_guard<boost::mutex> lock(zh->auth_h.get()->mutex_);
        BOOST_FOREACH(auth_info& info, zh->auth_h.get()->authList_) {
          rc = send_info_packet(zh, &info);
        }
    }
    LOG_DEBUG("Sending all auth info request to " << format_current_endpoint_info(zh));
    return (rc <0) ? ZMARSHALLINGERROR:ZOK;
}

static int send_last_auth_info(zhandle_t *zh) {
  int rc = 0;
  {
    boost::lock_guard<boost::mutex> lock(zh->auth_h.get()->mutex_);
    if (zh->auth_h.get()->authList_.empty()) {
      return ZOK; // there is nothing to send
    }
    rc = send_info_packet(zh, &(zh->auth_h.get()->authList_.back()));
  }
  LOG_DEBUG("Sending auth info request to " << format_current_endpoint_info(zh));
  return (rc < 0) ? ZMARSHALLINGERROR : ZOK;
}

static void free_key_list(char **list, int count)
{
    int i;

    for(i = 0; i < count; i++) {
        free(list[i]);
    }
    free(list);
}

static int send_set_watches(zhandle_t *zh)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER(xid , SET_WATCHES_XID), STRUCT_INITIALIZER(type , ZOO_SETWATCHES_OP)};
    struct SetWatches req;
    int rc;

    req.relativeZxid = zh->last_zxid;
    req.dataWatches.data = collect_keys(zh->active_node_watchers, (int*)&req.dataWatches.count);
    req.existWatches.data = collect_keys(zh->active_exist_watchers, (int*)&req.existWatches.count);
    req.childWatches.data = collect_keys(zh->active_child_watchers, (int*)&req.childWatches.count);

    // return if there are no pending watches
    if (!req.dataWatches.count && !req.existWatches.count &&
        !req.childWatches.count) {
        free_key_list(req.dataWatches.data, req.dataWatches.count);
        free_key_list(req.existWatches.data, req.existWatches.count);
        free_key_list(req.childWatches.data, req.childWatches.count);
        return ZOK;
    }


    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_SetWatches(oa, "req", &req);
    /* add this buffer to the head of the send queue */
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(),
        get_buffer(oa), get_buffer_len(oa));
    /* We queued the buffer, so don't free it */   
    close_buffer_oarchive(&oa, 0);
    free_key_list(req.dataWatches.data, req.dataWatches.count);
    free_key_list(req.existWatches.data, req.existWatches.count);
    free_key_list(req.childWatches.data, req.childWatches.count);
    LOG_DEBUG("Sending set watches request to " << format_current_endpoint_info(zh));
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
  zh->input_buffer = &zh->primer_buffer;
  /* This seems a bit weird to to set the offset to 4, but we already have a
   * length, so we skip reading the length (and allocating the buffer) by
   * saying that we are already at offset 4 */
  zh->input_buffer->curr_offset = 4;
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
     const void *data);
 static int add_string_completion(zhandle_t *zh, int xid,
     string_completion_t dc, const void *data);

 int send_ping(zhandle_t* zh)
 {
    int rc;
    struct oarchive *oa = create_buffer_oarchive();
    struct RequestHeader h = { STRUCT_INITIALIZER(xid ,PING_XID), STRUCT_INITIALIZER (type , ZOO_PING_OP) };

    rc = serialize_RequestHeader(oa, "header", &h);
    enter_critical(zh);
    gettimeofday(&zh->last_ping, 0);
    rc = rc < 0 ? rc : add_void_completion(zh, h.xid, 0, 0);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    close_buffer_oarchive(&oa, 0);
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
        if ((!zh->to_send.get()->bufferList_.empty() &&
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
    if (!(zh->to_send.get()->bufferList_.empty()) && (events&ZOOKEEPER_WRITE)) {
        /* make the flush call non-blocking by specifying a 0 timeout */
        int rc=flush_send_queue(zh,0);
        if (rc < 0)
            return handle_socket_error_msg(zh,__LINE__,ZCONNECTIONLOSS,
                "failed while flushing send queue");
    }
    if (events&ZOOKEEPER_READ) {
        int rc;
        if (zh->input_buffer == 0) {
            zh->input_buffer = allocate_buffer(0,0);
        }

        rc = recv_buffer(zh->fd, zh->input_buffer);
        if (rc < 0) {
            return handle_socket_error_msg(zh, __LINE__,ZCONNECTIONLOSS,
                "failed while receiving a server response");
        }
        if (rc > 0) {
            gettimeofday(&zh->last_recv, 0);
            if (zh->input_buffer != &zh->primer_buffer) {
                boost::lock_guard<boost::recursive_mutex> lock(zh->to_process.get()->mutex_);
                zh->to_process.get()->bufferList_.push_back(zh->input_buffer);
            } else  {
                int64_t oldid,newid;
                // Deserialize. Skipping the first 4 bytes (length field).
                MemoryInStream istream((zh->primer_buffer.buffer) + 4, zh->primer_buffer.len - 4);
                hadoop::IBinArchive iarchive(istream);
                zh->connectResponse->deserialize(iarchive,"connect");

                /* We are processing the primer_buffer, so we need to finish
                 * the connection handshake */
                oldid = zh->client_id.client_id;
                newid = zh->connectResponse.get()->getsessionId();
                if (oldid != 0 && oldid != newid) {
                    zh->state = ZOO_EXPIRED_SESSION_STATE;
                    errno = ESTALE;
                    return handle_socket_error_msg(zh,__LINE__,ZSESSIONEXPIRED,
                    "");
                } else {
                    zh->recv_timeout = zh->connectResponse.get()->gettimeOut();
                    zh->client_id.client_id = newid;

                    memcpy(zh->client_id.passwd, zh->connectResponse.get()->getpasswd().c_str(),
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
                    zh->input_buffer = 0; // just in case the watcher calls zookeeper_process() again
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
static int queue_session_event(zhandle_t *zh, int state)
{
    int rc;
    struct WatcherEvent evt = { ZOO_SESSION_EVENT, state, (char*)"" };
    struct ReplyHeader hdr = { WATCHER_EVENT_XID, 0, 0 };
    struct oarchive *oa;
    completion_list_t *cptr;

    if ((oa=create_buffer_oarchive())==NULL) {
        LOG_ERROR("out of memory");
        goto error;
    }
    rc = serialize_ReplyHeader(oa, "hdr", &hdr);
    rc = rc<0?rc: serialize_WatcherEvent(oa, "event", &evt);
    if(rc<0){
        close_buffer_oarchive(&oa, 1);
        goto error;
    }
    cptr = create_completion_entry(WATCHER_EVENT_XID,-1,0,0,0,0);
    cptr->buffer = allocate_buffer(get_buffer(oa), get_buffer_len(oa));
    cptr->buffer->curr_offset = get_buffer_len(oa);
    if (!cptr->buffer) {
        free(cptr);
        close_buffer_oarchive(&oa, 1);
        goto error;
    }
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);
    cptr->c.watcher_result = collectWatchers(zh, ZOO_SESSION_EVENT, (char*)"");
    queue_completion(&zh->completions_to_process, cptr);
    if (process_async(zh->outstanding_sync)) {
        process_completions(zh);
    }
    return ZOK;
error:
    errno=ENOMEM;
    return ZSYSTEMERROR;
}
//#endif

completion_list_t *dequeue_completion(completion_head_t *list)
{
    completion_list_t *cptr;
    {
        boost::lock_guard<boost::mutex> lock(*(list->lock));
        cptr = list->head;
        if (cptr) {
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

static void process_sync_completion(
        completion_list_t *cptr,
        struct sync_completion *sc,
        struct iarchive *ia,
	zhandle_t *zh)
{
    LOG_DEBUG(boost::format("Processing sync_completion with type=%d xid=%#x rc=%d") %
                           cptr->c.type % cptr->xid % sc->rc);

    switch(cptr->c.type) {
    case COMPLETION_DATA: 
        if (sc->rc==0) {
            struct GetDataResponse res;
            int len;
            deserialize_GetDataResponse(ia, "reply", &res);
            if (res.data.len <= sc->data.buff_len) {
                len = res.data.len;
            } else {
                len = sc->data.buff_len;
            }
            sc->data.buff_len = len;
            // check if len is negative
            // just of NULL which is -1 int
            if (len == -1) {
                sc->data.buffer = NULL;
            } else {
                memcpy(sc->data.buffer, res.data.buff, len);
            }
            sc->data.stat = res.stat;
            deallocate_GetDataResponse(&res);
        }
        break;
    case COMPLETION_STAT:
        if (sc->rc==0) {
            struct SetDataResponse res;
            deserialize_SetDataResponse(ia, "reply", &res);
            sc->stat = res.stat;
            deallocate_SetDataResponse(&res);
        }
        break;
    case COMPLETION_STRINGLIST:
        if (sc->rc==0) {
            struct GetChildrenResponse res;
            deserialize_GetChildrenResponse(ia, "reply", &res);
            sc->strs2 = res.children;
            /* We don't deallocate since we are passing it back */
            // deallocate_GetChildrenResponse(&res);
        }
        break;
    case COMPLETION_STRINGLIST_STAT:
        if (sc->rc==0) {
            struct GetChildren2Response res;
            deserialize_GetChildren2Response(ia, "reply", &res);
            sc->strs_stat.strs2 = res.children;
            sc->strs_stat.stat2 = res.stat;
            /* We don't deallocate since we are passing it back */
            // deallocate_GetChildren2Response(&res);
        }
        break;
    case COMPLETION_STRING:
        if (sc->rc==0) {
            struct CreateResponse res;
            int len;
            const char * client_path;
            deserialize_CreateResponse(ia, "reply", &res);
            //ZOOKEEPER-1027
            client_path = sub_string(zh, res.path); 
            len = strlen(client_path) + 1;if (len > sc->str.str_len) {
                len = sc->str.str_len;
            }
            if (len > 0) {
                memcpy(sc->str.str, client_path, len - 1);
                sc->str.str[len - 1] = '\0';
            }
            free_duplicate_path(client_path, res.path);
            deallocate_CreateResponse(&res);
        }
        break;
    case COMPLETION_ACLLIST:
        if (sc->rc==0) {
            struct GetACLResponse res;
            deserialize_GetACLResponse(ia, "reply", &res);
            sc->acl.acl = res.acl;
            sc->acl.stat = res.stat;
            /* We don't deallocate since we are passing it back */
            //deallocate_GetACLResponse(&res);
        }
        break;
    case COMPLETION_VOID:
        break;
    case COMPLETION_MULTI:
        sc->rc = deserialize_multi(cptr->xid, cptr, ia);
        break;
    default:
        LOG_DEBUG("Unsupported completion type=" << cptr->c.type);
        break;
    }
}

static int deserialize_multi(int xid, completion_list_t *cptr, struct iarchive *ia)
{
    int rc = 0;
    completion_head_t *clist = &cptr->c.clist;
    struct MultiHeader mhdr = { STRUCT_INITIALIZER(type , 0), STRUCT_INITIALIZER(done , 0), STRUCT_INITIALIZER(err , 0) };
    assert(clist);
    deserialize_MultiHeader(ia, "multiheader", &mhdr);
    while (!mhdr.done) {
        completion_list_t *entry = dequeue_completion(clist);
        assert(entry);

        if (mhdr.type == -1) {
            struct ErrorResponse er;
            deserialize_ErrorResponse(ia, "error", &er);
            mhdr.err = er.err ;
            if (rc == 0 && er.err != 0 && er.err != ZRUNTIMEINCONSISTENCY) {
                rc = er.err;
            }
        }

        deserialize_response(entry->c.type, xid, mhdr.type == -1, mhdr.err, entry, ia);
        deserialize_MultiHeader(ia, "multiheader", &mhdr);
    }

    return rc;
}

static void deserialize_response(int type, int xid, int failed, int rc, completion_list_t *cptr, struct iarchive *ia)
{
    switch (type) {
    case COMPLETION_DATA:
        LOG_DEBUG(boost::format("Calling COMPLETION_DATA for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.data_result(rc, 0, 0, 0, cptr->data);
        } else {
            struct GetDataResponse res;
            deserialize_GetDataResponse(ia, "reply", &res);
            cptr->c.data_result(rc, res.data.buff, res.data.len,
                    &res.stat, cptr->data);
            deallocate_GetDataResponse(&res);
        }
        break;
    case COMPLETION_STAT:
        LOG_DEBUG(boost::format("Calling COMPLETION_STAT for xid=%#08x failed=%d rc=%d") % 
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.stat_result(rc, 0, cptr->data);
        } else {
            struct SetDataResponse res;
            deserialize_SetDataResponse(ia, "reply", &res);
            cptr->c.stat_result(rc, &res.stat, cptr->data);
            deallocate_SetDataResponse(&res);
        }
        break;
    case COMPLETION_STRINGLIST:
        LOG_DEBUG(boost::format("Calling COMPLETION_STRINGLIST for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.strings_result(rc, 0, cptr->data);
        } else {
            struct GetChildrenResponse res;
            deserialize_GetChildrenResponse(ia, "reply", &res);
            cptr->c.strings_result(rc, &res.children, cptr->data);
            deallocate_GetChildrenResponse(&res);
        }
        break;
    case COMPLETION_STRINGLIST_STAT:
        LOG_DEBUG(boost::format("Calling COMPLETION_STRINGLIST_STAT for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.strings_stat_result(rc, 0, 0, cptr->data);
        } else {
            struct GetChildren2Response res;
            deserialize_GetChildren2Response(ia, "reply", &res);
            cptr->c.strings_stat_result(rc, &res.children, &res.stat, cptr->data);
            deallocate_GetChildren2Response(&res);
        }
        break;
    case COMPLETION_STRING:
        LOG_DEBUG(boost::format("Calling COMPLETION_STRING for xid=%#08x failed=%d, rc=%d") %
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.string_result(rc, 0, cptr->data);
        } else {
            struct CreateResponse res;
            deserialize_CreateResponse(ia, "reply", &res);
            cptr->c.string_result(rc, res.path, cptr->data);
            deallocate_CreateResponse(&res);
        }
        break;
    case COMPLETION_ACLLIST:
        LOG_DEBUG(boost::format("Calling COMPLETION_ACLLIST for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        if (failed) {
            cptr->c.acl_result(rc, 0, 0, cptr->data);
        } else {
            struct GetACLResponse res;
            deserialize_GetACLResponse(ia, "reply", &res);
            cptr->c.acl_result(rc, &res.acl, &res.stat, cptr->data);
            deallocate_GetACLResponse(&res);
        }
        break;
    case COMPLETION_VOID:
        LOG_DEBUG(boost::format("Calling COMPLETION_VOID for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        if (xid == PING_XID) {
            // We want to skip the ping
        } else {
            assert(cptr->c.void_result);
            cptr->c.void_result(rc, cptr->data);
        }
        break;
    case COMPLETION_MULTI:
        LOG_DEBUG(boost::format("Calling COMPLETION_MULTI for xid=%#08x failed=%d rc=%d") %
                                cptr->xid % failed % rc);
        rc = deserialize_multi(xid, cptr, ia);
        assert(cptr->c.void_result);
        cptr->c.void_result(rc, cptr->data);
        break;
    default:
        LOG_DEBUG("Unsupported completion type=" << cptr->c.type);
    }
}


/* handles async completion (both single- and multithreaded) */
void process_completions(zhandle_t *zh)
{
    completion_list_t *cptr;
    while ((cptr = dequeue_completion(&zh->completions_to_process)) != 0) {
        struct ReplyHeader hdr;
        buffer_t *bptr = cptr->buffer;
        struct iarchive *ia = create_buffer_iarchive(bptr->buffer,
                bptr->len);
        deserialize_ReplyHeader(ia, "hdr", &hdr);

        if (hdr.xid == WATCHER_EVENT_XID) {
            int type, state;
            struct WatcherEvent evt;
            deserialize_WatcherEvent(ia, "event", &evt);
            /* We are doing a notification, so there is no pending request */
            type = evt.type;
            state = evt.state;
            /* This is a notification so there aren't any pending requests */
            LOG_DEBUG(boost::format("Calling a watcher for node [%s], type = %d event=%s") %
                                    (evt.path==NULL?"NULL":evt.path) % cptr->c.type %
                                    watcherEvent2String(type));
            deliverWatchers(zh,type,state,evt.path, &cptr->c.watcher_result);
            deallocate_WatcherEvent(&evt);
        } else {
            deserialize_response(cptr->c.type, hdr.xid, hdr.err != 0, hdr.err, cptr, ia);
        }
        destroy_completion_entry(cptr);
        close_buffer_iarchive(&ia);
    }
}

int zookeeper_process(zhandle_t *zh, int events)
{
    buffer_t *bptr;
    int rc;

    if (zh==NULL)
        return ZBADARGUMENTS;
    if (is_unrecoverable(zh))
        return ZINVALIDSTATE;
    api_prolog(zh);
    // TODO decide what to do
    //IF_DEBUG(checkResponseLatency(zh));
    rc = check_events(zh, events);
    if (rc!=ZOK)
        return api_epilog(zh, rc);

    // TODO decide what to do
    //IF_DEBUG(isSocketReadable(zh));

    while (rc >= 0 && (bptr=dequeue_buffer(zh->to_process.get()))) {
        struct ReplyHeader hdr;
        struct iarchive *ia = create_buffer_iarchive(
                                    bptr->buffer, bptr->curr_offset);
        deserialize_ReplyHeader(ia, "hdr", &hdr);
        if (hdr.zxid > 0) {
            zh->last_zxid = hdr.zxid;
        } else {
            // fprintf(stderr, "Got %#x for %#x\n", hdr.zxid, hdr.xid);
        }

        if (hdr.xid == WATCHER_EVENT_XID) {
            struct WatcherEvent evt;
            int type = 0;
            char *path = NULL;
            completion_list_t *c = NULL;

            LOG_DEBUG("Processing WATCHER_EVENT");

            deserialize_WatcherEvent(ia, "event", &evt);
            type = evt.type;
            path = evt.path;
            /* We are doing a notification, so there is no pending request */
            c = create_completion_entry(WATCHER_EVENT_XID,-1,0,0,0,0);
            c->buffer = bptr;
            c->c.watcher_result = collectWatchers(zh, type, path);

            // We cannot free until now, otherwise path will become invalid
            deallocate_WatcherEvent(&evt);
            queue_completion(&zh->completions_to_process, c);
        } else if (hdr.xid == SET_WATCHES_XID) {
            LOG_DEBUG("Processing SET_WATCHES");
            free_buffer(bptr);
        } else if (hdr.xid == AUTH_XID){
            LOG_DEBUG("Processing AUTH_XID");

            /* special handling for the AUTH response as it may come back
             * out-of-band */
            auth_completion_func(hdr.err,zh);
            free_buffer(bptr);
            /* authentication completion may change the connection state to
             * unrecoverable */
            if(is_unrecoverable(zh)){
                handle_error(zh, ZAUTHFAILED);
                close_buffer_iarchive(&ia);
                return api_epilog(zh, ZAUTHFAILED);
            }
        } else {
            int rc = hdr.err;
            /* Find the request corresponding to the response */
            completion_list_t *cptr = dequeue_completion(&zh->sent_requests);

            /* [ZOOKEEPER-804] Don't assert if zookeeper_close has been called. */
            if (zh->close_requested == 1) {
                if (cptr) {
                    destroy_completion_entry(cptr);
                    cptr = NULL;
                }
                close_buffer_iarchive(&ia);
                return api_epilog(zh,ZINVALIDSTATE);
            }
            assert(cptr);
            /* The requests are going to come back in order */
            if (cptr->xid != hdr.xid) {
                LOG_DEBUG("Processing unexpected or out-of-order response!");

                // received unexpected (or out-of-order) response
                close_buffer_iarchive(&ia);
                free_buffer(bptr);
                // put the completion back on the queue (so it gets properly
                // signaled and deallocated) and disconnect from the server
                // TODO destroy completion
                //queue_completion(&zh->sent_requests,cptr);
                return handle_socket_error_msg(zh, __LINE__,ZRUNTIMEINCONSISTENCY,
                "");
            }

            activateWatcher(zh, cptr->watcher, rc);

            if (cptr->c.void_result != SYNCHRONOUS_MARKER) {
                if(hdr.xid == PING_XID){
                    int elapsed = 0;
                    struct timeval now;
                    gettimeofday(&now, 0);
                    elapsed = calculate_interval(&zh->last_ping, &now);
                    LOG_DEBUG("Got ping response in " << elapsed << "ms");

                    // Nothing to do with a ping response
                    free_buffer(bptr);
                    destroy_completion_entry(cptr);
                } else {
                    cptr->buffer = bptr;
                    queue_completion(&zh->completions_to_process, cptr);
                }
            } else {
                struct sync_completion
                        *sc = (struct sync_completion*)cptr->data;
                sc->rc = rc;
                
                process_sync_completion(cptr, sc, ia, zh); 
                
                notify_sync_completion(sc);
                free_buffer(bptr);
                zh->outstanding_sync--;
                destroy_completion_entry(cptr);
            }
        }

        close_buffer_iarchive(&ia);

    }
    if (process_async(zh->outstanding_sync)) {
        process_completions(zh);
    }
    return api_epilog(zh,ZOK);}

int zoo_state(zhandle_t *zh)
{
    if(zh!=0)
        return zh->state;
    return 0;
}

static watcher_registration_t* create_watcher_registration(const char* path,
        result_checker_fn checker,watcher_fn watcher,void* ctx){
    watcher_registration_t* wo;
    if(watcher==0)
        return 0;
    wo=(watcher_registration_t*)calloc(1,sizeof(watcher_registration_t));
    wo->path=strdup(path);
    wo->watcher=watcher;
    wo->context=ctx;
    wo->checker=checker;
    return wo;
}

static void destroy_watcher_registration(watcher_registration_t* wo){
    if(wo!=0){
        free((void*)wo->path);
        free(wo);
    }
}

static completion_list_t* create_completion_entry(int xid, int completion_type,
        const void *dc, const void *data,watcher_registration_t* wo, completion_head_t *clist)
{
    completion_list_t *c = (completion_list_t*)calloc(1,sizeof(completion_list_t));
    if (!c) {
        LOG_ERROR("out of memory");
        return 0;
    }
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
        assert(clist);
        c->c.void_result = (void_completion_t)dc;
        c->c.clist = *clist;
        break;
    }
    c->xid = xid;
    c->watcher = wo;
    
    return c;
}

static void destroy_completion_entry(completion_list_t* c){
    if(c!=0){
        destroy_watcher_registration(c->watcher);
        if(c->buffer!=0)
            free_buffer(c->buffer);
        free(c);
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
        watcher_registration_t* wo, completion_head_t *clist)
{
    completion_list_t *c =create_completion_entry(xid, completion_type, dc,
            data, wo, clist);
    int rc = 0;
    if (!c)
        return ZSYSTEMERROR;
    {
        boost::lock_guard<boost::mutex> lock(*(zh->sent_requests.lock));
        if (zh->close_requested != 1) {
            queue_completion_nolock(&zh->sent_requests, c);
            if (dc == SYNCHRONOUS_MARKER) {
                zh->outstanding_sync++;
            }
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
        const void *data,watcher_registration_t* wo)
{
    return add_completion(zh, xid, COMPLETION_DATA, (const void*)dc, data, wo, 0);
}

static int add_stat_completion(zhandle_t *zh, int xid, stat_completion_t dc,
        const void *data,watcher_registration_t* wo)
{
    return add_completion(zh, xid, COMPLETION_STAT, (const void*)dc, data, wo, 0);
}

static int add_strings_completion(zhandle_t *zh, int xid,
        strings_completion_t dc, const void *data,watcher_registration_t* wo)
{
    return add_completion(zh, xid, COMPLETION_STRINGLIST, (const void*)dc, data, wo, 0);
}

static int add_strings_stat_completion(zhandle_t *zh, int xid,
        strings_stat_completion_t dc, const void *data,watcher_registration_t* wo)
{
    return add_completion(zh, xid, COMPLETION_STRINGLIST_STAT, (const void*)dc, data, wo, 0);
}

static int add_acl_completion(zhandle_t *zh, int xid, acl_completion_t dc,
        const void *data)
{
    return add_completion(zh, xid, COMPLETION_ACLLIST, (const void*)dc, data, 0, 0);
}

static int add_void_completion(zhandle_t *zh, int xid, void_completion_t dc,
        const void *data)
{
    return add_completion(zh, xid, COMPLETION_VOID, (const void*)dc, data, 0, 0);
}

static int add_string_completion(zhandle_t *zh, int xid,
        string_completion_t dc, const void *data)
{
    return add_completion(zh, xid, COMPLETION_STRING, (const void*)dc, data, 0, 0);
}

static int add_multi_completion(zhandle_t *zh, int xid, void_completion_t dc,
        const void *data, completion_head_t *clist)
{
    return add_completion(zh, xid, COMPLETION_MULTI, (const void*)dc, data, 0, clist);
}

int zookeeper_close(zhandle_t *zh)
{
    int rc=ZOK;
    if (zh==0)
        return ZBADARGUMENTS;

    zh->close_requested=1;
    if (inc_ref_counter(zh)>1) {
        /* We have incremented the ref counter to prevent the
         * completions from calling zookeeper_close before we have
         * completed the adaptor_finish call below. */

	/* Signal any syncronous completions before joining the threads */
        enter_critical(zh);
        free_completions(zh,1,ZCLOSING);
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
        struct oarchive *oa;
        struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_CLOSE_OP)};
        LOG_INFO(boost::format("Closing zookeeper sessionId=%#llx to [%s]\n") %
            zh->client_id.client_id % format_current_endpoint_info(zh));
        oa = create_buffer_oarchive();
        rc = serialize_RequestHeader(oa, "header", &h);
        rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
                get_buffer_len(oa));
        /* We queued the buffer, so don't free it */
        close_buffer_oarchive(&oa, 0);
        if (rc < 0) {
            rc = ZMARSHALLINGERROR;
            goto finish;
        }

        /* make sure the close request is sent; we set timeout to an arbitrary
         * (but reasonable) number of milliseconds since we want the call to block*/
        rc=adaptor_send_queue(zh, 3000);
    }else{
        LOG_INFO(boost::format("Freeing zookeeper resources for sessionId=%#llx") %
                               zh->client_id.client_id);
        rc = ZOK;
    }

finish:
    destroy(zh);
    adaptor_destroy(zh);
    free(zh);
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

static int Request_path_watch_init(zhandle_t *zh, int flags,
        char **path_out, const char *path,
        int32_t *watch_out, uint32_t watch)
{
    int rc = Request_path_init(zh, flags, path_out, path);
    if (rc != ZOK) {
        return rc;
    }
    *watch_out = watch;
    return ZOK;
}

/*---------------------------------------------------------------------------*
 * ASYNC API
 *---------------------------------------------------------------------------*/
int zoo_aget(zhandle_t *zh, const char *path, int watch, data_completion_t dc,
        const void *data)
{
    return zoo_awget(zh,path,watch?zh->watcher:0,zh->context,dc,data);
}

int zoo_awget(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        data_completion_t dc, const void *data)
{
    struct oarchive *oa;
    char *server_path = prepend_string(zh, path);
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type ,ZOO_GETDATA_OP)};
    struct GetDataRequest req =  { (char*)server_path, watcher!=0 };
    int rc;

    if (zh==0 || !isValidPath(server_path, 0)) {
        free_duplicate_path(server_path, path);
        return ZBADARGUMENTS;
    }
    if (is_unrecoverable(zh)) {
        free_duplicate_path(server_path, path);
        return ZINVALIDSTATE;
    }
    oa=create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_GetDataRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_data_completion(zh, h.xid, dc, data,
        create_watcher_registration(server_path,data_result_checker,watcher,watcherCtx));
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(server_path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

static int SetDataRequest_init(zhandle_t *zh, struct SetDataRequest *req,
        const char *path, const char *buffer, int buflen, int version)
{
    int rc;
    assert(req);
    rc = Request_path_init(zh, 0, &req->path, path);
    if (rc != ZOK) {
        return rc;
    }
    req->data.buff = (char*)buffer;
    req->data.len = buflen;
    req->version = version;

    return ZOK;
}

int zoo_aset(zhandle_t *zh, const char *path, const char *buffer, int buflen,
        int version, stat_completion_t dc, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER(xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_SETDATA_OP)};
    struct SetDataRequest req;
    int rc = SetDataRequest_init(zh, &req, path, buffer, buflen, version);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_SetDataRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_stat_completion(zh, h.xid, dc, data,0);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
             h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

static int CreateRequest_init(zhandle_t *zh, struct CreateRequest *req,
        const char *path, const char *value,
        int valuelen, const struct ACL_vector *acl_entries, int flags)
{
    int rc;
    assert(req);
    rc = Request_path_init(zh, flags, &req->path, path);
    assert(req);
    if (rc != ZOK) {
        return rc;
    }
    req->flags = flags;
    req->data.buff = (char*)value;
    req->data.len = valuelen;
    if (acl_entries == 0) {
        req->acl.count = 0;
        req->acl.data = 0;
    } else {
        req->acl = *acl_entries;
    }

    return ZOK;
}

int zoo_acreate(zhandle_t *zh, const char *path, const char *value,
        int valuelen, const struct ACL_vector *acl_entries, int flags,
        string_completion_t completion, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type ,ZOO_CREATE_OP) };
    struct CreateRequest req;

    int rc = CreateRequest_init(zh, &req, 
            path, value, valuelen, acl_entries, flags);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_CreateRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_string_completion(zh, h.xid, completion, data);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int DeleteRequest_init(zhandle_t *zh, struct DeleteRequest *req, 
        const char *path, int version)
{
    int rc = Request_path_init(zh, 0, &req->path, path);
    if (rc != ZOK) {
        return rc;
    }
    req->version = version;
    return ZOK;
}

int zoo_adelete(zhandle_t *zh, const char *path, int version,
        void_completion_t completion, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_DELETE_OP)};
    struct DeleteRequest req;
    int rc = DeleteRequest_init(zh, &req, path, version);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_DeleteRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_void_completion(zh, h.xid, completion, data);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aexists(zhandle_t *zh, const char *path, int watch,
        stat_completion_t sc, const void *data)
{
    return zoo_awexists(zh,path,watch?zh->watcher:0,zh->context,sc,data);
}

int zoo_awexists(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        stat_completion_t completion, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid ,get_xid()), STRUCT_INITIALIZER (type , ZOO_EXISTS_OP) };
    struct ExistsRequest req;
    int rc = Request_path_watch_init(zh, 0, &req.path, path, 
            &req.watch, watcher != NULL);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_ExistsRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_stat_completion(zh, h.xid, completion, data,
        create_watcher_registration(req.path,exists_result_checker,
                watcher,watcherCtx));
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

static int zoo_awget_children_(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_completion_t sc,
         const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_GETCHILDREN_OP)};
    struct GetChildrenRequest req ;
    int rc = Request_path_watch_init(zh, 0, &req.path, path, 
            &req.watch, watcher != NULL);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_GetChildrenRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_strings_completion(zh, h.xid, sc, data,
            create_watcher_registration(req.path,child_result_checker,watcher,watcherCtx));
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aget_children(zhandle_t *zh, const char *path, int watch,
        strings_completion_t dc, const void *data)
{
    return zoo_awget_children_(zh,path,watch?zh->watcher:0,zh->context,dc,data);
}

int zoo_awget_children(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_completion_t dc,
         const void *data)
{
    return zoo_awget_children_(zh,path,watcher,watcherCtx,dc,data);
}

static int zoo_awget_children2_(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_stat_completion_t ssc,
         const void *data)
{
    /* invariant: (sc == NULL) != (sc == NULL) */
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER( xid, get_xid()), STRUCT_INITIALIZER (type ,ZOO_GETCHILDREN2_OP)};
    struct GetChildren2Request req ;
    int rc = Request_path_watch_init(zh, 0, &req.path, path, 
            &req.watch, watcher != NULL);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_GetChildren2Request(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_strings_stat_completion(zh, h.xid, ssc, data,
            create_watcher_registration(req.path,child_result_checker,watcher,watcherCtx));
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aget_children2(zhandle_t *zh, const char *path, int watch,
        strings_stat_completion_t dc, const void *data)
{
    return zoo_awget_children2_(zh,path,watch?zh->watcher:0,zh->context,dc,data);
}

int zoo_awget_children2(zhandle_t *zh, const char *path,
         watcher_fn watcher, void* watcherCtx,
         strings_stat_completion_t dc,
         const void *data)
{
    return zoo_awget_children2_(zh,path,watcher,watcherCtx,dc,data);
}

int zoo_async(zhandle_t *zh, const char *path,
        string_completion_t completion, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_SYNC_OP)};
    struct SyncRequest req;
    int rc = Request_path_init(zh, 0, &req.path, path);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_SyncRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_string_completion(zh, h.xid, completion, data);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}


int zoo_aget_acl(zhandle_t *zh, const char *path, acl_completion_t completion,
        const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER(type ,ZOO_GETACL_OP)};
    struct GetACLRequest req;
    int rc = Request_path_init(zh, 0, &req.path, path) ;
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_GetACLRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_acl_completion(zh, h.xid, completion, data);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

int zoo_aset_acl(zhandle_t *zh, const char *path, int version,
        struct ACL_vector *acl, void_completion_t completion, const void *data)
{
    struct oarchive *oa;
    struct RequestHeader h = { STRUCT_INITIALIZER(xid ,get_xid()), STRUCT_INITIALIZER (type , ZOO_SETACL_OP)};
    struct SetACLRequest req;
    int rc = Request_path_init(zh, 0, &req.path, path);
    if (rc != ZOK) {
        return rc;
    }
    oa = create_buffer_oarchive();
    req.acl = *acl;
    req.version = version;
    rc = serialize_RequestHeader(oa, "header", &h);
    rc = rc < 0 ? rc : serialize_SetACLRequest(oa, "req", &req);
    enter_critical(zh);
    rc = rc < 0 ? rc : add_void_completion(zh, h.xid, completion, data);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    free_duplicate_path(req.path, path);
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending request xid=%#08x for path [%s] to %s") %
                            h.xid % path % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);
    return (rc < 0)?ZMARSHALLINGERROR:ZOK;
}

/* Completions for multi-op results */
static void op_result_string_completion(int err, const char *value, const void *data)
{
    struct zoo_op_result *result = (struct zoo_op_result *)data;
    assert(result);
    result->err = err;
    
    if (result->value && value) {
        int len = strlen(value) + 1;
		if (len > result->valuelen) {
			len = result->valuelen;
		}
		if (len > 0) {
			memcpy(result->value, value, len - 1);
			result->value[len - 1] = '\0';
		}
    } else {
        result->value = NULL;
    }
}

static void op_result_void_completion(int err, const void *data)
{
    struct zoo_op_result *result = (struct zoo_op_result *)data;
    assert(result);
    result->err = err;
}

static void op_result_stat_completion(int err, const struct Stat *stat, const void *data)
{
    struct zoo_op_result *result = (struct zoo_op_result *)data;
    assert(result);
    result->err = err;

    if (result->stat && err == 0 && stat) {
        *result->stat = *stat;
    } else {
        result->stat = NULL ;
    }
}   

static int CheckVersionRequest_init(zhandle_t *zh, struct CheckVersionRequest *req,
        const char *path, int version)
{
    int rc ;
    assert(req);
    rc = Request_path_init(zh, 0, &req->path, path);
    if (rc != ZOK) {
        return rc;
    }
    req->version = version;

    return ZOK;
}

int zoo_amulti(zhandle_t *zh, int count, const zoo_op_t *ops,
        zoo_op_result_t *results, void_completion_t completion, const void *data)
{
    struct RequestHeader h = { STRUCT_INITIALIZER(xid, get_xid()), STRUCT_INITIALIZER(type, ZOO_MULTI_OP) };
    struct MultiHeader mh = { STRUCT_INITIALIZER(type, -1), STRUCT_INITIALIZER(done, 1), STRUCT_INITIALIZER(err, -1) };
    struct oarchive *oa = create_buffer_oarchive();
    completion_head_t clist = { 0 };
    clist.lock.reset(new boost::mutex());
    clist.cond.reset(new boost::condition_variable());

    int rc = serialize_RequestHeader(oa, "header", &h);

    int index = 0;
    for (index=0; index < count; index++) {
        const zoo_op_t *op = ops+index;
        zoo_op_result_t *result = results+index;
        completion_list_t *entry = NULL;

        struct MultiHeader mh = { STRUCT_INITIALIZER(type, op->type), STRUCT_INITIALIZER(done, 0), STRUCT_INITIALIZER(err, -1) };
        rc = rc < 0 ? rc : serialize_MultiHeader(oa, "multiheader", &mh);
     
        switch(op->type) {
            case ZOO_CREATE_OP: {
                struct CreateRequest req;

                rc = rc < 0 ? rc : CreateRequest_init(zh, &req, 
                                        op->create_op.path, op->create_op.data, 
                                        op->create_op.datalen, op->create_op.acl, 
                                        op->create_op.flags);
                rc = rc < 0 ? rc : serialize_CreateRequest(oa, "req", &req);
                result->value = op->create_op.buf;
				result->valuelen = op->create_op.buflen;

                enter_critical(zh);
                entry = create_completion_entry(h.xid, COMPLETION_STRING, (const void*)op_result_string_completion, result, 0, 0); 
                leave_critical(zh);
                free_duplicate_path(req.path, op->create_op.path);
                break;
            }

            case ZOO_DELETE_OP: {
                struct DeleteRequest req;
                rc = rc < 0 ? rc : DeleteRequest_init(zh, &req, op->delete_op.path, op->delete_op.version);
                rc = rc < 0 ? rc : serialize_DeleteRequest(oa, "req", &req);

                enter_critical(zh);
                entry = create_completion_entry(h.xid, COMPLETION_VOID, (const void*)op_result_void_completion, result, 0, 0); 
                leave_critical(zh);
                free_duplicate_path(req.path, op->delete_op.path);
                break;
            }

            case ZOO_SETDATA_OP: {
                struct SetDataRequest req;
                rc = rc < 0 ? rc : SetDataRequest_init(zh, &req,
                                        op->set_op.path, op->set_op.data, 
                                        op->set_op.datalen, op->set_op.version);
                rc = rc < 0 ? rc : serialize_SetDataRequest(oa, "req", &req);
                result->stat = op->set_op.stat;

                enter_critical(zh);
                entry = create_completion_entry(h.xid, COMPLETION_STAT, (const void*)op_result_stat_completion, result, 0, 0); 
                leave_critical(zh);
                free_duplicate_path(req.path, op->set_op.path);
                break;
            }

            case ZOO_CHECK_OP: {
                struct CheckVersionRequest req;
                rc = rc < 0 ? rc : CheckVersionRequest_init(zh, &req,
                                        op->check_op.path, op->check_op.version);
                rc = rc < 0 ? rc : serialize_CheckVersionRequest(oa, "req", &req);

                enter_critical(zh);
                entry = create_completion_entry(h.xid, COMPLETION_VOID, (const void*)op_result_void_completion, result, 0, 0); 
                leave_critical(zh);
                free_duplicate_path(req.path, op->check_op.path);
                break;
            } 

            default:
                LOG_ERROR("Unimplemented sub-op type=" <<
                          op->type << " in multi-op");
                return ZUNIMPLEMENTED; 
        }

        queue_completion(&clist, entry);
    }

    rc = rc < 0 ? rc : serialize_MultiHeader(oa, "multiheader", &mh);
  
    /* BEGIN: CRTICIAL SECTION */
    enter_critical(zh);
    rc = rc < 0 ? rc : add_multi_completion(zh, h.xid, completion, data, &clist);
    rc = rc < 0 ? rc : queue_buffer_bytes(zh->to_send.get(), get_buffer(oa),
            get_buffer_len(oa));
    leave_critical(zh);
    
    /* We queued the buffer, so don't free it */
    close_buffer_oarchive(&oa, 0);

    LOG_DEBUG(boost::format("Sending multi request xid=%#x with %d subrequests to %s") %
                            h.xid % index % format_current_endpoint_info(zh));
    /* make a best (non-blocking) effort to send the requests asap */
    adaptor_send_queue(zh, 0);

    return (rc < 0) ? ZMARSHALLINGERROR : ZOK;
}

void zoo_create_op_init(zoo_op_t *op, const char *path, const char *value,
        int valuelen,  const struct ACL_vector *acl, int flags, 
        char *path_buffer, int path_buffer_len)
{
    assert(op);
    op->type = ZOO_CREATE_OP;
    op->create_op.path = path;
    op->create_op.data = value;
    op->create_op.datalen = valuelen;
    op->create_op.acl = acl;
    op->create_op.flags = flags;
    op->create_op.buf = path_buffer;
    op->create_op.buflen = path_buffer_len;
}

void zoo_delete_op_init(zoo_op_t *op, const char *path, int version)
{
    assert(op);
    op->type = ZOO_DELETE_OP;
    op->delete_op.path = path;
    op->delete_op.version = version;
}

void zoo_set_op_init(zoo_op_t *op, const char *path, const char *buffer, 
        int buflen, int version, struct Stat *stat)
{
    assert(op);
    op->type = ZOO_SETDATA_OP;
    op->set_op.path = path;
    op->set_op.data = buffer;
    op->set_op.datalen = buflen;
    op->set_op.version = version;
    op->set_op.stat = stat;
}

void zoo_check_op_init(zoo_op_t *op, const char *path, int version)
{
    assert(op);
    op->type = ZOO_CHECK_OP;
    op->check_op.path = path;
    op->check_op.version = version;
}

int zoo_multi(zhandle_t *zh, int count, const zoo_op_t *ops, zoo_op_result_t *results)
{
    int rc;
 
    struct sync_completion *sc = alloc_sync_completion();
    if (!sc) {
        return ZSYSTEMERROR;
    }
   
    rc = zoo_amulti(zh, count, ops, results, (void (*)(int, const void*))SYNCHRONOUS_MARKER, sc);
    if (rc == ZOK) {
        wait_sync_completion(sc);
        rc = sc->rc;
    }
    free_sync_completion(sc);

    return rc;
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
          lock(zh->to_send.get()->mutex_);
        while (!(zh->to_send.get()->bufferList_.empty()) &&
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

            rc = send_buffer(zh->fd, &(zh->to_send.get()->bufferList_.front()));
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
                remove_buffer(zh->to_send.get());
            gettimeofday(&zh->last_send, 0);
            rc = ZOK;
        }
    }
    return rc;
}

const char* zerror(int c)
{
    switch (c){
    case ZOK:
      return "ok";
    case ZSYSTEMERROR:
      return "system error";
    case ZRUNTIMEINCONSISTENCY:
      return "run time inconsistency";
    case ZDATAINCONSISTENCY:
      return "data inconsistency";
    case ZCONNECTIONLOSS:
      return "connection loss";
    case ZMARSHALLINGERROR:
      return "marshalling error";
    case ZUNIMPLEMENTED:
      return "unimplemented";
    case ZOPERATIONTIMEOUT:
      return "operation timeout";
    case ZBADARGUMENTS:
      return "bad arguments";
    case ZINVALIDSTATE:
      return "invalid zhandle state";
    case ZAPIERROR:
      return "api error";
    case ZNONODE:
      return "no node";
    case ZNOAUTH:
      return "not authenticated";
    case ZBADVERSION:
      return "bad version";
    case  ZNOCHILDRENFOREPHEMERALS:
      return "no children for ephemerals";
    case ZNODEEXISTS:
      return "node exists";
    case ZNOTEMPTY:
      return "not empty";
    case ZSESSIONEXPIRED:
      return "session expired";
    case ZINVALIDCALLBACK:
      return "invalid callback";
    case ZINVALIDACL:
      return "invalid acl";
    case ZAUTHFAILED:
      return "authentication failed";
    case ZCLOSING:
      return "zookeeper is closing";
    case ZNOTHING:
      return "(not error) no server responses to process";
    case ZSESSIONMOVED:
      return "session moved to another server, so operation is ignored";
    }
    if (c > 0) {
      return strerror(c);
    }
    return "unknown error";
}

int zoo_add_auth(zhandle_t *zh,const char* scheme,const char* cert,
        int certLen,void_completion_t completion, const void *data)
{
    struct buffer auth;
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

    if(cert!=NULL && certLen!=0){
        auth.buff=(char*)calloc(1,certLen);
        if(auth.buff==0) {
            return ZSYSTEMERROR;
        }
        memcpy(auth.buff,cert,certLen);
        auth.len=certLen;
    } else {
        auth.buff = 0;
        auth.len = 0;
    }

    {
        boost::lock_guard<boost::mutex> lock(zh->auth_h.get()->mutex_);
        authinfo = new auth_info();
        authinfo->scheme = scheme;
        authinfo->auth = auth;
        authinfo->completion=completion;
        authinfo->data=(const char*)data;
        zh->auth_h.get()->authList_.push_back(authinfo);
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

/*---------------------------------------------------------------------------*
 * SYNC API
 *---------------------------------------------------------------------------*/
int zoo_create(zhandle_t *zh, const char *path, const char *value,
        int valuelen, const struct ACL_vector *acl, int flags,
        char *path_buffer, int path_buffer_len)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    sc->str.str = path_buffer;
    sc->str.str_len = path_buffer_len;
    rc=zoo_acreate(zh, path, value, valuelen, acl, flags, (void (*)(int, const char*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_delete(zhandle_t *zh, const char *path, int version)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc=zoo_adelete(zh, path, version, (void (*)(int, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_exists(zhandle_t *zh, const char *path, int watch, struct Stat *stat)
{
    return zoo_wexists(zh,path,watch?zh->watcher:0,zh->context,stat);
}

int zoo_wexists(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx, struct Stat *stat)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc=zoo_awexists(zh,path,watcher,watcherCtx,(void (*)(int, const Stat*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0&& stat) {
            *stat = sc->stat;
        }
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer,
        int* buffer_len, struct Stat *stat)
{
    return zoo_wget(zh,path,watch?zh->watcher:0,zh->context,
            buffer,buffer_len,stat);
}

int zoo_wget(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        char *buffer, int* buffer_len, struct Stat *stat)
{
    struct sync_completion *sc;
    int rc=0;

    if(buffer_len==NULL)
        return ZBADARGUMENTS;
    if((sc=alloc_sync_completion())==NULL)
        return ZSYSTEMERROR;

    sc->data.buffer = buffer;
    sc->data.buff_len = *buffer_len;
    rc=zoo_awget(zh, path, watcher, watcherCtx, (void (*)(int, const char*, int, const Stat*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0) {
            if(stat)
                *stat = sc->data.stat;
            *buffer_len = sc->data.buff_len;
        }
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_set(zhandle_t *zh, const char *path, const char *buffer, int buflen,
        int version)
{
  return zoo_set2(zh, path, buffer, buflen, version, 0);
}

int zoo_set2(zhandle_t *zh, const char *path, const char *buffer, int buflen,
        int version, struct Stat *stat)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc=zoo_aset(zh, path, buffer, buflen, version, (void (*)(int, const Stat*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0 && stat) {
            *stat = sc->stat;
        }
    }
    free_sync_completion(sc);
    return rc;
}

static int zoo_wget_children_(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        struct String_vector *strings)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc= zoo_awget_children (zh, path, watcher, watcherCtx, (void (*)(int, const String_vector*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0) {
            if (strings) {
                *strings = sc->strs2;
            } else {
                deallocate_String_vector(&sc->strs2);
            }
        }
    }
    free_sync_completion(sc);
    return rc;
}

static int zoo_wget_children2_(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        struct String_vector *strings, struct Stat *stat)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc= zoo_awget_children2(zh, path, watcher, watcherCtx, (void (*)(int, const String_vector*, const Stat*, const void*))SYNCHRONOUS_MARKER, sc);

    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0) {
            *stat = sc->strs_stat.stat2;
            if (strings) {
                *strings = sc->strs_stat.strs2;
            } else {
                deallocate_String_vector(&sc->strs_stat.strs2);
            }
        }
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_get_children(zhandle_t *zh, const char *path, int watch,
        struct String_vector *strings)
{
    return zoo_wget_children_(zh,path,watch?zh->watcher:0,zh->context,strings);
}

int zoo_wget_children(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        struct String_vector *strings)
{
    return zoo_wget_children_(zh,path,watcher,watcherCtx,strings);
}

int zoo_get_children2(zhandle_t *zh, const char *path, int watch,
        struct String_vector *strings, struct Stat *stat)
{
    return zoo_wget_children2_(zh,path,watch?zh->watcher:0,zh->context,strings,stat);
}

int zoo_wget_children2(zhandle_t *zh, const char *path,
        watcher_fn watcher, void* watcherCtx,
        struct String_vector *strings, struct Stat *stat)
{
    return zoo_wget_children2_(zh,path,watcher,watcherCtx,strings,stat);
}

int zoo_get_acl(zhandle_t *zh, const char *path, struct ACL_vector *acl,
        struct Stat *stat)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc=zoo_aget_acl(zh, path, (void (*)(int, ACL_vector*, Stat*, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
        if (rc == 0&& stat) {
            *stat = sc->acl.stat;
        }
        if (rc == 0) {
            if (acl) {
                *acl = sc->acl.acl;
            } else {
                deallocate_ACL_vector(&sc->acl.acl);
            }
        }
    }
    free_sync_completion(sc);
    return rc;
}

int zoo_set_acl(zhandle_t *zh, const char *path, int version,
        const struct ACL_vector *acl)
{
    struct sync_completion *sc = alloc_sync_completion();
    int rc;
    if (!sc) {
        return ZSYSTEMERROR;
    }
    rc=zoo_aset_acl(zh, path, version, (struct ACL_vector*)acl,
            (void (*)(int, const void*))SYNCHRONOUS_MARKER, sc);
    if(rc==ZOK){
        wait_sync_completion(sc);
        rc = sc->rc;
    }
    free_sync_completion(sc);
    return rc;
}
