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
#include <cstring>
#include <cerrno>
#include <boost/thread/condition.hpp>
#include "ZooKeeperImpl.h"
#include "zookeeper_log.h"

namespace org { namespace apache { namespace zookeeper {

class ExistsCallback : public StatCallback {
  public:
    ExistsCallback(struct Stat* stat) : stat_(stat), completed_(false) {};
    void processResult(ReturnCode rc, std::string path, struct Stat* stat) {
      if (rc == Ok) {
        memmove(stat_, stat, sizeof(*stat));
        LOG_DEBUG(("czxid=%ld mzxid=%ld ctime=%ld mtime=%ld version=%d "
                   "cversion=%d aversion=%d ephemeralOwner=%ld dataLength=%d "
                   "numChildren=%d pzxid=%ld",
                   stat->czxid, stat->mzxid, stat->ctime, stat->mtime,
                   stat->version, stat->cversion, stat->aversion,
                   stat->ephemeralOwner, stat->dataLength, stat->numChildren,
                   stat->pzxid));
      }
      rc_ = rc;
      path_ = path;
      {
        boost::lock_guard<boost::mutex> lock(mutex_);
        completed_ = true;
      }
      cond_.notify_all();
    }

    void waitForCompleted() {
        boost::unique_lock<boost::mutex> lock(mutex_);
        while (!completed_) {
          cond_.wait(lock);
        }
    }

    boost::condition_variable cond_;
    boost::mutex mutex_;
    ReturnCode rc_;
    std::string path_;
    struct Stat* stat_;
    bool completed_;
};

class WatchContext {
public:
  WatchContext(ZooKeeperImpl* zk, boost::shared_ptr<Watch> watch, bool deleteAfterCallback) :
      zk_(zk), watch_(watch), deleteAfterCallback_(deleteAfterCallback) {};
  ZooKeeperImpl* zk_;
  boost::shared_ptr<Watch> watch_;
  bool deleteAfterCallback_;
};

void ZooKeeperImpl::
watchCallback(zhandle_t *zh, int type, int state, const char *path,
         void *watcherCtx) {
  assert(watcherCtx);
  LOG_DEBUG(("%d, %d", type, state));
  WatchContext* context = (WatchContext*)watcherCtx;

  if ((Event)type == Session) {
    bool validState = false;
    switch((State) state) {
      case Expired:
      case SessionAuthFailed:
      case Connecting:
      case Connected:
        context->zk_->setState((State)state);
        validState = true;
        break;
    }
    if (!validState) {
      fprintf(stderr, "Got unknown state: %d\n", state);
      LOG_ERROR(("Got unknown state: %d", state));
      assert(!"Got unknown state");
    }
  }

  Watch* watch = context->watch_.get();
  if (watch) {
    watch->process((Event)type, (State)state, path);
  }
  if (context->deleteAfterCallback_) {
    delete context;
  }
}

class CompletionContext {
public:
  CompletionContext(boost::shared_ptr<Callback> callback,
                  std::string path) : callback_(callback), path_(path) {};
  boost::shared_ptr<Callback> callback_;
  std::string path_;
};


void ZooKeeperImpl::
stringCompletion(int rc, const char* value, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  std::string result;
  if (rc == Ok) {
    assert(value != NULL);
    result = value;
  }
  StringCallback* callback = (StringCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_, result);
  }
  delete context;
}

void ZooKeeperImpl::
voidCompletion(int rc, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  VoidCallback* callback = (VoidCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_);
  }
  delete context;
}

void ZooKeeperImpl::
statCompletion(int rc, const struct Stat* stat,
                           const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  StatCallback* callback = (StatCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_, (struct Stat*)stat);
  }
  delete context;
}

void ZooKeeperImpl::
dataCompletion(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  std::string result;
  if (rc == Ok) {
    assert(value);
    assert(value_len >= 0);
    result.assign(value, value_len);
  }
  DataCallback* callback = (DataCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_, result,
                            (struct Stat*)stat);
  }
  delete context;

}

void ZooKeeperImpl::
childrenCompletion(int rc, const struct String_vector *strings,
                   const struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  std::vector<std::string> children;
  if (rc == Ok) {
    for (int i = 0; i < strings->count; i++) {
      children.push_back(strings->data[i]);
    }
  }
  ChildrenCallback* callback = (ChildrenCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_, children,
                            (struct Stat*)stat);
  }
  delete context;

}

void ZooKeeperImpl::
aclCompletion(int rc, struct ACL_vector *acl,
              struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  AclCallback* callback = (AclCallback*)context->callback_.get();
  if (callback) {
    callback->processResult((ReturnCode)rc, context->path_, acl,
                            (struct Stat*)stat);
  }
  delete context;
}

ZooKeeperImpl::
ZooKeeperImpl() : handle_(NULL), inited_(false), state_(Expired) {
}

ZooKeeperImpl::
~ZooKeeperImpl() {
  close();
}

ReturnCode ZooKeeperImpl::
init(const std::string& hosts, int32_t sessionTimeoutMs,
     boost::shared_ptr<Watch> watch) {
  watcher_fn watcher = NULL;
  WatchContext* context = NULL;

  if (watch.get()) {
    watcher = &watchCallback;
    context = new WatchContext(this, watch, false);
  }
  handle_ = zookeeper_init(hosts.c_str(), watcher, sessionTimeoutMs,
                           NULL, (void*)context, 0);
  if (handle_ == NULL) {
    return Error;
  }
  inited_ = true;
  return Ok;
}

ReturnCode ZooKeeperImpl::
addAuthInfo(const std::string& scheme, const std::string& cert,
                       boost::shared_ptr<VoidCallback> callback) {
  return Unimplemented;
}


ReturnCode ZooKeeperImpl::
create(const std::string& path, const std::string& data,
                  const struct ACL_vector *acl, CreateMode mode,
                  boost::shared_ptr<StringCallback> callback) {
  CompletionContext* context = new CompletionContext(callback, path);
  return (ReturnCode)zoo_acreate(handle_, path.c_str(),
                                 data.c_str(), data.size(),
                                 acl, mode, &stringCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
remove(const std::string& path, int version,
       boost::shared_ptr<VoidCallback> callback) {
  CompletionContext* context = new CompletionContext(callback, path);
  return (ReturnCode)zoo_adelete(handle_, path.c_str(), version,
         &voidCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<StatCallback> cb) {
  watcher_fn watcher = NULL;
  WatchContext* watchContext = NULL;
  stat_completion_t completion = NULL;
  CompletionContext* completionContext = NULL;

  if (cb.get()) {
    completion = &statCompletion;
    completionContext = new CompletionContext(cb, path);
  }
  if (watch.get()) {
    watcher = &watchCallback;
    watchContext = new WatchContext(this, watch, true);
  }


  return (ReturnCode)zoo_awexists(handle_, path.c_str(),
         watcher, (void*)watchContext, completion,  (void*)completionContext);
}

ReturnCode ZooKeeperImpl::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       struct Stat* stat) {
  boost::shared_ptr<ExistsCallback> callback(new ExistsCallback(stat));
  ReturnCode rc = exists(path, watch, callback);
  if (rc != Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}

ReturnCode ZooKeeperImpl::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<DataCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_awget(handle_, path.c_str(),
         &watchCallback, (void*) new WatchContext(this, watch, true),
         &dataCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
set(const std::string& path, const std::string& data,
               int version, boost::shared_ptr<StatCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_aset(handle_, path.c_str(),
         data.c_str(), data.size(), version,
         &statCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ChildrenCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_awget_children2(handle_, path.c_str(),
         &watchCallback, (void*) new WatchContext(this, watch, true),
         &childrenCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
getAcl(const std::string& path, boost::shared_ptr<AclCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_aget_acl(handle_, path.c_str(),
         &aclCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
setAcl(const std::string& path, int version, struct ACL_vector *acl,
       boost::shared_ptr<VoidCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_aset_acl(handle_, path.c_str(),
         version, acl, &voidCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
sync(const std::string& path, boost::shared_ptr<StringCallback> cb) {
  CompletionContext* context = new CompletionContext(cb, path);
  return (ReturnCode)zoo_async(handle_, path.c_str(),
         &stringCompletion, context);
}

//ReturnCode ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops,
//        zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

//ReturnCode ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);

ReturnCode ZooKeeperImpl::
setDebugLevel(ZooLogLevel level) {
  zoo_set_debug_level(level);
  return Ok;
}

ReturnCode ZooKeeperImpl::
setLogStream(FILE* logStream) {
  zoo_set_log_stream(logStream);
  return Ok;
}

ReturnCode ZooKeeperImpl::
close() {
  if (!inited_) {
    return Error;
  }
  inited_ = false;
  // XXX handle return codes.
  zookeeper_close(handle_);
  handle_ = NULL;
  return Ok;
}

State ZooKeeperImpl::
getState() {
  return state_;
}

void ZooKeeperImpl::
setState(State state) {
  state_ = state;
}

}}} // namespace org::apache::zookeeper
