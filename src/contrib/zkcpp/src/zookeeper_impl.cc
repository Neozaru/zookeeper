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
#include "zookeeper_impl.hh"
#include "logging.hh"
ENABLE_LOGGING;

namespace org { namespace apache { namespace zookeeper {

class Waitable {
  public:
    Waitable() : completed_(false) {}

    void notifyCompleted() {
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

  private:
    boost::condition_variable cond_;
    boost::mutex mutex_;
    bool completed_;
};

class MyAddAuthCallback : public AddAuthCallback, public Waitable {
  public:
    MyAddAuthCallback(const std::string& scheme, const std::string& cert) :
                     scheme_(scheme), cert_(cert) {}

    void process(ReturnCode::type rc,
                const std::string& scheme,
                const std::string& cert) {
      assert(scheme == scheme_);
      assert(cert == cert_);
      rc_ = rc;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    const std::string& scheme_;
    const std::string& cert_;
};

class MyCreateCallback : public CreateCallback, public Waitable {
  public:
    MyCreateCallback(const std::string& pathRequested,
                     std::string& pathCreated) :
                     pathRequested_(pathRequested),
                     pathCreated_(pathCreated) {
    }

    void process(ReturnCode::type rc,
                const std::string& pathRequested,
                const std::string& pathCreated) {
      LOG_DEBUG(boost::format("rc=%s pathRequested='%s' pathCreated='%s'") %
                ReturnCode::toString(rc) % pathRequested % pathCreated);
      assert(pathRequested_ == pathRequested);
      rc_ = rc;
      if (rc == ReturnCode::Ok) {
        pathCreated_ = pathCreated;
      }
      notifyCompleted();
    }

    ReturnCode::type rc_;
    const std::string& pathRequested_;
    std::string& pathCreated_;
};

class MyExistsCallback : public ExistsCallback, public Waitable {
  public:
    MyExistsCallback(ZnodeStat& stat) : stat_(stat) {};
    void process(ReturnCode::type rc, const std::string& path,
                 const ZnodeStat& stat) {
      if (rc == ReturnCode::Ok) {
        stat_ = stat;
      }
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
    ZnodeStat& stat_;
};

class MyGetCallback : public GetCallback, public Waitable {
  public:
    MyGetCallback(std::string& data, ZnodeStat& stat) :
      data_(data), stat_(stat) {};
    void process(ReturnCode::type rc, const std::string& path,
                 const std::string& data, const ZnodeStat& stat) {
      if (rc == ReturnCode::Ok) {
        data_ = data;
        stat_ = stat;
      }
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
    std::string& data_;
    ZnodeStat& stat_;
};

class MyGetAclCallback : public GetAclCallback, public Waitable {
  public:
    MyGetAclCallback(std::vector<Acl>& acl, ZnodeStat& stat) :
      acl_(acl), stat_(stat) {};

    void process(ReturnCode::type rc, const std::string& path,
                 const std::vector<Acl>& acl,
                 const ZnodeStat& stat) {
      if (rc == ReturnCode::Ok) {
        acl_ = acl;
        stat_ = stat;
      }
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
    std::vector<Acl>& acl_;
    ZnodeStat& stat_;
};

class MySetAclCallback : public SetAclCallback, public Waitable {
  public:
    MySetAclCallback() {}

    void process(ReturnCode::type rc, const std::string& path) {
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
};

class MyGetChildrenCallback : public GetChildrenCallback, public Waitable {
  public:
    MyGetChildrenCallback(std::vector<std::string>& children, ZnodeStat& stat) :
      children_(children), stat_(stat) {};

    void process(ReturnCode::type rc, const std::string& path,
                 const std::vector<std::string>& children,
                 const ZnodeStat& stat) {
      if (rc == ReturnCode::Ok) {
        children_ = children;
        stat_ = stat;
      }
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
    std::vector<std::string>& children_;
    ZnodeStat& stat_;
};


class MySetCallback : public SetCallback, public Waitable {
  public:
    MySetCallback(ZnodeStat& stat) : stat_(stat) {};
    void process(ReturnCode::type rc, const std::string& path,
                 const ZnodeStat& stat) {
      if (rc == ReturnCode::Ok) {
        stat_ = stat;
      }
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
    ZnodeStat& stat_;
};

class MyRemoveCallback : public RemoveCallback, public Waitable {
  public:
    MyRemoveCallback() {};
    void process(ReturnCode::type rc, const std::string& path) {
      rc_ = rc;
      path_ = path;
      notifyCompleted();
    }

    ReturnCode::type rc_;
    std::string path_;
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
  WatchContext* context = (WatchContext*)watcherCtx;
  WatchEvent::type eventType = (WatchEvent::type)type;
  SessionState::type stateType = (SessionState::type)state;

  LOG_DEBUG(boost::format("Got a watch event: type=%s, state=%s, path='%s'") %
            WatchEvent::toString(eventType) %
            SessionState::toString(stateType) % path);
  if (eventType == WatchEvent::SessionStateChanged) {
    bool validState = false;
    switch((SessionState::type) state) {
      case SessionState::Expired:
      case SessionState::AuthFailed:
      case SessionState::Connecting:
      case SessionState::Connected:
        context->zk_->setState((SessionState::type)state);
        validState = true;
        break;
    }
    if (!validState) {
      fprintf(stderr, "Got unknown state: %d\n", state);
      LOG_ERROR("Got unknown state: " << state);
      assert(!"Got unknown state");
    }
  }

  Watch* watch = context->watch_.get();
  if (watch) {
    watch->process((WatchEvent::type)type, (SessionState::type)state, path);
  }
  if (context->deleteAfterCallback_) {
    delete context;
  }
}

ReturnCode::type ZooKeeperImpl::
intToReturnCode(int rc) {
  /**
   * ZINVALIDSTATE is a C client specific error, but it was put under system
   * error range (between -1 and -100). To avoid future collision, I moved it
   * to C++ error range (greater than 0).
   */
  if (rc == ZINVALIDSTATE) {
    return ReturnCode::InvalidState;
  }
  return (ReturnCode::type)rc;
}

void ZooKeeperImpl::
copyStat(const Stat* src, ZnodeStat& dst) {
  if (!src) {
    return;
  }
        LOG_DEBUG(
          boost::format("czxid=%ld mzxid=%ld ctime=%ld mtime=%ld version=%d "
                        "cversion=%d aversion=%d ephemeralOwner=%ld "
                        "dataLength=%d numChildren=%d pzxid=%ld") %
                        src->czxid % src->mzxid % src->ctime % src->mtime %
                        src->version % src->cversion % src->aversion %
                        src->ephemeralOwner % src->dataLength %
                        src->numChildren % src->pzxid);

  dst.setCzxid(src->czxid);
  dst.setMzxid(src->mzxid);
  dst.setCtime(src->ctime);
  dst.setMtime(src->mtime);
  dst.setVersion(src->version);
  dst.setCversion(src->cversion);
  dst.setAversion(src->aversion);
  dst.setEphemeralOwner(src->ephemeralOwner);
  dst.setDataLength(src->dataLength);
  dst.setNumChildren(src->numChildren);
  dst.setPzxid(src->pzxid);
}

class CompletionContext {
public:
  CompletionContext(boost::shared_ptr<void> callback,
                  std::string path) : callback_(callback), path_(path) {};
  boost::shared_ptr<void> callback_;
  std::string path_;
};

class AuthCompletionContext {
public:
  AuthCompletionContext(boost::shared_ptr<void> callback,
                        const std::string& scheme,
                        const std::string& cert) :
    callback_(callback), scheme_(scheme), cert_(cert) {}
  boost::shared_ptr<void> callback_;
  std::string scheme_;
  std::string cert_;
};


void ZooKeeperImpl::
stringCompletion(int rc, const char* value, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  std::string result;
  if (rc == ReturnCode::Ok) {
    assert(value != NULL);
    result = value;
  }
  CreateCallback* callback = (CreateCallback*)context->callback_.get();
  if (callback) {
    callback->process((ReturnCode::type)rc, context->path_, result);
  }
  delete context;
}

void ZooKeeperImpl::
removeCompletion(int rc, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  RemoveCallback* callback = (RemoveCallback*)context->callback_.get();
  if (callback) {
    callback->process((ReturnCode::type)rc, context->path_);
  }
  delete context;
}

void ZooKeeperImpl::
setAclCompletion(int rc, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  SetAclCallback* callback = (SetAclCallback*)context->callback_.get();
  if (callback) {
    callback->process((ReturnCode::type)rc, context->path_);
  }
  delete context;
}

void ZooKeeperImpl::
existsCompletion(int rc, const struct Stat* stat,
                           const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  ExistsCallback* callback = (ExistsCallback*)context->callback_.get();
  if (callback) {
    ZnodeStat statObject;
    copyStat(stat, statObject);
    callback->process((ReturnCode::type)rc, context->path_, statObject);
  }
  delete context;
}

void ZooKeeperImpl::
setCompletion(int rc, const struct Stat* stat,
                           const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  SetCallback* callback = (SetCallback*)context->callback_.get();
  if (callback) {
    ZnodeStat statObject;
    copyStat(stat, statObject);
    callback->process((ReturnCode::type)rc, context->path_, statObject);
  }
  delete context;
}

void ZooKeeperImpl::
dataCompletion(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  std::string result;
  ZnodeStat statCopy;
  if (rc == ReturnCode::Ok) {
    assert(value);
    assert(value_len >= 0);
    assert(stat);
  }
  GetCallback* callback = (GetCallback*)context->callback_.get();
  if (callback) {
    // TODO avoid copy
    result.assign(value, value_len);
    copyStat(stat, statCopy);
    callback->process((ReturnCode::type)rc, context->path_, result, statCopy);
  }
  delete context;

}

void ZooKeeperImpl::
childrenCompletion(int rc, const struct String_vector *strings,
                   const struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  LOG_DEBUG("getChildren() for " << context->path_ << " returned: " <<
            ReturnCode::toString((ReturnCode::type)rc));
  GetChildrenCallback* callback =
    (GetChildrenCallback*)context->callback_.get();
  assert(callback);
  std::vector<std::string> children;
  if (rc == ReturnCode::Ok) {
    assert(strings);
    for (int i = 0; i < strings->count; i++) {
      children.push_back(strings->data[i]);
      LOG_DEBUG("Got a child for " << context->path_ << ": " <<
                strings->data[i]);
    }
  }
  // TODO avoid copy
  ZnodeStat statObject;
  copyStat(stat, statObject);
  callback->process((ReturnCode::type)rc, context->path_, children,
                    statObject);
  delete context;

}

void ZooKeeperImpl::
aclCompletion(int rc, struct ACL_vector *acl,
              struct Stat *stat, const void *data) {
  CompletionContext* context = (CompletionContext*)data;
  LOG_DEBUG("getAcl() for " << context->path_ << " returned: " <<
            ReturnCode::toString((ReturnCode::type)rc));
  GetAclCallback* callback = (GetAclCallback*)context->callback_.get();
  assert(callback);
  std::vector<Acl> aclVector;
  if (rc == ReturnCode::Ok) {
    assert(acl);
    for (int i = 0; i < acl->count; i++) {
      aclVector.push_back(Acl(acl->data[i].id.scheme,
                              acl->data[i].id.id,
                              acl->data[i].perms));
      LOG_DEBUG(
        boost::format("Got an ACL for %s: %s:%s:'%s'") %
                      context->path_ % acl->data[i].id.scheme %
                      acl->data[i].id.id %
                      Permission::toString(acl->data[i].perms));
    }
  }
  ZnodeStat statObject;
  copyStat(stat, statObject);
  callback->process((ReturnCode::type)rc, context->path_,
                     aclVector, statObject);
  delete context;
}

void ZooKeeperImpl::
authCompletion(int rc, const void* data) {
  AuthCompletionContext* context = (AuthCompletionContext*)data;
  AddAuthCallback* callback = (AddAuthCallback*)context->callback_.get();
  LOG_DEBUG(boost::format("rc=%d, scheme='%s', cert='%s'") % rc %
                          context->scheme_.c_str() % context->cert_.c_str());
  assert(callback);
  callback->process((ReturnCode::type)rc, context->scheme_, context->cert_);
  delete context;
}

void ZooKeeperImpl::
syncCompletion(int rc, const char* value, const void* data) {
  CompletionContext* context = (CompletionContext*)data;
  std::string result;
  SyncCallback* callback = (SyncCallback*)context->callback_.get();
  if (callback) {
    callback->process((ReturnCode::type)rc, context->path_);
  }
  delete context;
}


ZooKeeperImpl::
ZooKeeperImpl() : handle_(NULL), inited_(false), state_(SessionState::Expired) {
}

ZooKeeperImpl::
~ZooKeeperImpl() {
  close();
}

ReturnCode::type ZooKeeperImpl::
init(const std::string& hosts, int32_t sessionTimeoutMs,
     boost::shared_ptr<Watch> watch) {
  watcher_fn watcher = NULL;
  WatchContext* context = NULL;

  watcher = &watchCallback;
  context = new WatchContext(this, watch, false);
  handle_ = zookeeper_init(hosts.c_str(), watcher, sessionTimeoutMs,
                           NULL, (void*)context, 0);
  if (handle_ == NULL) {
    return ReturnCode::Error;
  }
  inited_ = true;
  return ReturnCode::Ok;
}

ReturnCode::type ZooKeeperImpl::
addAuth(const std::string& scheme, const std::string& cert,
        boost::shared_ptr<AddAuthCallback> callback) {
  void_completion_t completion = NULL;
  AuthCompletionContext* context = NULL;
  if (callback.get()) {
    completion = &authCompletion;
    context = new AuthCompletionContext(callback, scheme, cert);
  }
  int rc = zoo_add_auth(handle_, scheme.c_str(), cert.c_str(),
                        cert.size(), completion, (void*)context);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
addAuth(const std::string& scheme, const std::string& cert) {
  boost::shared_ptr<MyAddAuthCallback> callback(
    new MyAddAuthCallback(scheme, cert));
  ReturnCode::type rc = addAuth(scheme, cert, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}


ReturnCode::type ZooKeeperImpl::
create(const std::string& path, const std::string& data,
                  const std::vector<Acl>& acl, CreateMode::type mode,
                  boost::shared_ptr<CreateCallback> callback) {
  if ((int)this->getState() < 0) {
    return ReturnCode::InvalidState;
  }

  string_completion_t completion = NULL;
  CompletionContext* context = NULL;
  ACL acls[acl.size()];
  ACL_vector aclVector;
  aclVector.count = acl.size();
  for (int i = 0; i < acl.size(); i++) {
    acls[i].id.scheme = (char*) acl[i].getScheme().c_str();
    acls[i].id.id = (char*) acl[i].getExpression().c_str();
    acls[i].perms = acl[i].getPermissions();
  }
  aclVector.data = acls;

  if (callback.get()) {
    completion = &stringCompletion;
    context = new CompletionContext(callback, path);
  }
  int rc = zoo_acreate(handle_, path.c_str(), data.c_str(), data.size(),
                       &aclVector, mode, completion, (void*)context);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
create(const std::string& path, const std::string& data,
       const std::vector<Acl>& acl, CreateMode::type mode,
       std::string& pathCreated) {
  boost::shared_ptr<MyCreateCallback> callback(
    new MyCreateCallback(path, pathCreated));
  ReturnCode::type rc = create(path, data, acl, mode, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
remove(const std::string& path, int32_t version,
       boost::shared_ptr<RemoveCallback> callback) {
  void_completion_t completion = NULL;
  CompletionContext* context = NULL;
  if (callback.get()) {
    completion = &removeCompletion;
    context = new CompletionContext(callback, path);
  }
  int rc = zoo_adelete(handle_, path.c_str(), version,
         completion, (void*)context);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
remove(const std::string& path, int32_t version) {
  boost::shared_ptr<MyRemoveCallback> callback(new MyRemoveCallback());
  ReturnCode::type rc = remove(path, version, callback);
 if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<ExistsCallback> cb) {
  watcher_fn watcher = NULL;
  WatchContext* watchContext = NULL;
  stat_completion_t completion = NULL;
  CompletionContext* completionContext = NULL;

  if (cb.get()) {
    completion = &existsCompletion;
    completionContext = new CompletionContext(cb, path);
  }
  if (watch.get()) {
    watcher = &watchCallback;
    watchContext = new WatchContext(this, watch, true);
  }

  int rc = zoo_awexists(handle_, path.c_str(), watcher, (void*)watchContext,
                        completion,  (void*)completionContext);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       ZnodeStat& stat) {
  boost::shared_ptr<MyExistsCallback> callback(new MyExistsCallback(stat));
  ReturnCode::type rc = exists(path, watch, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<GetCallback> cb) {
  watcher_fn watcher = NULL;
  WatchContext* watchContext = NULL;
  data_completion_t completion = NULL;
  CompletionContext* context = NULL;

  if (watch.get()) {
    watcher = &watchCallback;
    watchContext = new WatchContext(this, watch, true);
  }
  if (cb.get()) {
    completion = &dataCompletion;
    context = new CompletionContext(cb, path);
  }

  int rc = zoo_awget(handle_, path.c_str(), watcher, (void*)watchContext,
                    completion, (void*)context);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    std::string& data, ZnodeStat& stat) {
  boost::shared_ptr<MyGetCallback> callback(new MyGetCallback(data, stat));
  ReturnCode::type rc = get(path, watch, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
set(const std::string& path, const std::string& data,
               int32_t version, boost::shared_ptr<SetCallback> cb) {
  stat_completion_t completion = NULL;
  CompletionContext* context = NULL;
  if (cb.get()) {
    completion = &setCompletion;
    context = new CompletionContext(cb, path);
  }

  int rc = zoo_aset(handle_, path.c_str(), data.c_str(), data.size(), version,
                    completion, (void*)context);
  return intToReturnCode(rc);
}


ReturnCode::type ZooKeeperImpl::
set(const std::string& path, const std::string& data,
    int32_t version, ZnodeStat& stat) {
  boost::shared_ptr<MySetCallback> callback(new MySetCallback(stat));
  ReturnCode::type rc = set(path, data, version, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  assert(path == callback->path_);
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<GetChildrenCallback> cb) {
  watcher_fn watcher = NULL;
  WatchContext* watchContext = NULL;
  strings_stat_completion_t completion = NULL;
  CompletionContext* context = NULL;

  if (watch.get()) {
    watcher = &watchCallback;
    watchContext = new WatchContext(this, watch, true);
  }
  if (cb.get()) {
    completion = &childrenCompletion;
    context = new CompletionContext(cb, path);
  }

  int rc = zoo_awget_children2(handle_, path.c_str(), watcher,
                               (void*)watchContext, completion, (void*)context);
  return intToReturnCode(rc);
}

ReturnCode::type ZooKeeperImpl::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            std::vector<std::string>& children, ZnodeStat& stat) {
  boost::shared_ptr<MyGetChildrenCallback>
    callback(new MyGetChildrenCallback(children, stat));
  ReturnCode::type rc = getChildren(path, watch, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  assert(path == callback->path_);
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
getAcl(const std::string& path, boost::shared_ptr<GetAclCallback> cb) {
  acl_completion_t completion = NULL;
  CompletionContext* context = NULL;
  if (cb.get()) {
    completion = &aclCompletion;
    context = new CompletionContext(cb, path);
  }
  int rc = zoo_aget_acl(handle_, path.c_str(), completion, (void*)context);
  return intToReturnCode(rc);
}


ReturnCode::type ZooKeeperImpl::
getAcl(const std::string& path,
       std::vector<Acl>& acl, ZnodeStat& stat) {
  boost::shared_ptr<MyGetAclCallback> callback(new MyGetAclCallback(acl, stat));
  ReturnCode::type rc = getAcl(path, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  assert(path == callback->path_);
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
setAcl(const std::string& path, int32_t version, const std::vector<Acl>& acl,
       boost::shared_ptr<SetAclCallback> cb) {
  void_completion_t completion = NULL;
  CompletionContext* context = NULL;
  ACL acls[acl.size()];
  ACL_vector aclVector;
  aclVector.count = acl.size();
  for (int i = 0; i < acl.size(); i++) {
    acls[i].id.scheme = (char*) acl[i].getScheme().c_str();
    acls[i].id.id = (char*) acl[i].getExpression().c_str();
    acls[i].perms = acl[i].getPermissions();
  }
  aclVector.data = acls;

  if (cb.get()) {
    completion = &setAclCompletion;
    context = new CompletionContext(cb, path);
  }

  return (ReturnCode::type)zoo_aset_acl(handle_, path.c_str(),
         version, &aclVector, completion, (void*)context);
}

ReturnCode::type ZooKeeperImpl::
setAcl(const std::string& path, int32_t version, const std::vector<Acl>& acl) {
  boost::shared_ptr<MySetAclCallback> callback(new MySetAclCallback());
  ReturnCode::type rc = setAcl(path, version, acl, callback);
  if (rc != ReturnCode::Ok) {
    return rc;
  }
  callback->waitForCompleted();
  assert(path == callback->path_);
  return callback->rc_;
}

ReturnCode::type ZooKeeperImpl::
sync(const std::string& path, boost::shared_ptr<SyncCallback> cb) {
  string_completion_t completion = NULL;
  CompletionContext* context = NULL;
  if (cb.get()) {
    completion = &syncCompletion;
    context = new CompletionContext(cb, path);
  }
  return (ReturnCode::type)zoo_async(handle_, path.c_str(),
         completion, context);
}

//ReturnCode::type ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops,
//        zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

//ReturnCode::type ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);

ReturnCode::type ZooKeeperImpl::
close() {
  if (!inited_) {
    return ReturnCode::Error;
  }
  inited_ = false;
  // XXX handle return codes.
  zookeeper_close(handle_);
  handle_ = NULL;
  return ReturnCode::Ok;
}

SessionState::type ZooKeeperImpl::
getState() {
  return state_;
}

void ZooKeeperImpl::
setState(SessionState::type state) {
  state_ = state;
}

}}} // namespace org::apache::zookeeper
