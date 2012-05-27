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
#include <cerrno>
#include "ZooKeeperImpl.h"

namespace org { namespace apache { namespace zookeeper {

void ZooKeeperImpl::
callback(zhandle_t *zh, int type, int state, const char *path,
         void *watcherCtx) {
  assert(watcherCtx);
  printf("%d, %d\n", type, state);
  boost::shared_ptr<Watch>* watch = (boost::shared_ptr<Watch>*)watcherCtx;
  watch->get()->process((Event)type, (State)state, path);
  delete watch;
}

class CallbackContext {
public:
  CallbackContext(boost::shared_ptr<Callback> callback,
                  std::string path) : callback_(callback), path_(path) {};
  boost::shared_ptr<Callback> callback_;
  std::string path_;
};


void ZooKeeperImpl::
stringCompletion(int rc, const char* value, const void* data) {
  CallbackContext* context = (CallbackContext*)data;
  std::string result;
  if (rc == Ok) {
    assert(value != NULL);
    result = value;
  }
  ((StringCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_, result);
  delete context;
}

void ZooKeeperImpl::
voidCompletion(int rc, const void* data) {
  CallbackContext* context = (CallbackContext*)data;
  ((VoidCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_);
  delete context;
}

void ZooKeeperImpl::
statCompletion(int rc, const struct Stat* stat,
                           const void* data) {
  CallbackContext* context = (CallbackContext*)data;
  ((StatCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_, (struct Stat*)stat);
  delete context;
}

void ZooKeeperImpl::
dataCompletion(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data) {
  CallbackContext* context = (CallbackContext*)data;
  std::string result;
  if (rc == Ok) {
    assert(value);
    assert(value_len >= 0);
    result.assign(value, value_len);
  }
  ((DataCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_, result, (struct Stat*)stat);
  delete context;

}

void ZooKeeperImpl::
childrenCompletion(int rc, const struct String_vector *strings,
                   const struct Stat *stat, const void *data) {
  CallbackContext* context = (CallbackContext*)data;
  std::vector<std::string> children;
  if (rc == Ok) {
    for (int i = 0; i < strings->count; i++) {
      children.push_back(strings->data[i]);
    }
  }
  ((ChildrenCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_, children, (struct Stat*)stat);
  delete context;

}

void ZooKeeperImpl::
aclCompletion(int rc, struct ACL_vector *acl,
              struct Stat *stat, const void *data) {
  CallbackContext* context = (CallbackContext*)data;
  ((AclCallback*)(context->callback_.get()))->processResult((ReturnCode)rc,
                    context->path_, acl, (struct Stat*)stat);
  delete context;
}

ZooKeeperImpl::
ZooKeeperImpl() : handle_(NULL), inited_(false) {
}

ZooKeeperImpl::
~ZooKeeperImpl() {
  close();
}

ReturnCode ZooKeeperImpl::
init(const std::string& hosts, int32_t sessionTimeoutMs,
     boost::shared_ptr<Watch> watch) {

  // XXX HACK - heap allocate a copy of the watch and save it in zookeeper
  // handle
  boost::shared_ptr<Watch>* callbackWatch = new boost::shared_ptr<Watch>(watch);
  handle_ = zookeeper_init(hosts.c_str(), &callback, sessionTimeoutMs,
                           NULL, (void*)callbackWatch, 0);
  if (handle_ == NULL) {
    return Error;
  }
  inited_ = true;
  defaultWatch_ = watch;
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
  CallbackContext* context = new CallbackContext(callback, path);
  return (ReturnCode)zoo_acreate(handle_, path.c_str(),
                                 data.c_str(), data.size(),
                                 acl, mode, &stringCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
remove(const std::string& path, int version,
       boost::shared_ptr<VoidCallback> callback) {
  CallbackContext* context = new CallbackContext(callback, path);
  return (ReturnCode)zoo_adelete(handle_, path.c_str(), version,
         &voidCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<StatCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_awexists(handle_, path.c_str(),
         &callback, (void*) new boost::shared_ptr<Watch>(watch),
         &statCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<DataCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_awget(handle_, path.c_str(),
         &callback, (void*) new boost::shared_ptr<Watch>(watch),
         &dataCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
set(const std::string& path, const std::string& data,
               int version, boost::shared_ptr<StatCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_aset(handle_, path.c_str(),
         data.c_str(), data.size(), version,
         &statCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ChildrenCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_awget_children2(handle_, path.c_str(),
         &callback, (void*)new boost::shared_ptr<Watch>(watch),
         &childrenCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
getAcl(const std::string& path, boost::shared_ptr<AclCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_aget_acl(handle_, path.c_str(),
         &aclCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
setAcl(const std::string& path, int version, struct ACL_vector *acl,
       boost::shared_ptr<VoidCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_aset_acl(handle_, path.c_str(),
         version, acl, &voidCompletion, (void*)context);
}

ReturnCode ZooKeeperImpl::
sync(const std::string& path, boost::shared_ptr<StringCallback> cb) {
  CallbackContext* context = new CallbackContext(cb, path);
  return (ReturnCode)zoo_async(handle_, path.c_str(),
         &stringCompletion, context);
}

//ReturnCode ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops,
//        zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

//ReturnCode ZooKeeperImpl::
//multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);

ReturnCode ZooKeeperImpl::
setDebugLevel(LogLevel logLevel) {
  zoo_set_debug_level((ZooLogLevel)logLevel);
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

}}} // namespace org::apache::zookeeper
