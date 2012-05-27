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
}

ZooKeeperImpl::
ZooKeeperImpl() : handle_(NULL) {
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
    return INIT_FAILED;
  }
  return OK;
}


ZooKeeperImpl::
~ZooKeeperImpl() {
}
}}}
