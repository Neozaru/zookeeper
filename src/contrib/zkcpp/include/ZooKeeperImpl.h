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
#ifndef SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPERIMPL_H_
#define SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPERIMPL_H_

#include <stdint.h>
#include <string>
#include "ZooKeeper.h"
#include "zookeeper.h"

namespace org { namespace apache { namespace zookeeper {

class ZooKeeperImpl {
  public:
    ZooKeeperImpl();
    ~ZooKeeperImpl();
    ReturnCode init(const std::string& hosts, int32_t sessionTimeoutMs,
                    boost::shared_ptr<Watch> watch);
    ReturnCode addAuthInfo(const std::string& scheme, const std::string& cert,
                           boost::shared_ptr<AuthCallback> callback);
    ReturnCode create(const std::string& path, const std::string& data,
                      const struct ACL_vector *acl, CreateMode mode,
                      boost::shared_ptr<CreateCallback> callback);
    ReturnCode remove(const std::string& path, int version,
                      boost::shared_ptr<VoidCallback> callback);
    ReturnCode exists(const std::string& path,
            boost::shared_ptr<Watch> watch,
            boost::shared_ptr<StatCallback> callback);
    ReturnCode exists(const std::string& path, boost::shared_ptr<Watch> watch,
                      struct Stat* stat);
    ReturnCode get(const std::string& path,
                   boost::shared_ptr<Watch>,
                   boost::shared_ptr<GetCallback> callback);
    ReturnCode set(const std::string& path, const std::string& data,
                   int version, boost::shared_ptr<StatCallback> callback);
    ReturnCode getChildren(const std::string& path,
                           boost::shared_ptr<Watch> watch,
                           boost::shared_ptr<ChildrenCallback> callback);
    ReturnCode getAcl(const std::string& path,
                      boost::shared_ptr<AclCallback> callback);
    ReturnCode setAcl(const std::string& path, int version,
            struct ACL_vector *acl, boost::shared_ptr<VoidCallback> callback);
    ReturnCode sync(const std::string& path,
                    boost::shared_ptr<VoidCallback> callback);
    //ReturnCode multi(int count, const zoo_op_t *ops,
    //      zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

    //ReturnCode multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);
    ReturnCode close();
    State getState();
    void setState(State state);
    int64_t getSessionId();
    std::string getSessionPassword();

  private:
    static void watchCallback(zhandle_t *zh, int type, int state, const char *path,
                         void *watcherCtx);
    static void stringCompletion(int rc, const char *value, const void *data);
    static void voidCompletion(int rc, const void *data);
    static void statCompletion(int rc, const struct Stat* stat,
                               const void* data);
    static void dataCompletion(int rc, const char *value, int value_len,
                               const struct Stat *stat, const void *data);
    static void childrenCompletion(int rc, const struct String_vector *strings,
                                   const struct Stat *stat, const void *data);
    static void aclCompletion(int rc, struct ACL_vector *acl,
                              struct Stat *stat, const void *data);
    static void authCompletion(int rc, const void *data);
    static void syncCompletion(int rc, const char *value, const void *data);
    zhandle_t* handle_;
    bool inited_;
    State state_;
};
}}}

#endif  // SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPERIMPL_H_
