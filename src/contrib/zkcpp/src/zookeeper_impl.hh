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
#include "zookeeper.hh"
#include "zookeeper.h"

namespace org { namespace apache { namespace zookeeper {

class ZooKeeperImpl {
  public:
    ZooKeeperImpl();
    ~ZooKeeperImpl();
    ReturnCode::type init(const std::string& hosts, int32_t sessionTimeoutMs,
                    boost::shared_ptr<Watch> watch);
    ReturnCode::type addAuth(const std::string& scheme, const std::string& cert,
                       boost::shared_ptr<AddAuthCallback> callback);
    ReturnCode::type create(const std::string& path, const std::string& data,
                      const std::vector<Acl>& acl, CreateMode::type mode,
                      boost::shared_ptr<CreateCallback> callback);
    ReturnCode::type create(const std::string& path, const std::string& data,
                      const std::vector<Acl>& acl, CreateMode::type mode,
                      std::string& pathCreated);
    ReturnCode::type remove(const std::string& path, int version,
                      boost::shared_ptr<RemoveCallback> callback);
    ReturnCode::type remove(const std::string& path, int version);
    ReturnCode::type exists(const std::string& path,
            boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ExistsCallback> callback);
    ReturnCode::type exists(const std::string& path, boost::shared_ptr<Watch> watch,
                            ZnodeStat& stat);
    ReturnCode::type get(const std::string& path,
                   boost::shared_ptr<Watch>,
                   boost::shared_ptr<GetCallback> callback);
    ReturnCode::type get(const std::string& path,
                         boost::shared_ptr<Watch> watch,
                         std::string& data, ZnodeStat& stat);
    ReturnCode::type set(const std::string& path, const std::string& data,
                   int version, boost::shared_ptr<SetCallback> callback);
    ReturnCode::type getChildren(const std::string& path,
                           boost::shared_ptr<Watch> watch,
                           boost::shared_ptr<GetChildrenCallback> callback);
    ReturnCode::type getAcl(const std::string& path,
                      boost::shared_ptr<GetAclCallback> callback);
    ReturnCode::type setAcl(const std::string& path, int version,
                      const std::vector<Acl>& acl,
                      boost::shared_ptr<SetAclCallback> callback);
    ReturnCode::type sync(const std::string& path,
                    boost::shared_ptr<SyncCallback> callback);
    //ReturnCode::type multi(int count, const zoo_op_t *ops,
    //      zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

    //ReturnCode::type multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);
    ReturnCode::type close();
    SessionState::type getState();
    void setState(SessionState::type state);

  private:
    static ReturnCode::type intToReturnCode(int rc);
    static void copyStat(const Stat* src, ZnodeStat& dst);
    static void watchCallback(zhandle_t *zh, int type, int state, const char *path,
                         void *watcherCtx);
    static void stringCompletion(int rc, const char *value, const void *data);
    static void removeCompletion(int rc, const void *data);
    static void setAclCompletion(int rc, const void *data);
    static void syncCompletion(int rc, const void *data);
    static void existsCompletion(int rc, const struct Stat* stat,
                                 const void* data);
    static void setCompletion(int rc, const struct Stat* stat,
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
    SessionState::type state_;
};
}}}

#endif  // SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPERIMPL_H_
