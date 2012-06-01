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
#include "zookeeper.hh"
#include "zookeeper_impl.hh"

namespace org { namespace apache { namespace zookeeper {

ZooKeeper::
ZooKeeper() : impl_(new ZooKeeperImpl()) {
}

ZooKeeper::
~ZooKeeper() {
  delete impl_;
}

ReturnCode::type ZooKeeper::
init(const std::string& hosts, int32_t sessionTimeoutMs,
     boost::shared_ptr<Watch> watch) {
  return impl_->init(hosts, sessionTimeoutMs, watch);
}

ReturnCode::type ZooKeeper::
addAuth(const std::string& scheme, const std::string& cert,
        boost::shared_ptr<AddAuthCallback> callback) {
  return impl_->addAuth(scheme, cert, callback);
}

ReturnCode::type ZooKeeper::
create(const std::string& path, const std::string& data,
       const std::vector<Acl>& acl, CreateMode::type mode,
       boost::shared_ptr<CreateCallback> callback) {
  return impl_->create(path, data, acl, mode, callback);
}

ReturnCode::type ZooKeeper::
remove(const std::string& path, int32_t version,
       boost::shared_ptr<RemoveCallback> callback) {
  return impl_->remove(path, version, callback);
}

ReturnCode::type ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<ExistsCallback> callback) {
  return impl_->exists(path, watch, callback);
}

ReturnCode::type ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       Stat& stat) {
  return impl_->exists(path, watch, stat);
}

ReturnCode::type ZooKeeper::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<GetCallback> callback) {
  return impl_->get(path, watch, callback);
}

ReturnCode::type ZooKeeper::
set(const std::string& path, const std::string& data,
    int32_t version, boost::shared_ptr<SetCallback> callback) {
  return impl_->set(path, data, version, callback);
}

ReturnCode::type ZooKeeper::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<GetChildrenCallback> callback) {
  return impl_->getChildren(path, watch, callback);
}

ReturnCode::type ZooKeeper::
getAcl(const std::string& path, boost::shared_ptr<GetAclCallback> callback) {
  return impl_->getAcl(path, callback);
}

ReturnCode::type ZooKeeper::
setAcl(const std::string& path, int32_t version, const std::vector<Acl>& acl,
       boost::shared_ptr<SetAclCallback> callback) {
  return impl_->setAcl(path, version, acl, callback);
}

ReturnCode::type ZooKeeper::
sync(const std::string& path, boost::shared_ptr<SyncCallback> callback) {
  return impl_->sync(path, callback);
}

//ReturnCode::type ZooKeeper::
//multi(int count, const zoo_op_t *ops,
//        zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

//ReturnCode::type ZooKeeper::
//multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);

SessionState::type ZooKeeper::
getState() {
  return impl_->getState();
}

ReturnCode::type ZooKeeper::
ZooKeeper::
close() {
  return impl_->close();
}

}}}  // namespace org::apache::zookeeper
