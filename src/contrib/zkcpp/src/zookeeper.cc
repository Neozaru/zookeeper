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

ReturnCode ZooKeeper::
init(const std::string& hosts, int32_t sessionTimeoutMs,
     boost::shared_ptr<Watch> watch) {
  return impl_->init(hosts, sessionTimeoutMs, watch);
}

ReturnCode ZooKeeper::
addAuthInfo(const std::string& scheme, const std::string& cert,
            boost::shared_ptr<AuthCallback> callback) {
  return impl_->addAuthInfo(scheme, cert, callback);
}

ReturnCode ZooKeeper::
create(const std::string& path, const std::string& data,
       const struct ACL_vector *acl, CreateMode mode,
       boost::shared_ptr<CreateCallback> callback) {
  return impl_->create(path, data, acl, mode, callback);
}

ReturnCode ZooKeeper::
remove(const std::string& path, int version,
       boost::shared_ptr<VoidCallback> callback) {
  return impl_->remove(path, version, callback);
}

ReturnCode ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<StatCallback> callback) {
  return impl_->exists(path, watch, callback);
}

ReturnCode ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       struct Stat* stat) {
  return impl_->exists(path, watch, stat);
}

ReturnCode ZooKeeper::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<GetCallback> callback) {
  return impl_->get(path, watch, callback);
}

ReturnCode ZooKeeper::
set(const std::string& path, const std::string& data,
    int version, boost::shared_ptr<StatCallback> callback) {
  return impl_->set(path, data, version, callback);
}

ReturnCode ZooKeeper::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ChildrenCallback> callback) {
  return impl_->getChildren(path, watch, callback);
}

ReturnCode ZooKeeper::
getAcl(const std::string& path, boost::shared_ptr<GetAclCallback> callback) {
  return impl_->getAcl(path, callback);
}

ReturnCode ZooKeeper::
setAcl(const std::string& path, int version, struct ACL_vector *acl,
       boost::shared_ptr<VoidCallback> callback) {
  return impl_->setAcl(path, version, acl, callback);
}

ReturnCode ZooKeeper::
sync(const std::string& path, boost::shared_ptr<VoidCallback> callback) {
  return impl_->sync(path, callback);
}

//ReturnCode ZooKeeper::
//multi(int count, const zoo_op_t *ops,
//        zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);

//ReturnCode ZooKeeper::
//multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);

State ZooKeeper::
getState() {
  return impl_->getState();
}

ReturnCode ZooKeeper::
ZooKeeper::
close() {
  return impl_->close();
}

}}}  // namespace org::apache::zookeeper
