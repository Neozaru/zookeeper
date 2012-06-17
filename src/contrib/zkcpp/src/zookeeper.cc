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
#include <boost/algorithm/string/join.hpp>
#include <algorithm>
#include "logging.hh"
#include "zookeeper_impl.hh"
ENABLE_LOGGING;

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
addAuth(const std::string& scheme, const std::string& cert) {
  return impl_->addAuth(scheme, cert);
}

ReturnCode::type ZooKeeper::
create(const std::string& path, const std::string& data,
       const std::vector<data::ACL>& acl, CreateMode::type mode,
       boost::shared_ptr<CreateCallback> callback) {
  return impl_->create(path, data, acl, mode, callback);
}

ReturnCode::type ZooKeeper::
create(const std::string& path, const std::string& data,
       const std::vector<data::ACL>& acl, CreateMode::type mode,
       std::string& pathCreated) {
  return impl_->create(path, data, acl, mode, pathCreated);
}

ReturnCode::type ZooKeeper::
remove(const std::string& path, int32_t version,
       boost::shared_ptr<RemoveCallback> callback) {
  return impl_->remove(path, version, callback);
}

ReturnCode::type ZooKeeper::
remove(const std::string& path, int32_t version) {
  return impl_->remove(path, version);
}

ReturnCode::type ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       boost::shared_ptr<ExistsCallback> callback) {
  return impl_->exists(path, watch, callback);
}

ReturnCode::type ZooKeeper::
exists(const std::string& path, boost::shared_ptr<Watch> watch,
       data::Stat& stat) {
  return impl_->exists(path, watch, stat);
}

ReturnCode::type ZooKeeper::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    boost::shared_ptr<GetCallback> callback) {
  return impl_->get(path, watch, callback);
}

ReturnCode::type ZooKeeper::
get(const std::string& path, boost::shared_ptr<Watch> watch,
    std::string& data, data::Stat& stat) {
  return impl_->get(path, watch, data, stat);
}

ReturnCode::type ZooKeeper::
set(const std::string& path, const std::string& data,
    int32_t version, boost::shared_ptr<SetCallback> callback) {
  return impl_->set(path, data, version, callback);
}

ReturnCode::type ZooKeeper::
set(const std::string& path, const std::string& data,
    int32_t version, data::Stat& stat) {
  return impl_->set(path, data, version, stat);
}

ReturnCode::type ZooKeeper::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            boost::shared_ptr<GetChildrenCallback> callback) {
  return impl_->getChildren(path, watch, callback);
}

ReturnCode::type ZooKeeper::
getChildren(const std::string& path, boost::shared_ptr<Watch> watch,
            std::vector<std::string>& children, data::Stat& stat) {
  return impl_->getChildren(path, watch, children, stat);
}

ReturnCode::type ZooKeeper::
getAcl(const std::string& path, boost::shared_ptr<GetAclCallback> callback) {
  return impl_->getAcl(path, callback);
}

ReturnCode::type ZooKeeper::
getAcl(const std::string& path,
       std::vector<data::ACL>& acl, data::Stat& stat) {
  return impl_->getAcl(path, acl, stat);
}

ReturnCode::type ZooKeeper::
setAcl(const std::string& path, int32_t version,
       const std::vector<data::ACL>& acl,
       boost::shared_ptr<SetAclCallback> callback) {
  return impl_->setAcl(path, version, acl, callback);
}

ReturnCode::type ZooKeeper::
setAcl(const std::string& path, int32_t version,
       const std::vector<data::ACL>& acl) {
  return impl_->setAcl(path, version, acl);
}

ReturnCode::type ZooKeeper::
sync(const std::string& path, boost::shared_ptr<SyncCallback> callback) {
  return impl_->sync(path, callback);
}

ReturnCode::type ZooKeeper::
multi(const boost::ptr_vector<Op>& ops,
      boost::shared_ptr<MultiCallback> callback) {
  return impl_->multi(ops, callback);
}

ReturnCode::type ZooKeeper::
multi(const boost::ptr_vector<Op>& ops,
      boost::ptr_vector<OpResult>& results) {
  return impl_->multi(ops, results);
}

SessionState::type ZooKeeper::
getState() {
  return impl_->getState();
}

ReturnCode::type ZooKeeper::
ZooKeeper::
close() {
  return impl_->close();
}

// Acl
class AclImpl {
  public:
    AclImpl() {
      scheme_ = "";
      expression_ = "";
      permissions_ = 0;
    }

    AclImpl(const std::string& scheme, const std::string& expression,
            int32_t permissions) :
            scheme_(scheme), expression_(expression),
            permissions_(permissions) {
    }

    bool operator==(const AclImpl& orig) const {
      return this->getScheme() == orig.getScheme() &&
             this->getExpression() == orig.getExpression() &&
             this->getPermissions() == orig.getPermissions();
    }

    const std::string getScheme() const {
      return scheme_;
    }

    void setScheme(const std::string& scheme) {
      scheme_ = scheme;
    }

    const std::string getExpression() const {
      return expression_;
    }

    void setExpression(const std::string& expression) {
      expression_ = expression;
    }

    int32_t getPermissions() const {
      return permissions_;
    }

    void setPermissions(int32_t permissions) {
      permissions_ = permissions;
    }


  private:
    std::string scheme_;
    std::string expression_;
    int32_t permissions_;
};

Acl::
Acl() : impl_(new AclImpl("", "", 0)) {
}

Acl::
Acl(const std::string& scheme, const std::string& expression,
    int32_t permissions) : impl_(new AclImpl(scheme, expression, permissions)) {
}

Acl::
Acl(const Acl& orig) : impl_(new AclImpl(orig.getScheme(), orig.getExpression(),
                       orig.getPermissions())) {
}

Acl& Acl::
operator=(const Acl& orig) {
  assert(this->impl_);
  assert(orig.impl_);
  if (this != &orig) {
    this->setScheme(orig.getScheme());
    this->setExpression(orig.getExpression());
    this->setPermissions(orig.getPermissions());
  }
  return *this;
}

bool Acl::
operator==(const Acl& orig) const {
  return *(this->impl_) == *(orig.impl_);
}

Acl::
~Acl() {
  delete impl_;
}

const std::string Acl::
getScheme() const {
  return impl_->getScheme();
}

void Acl::
setScheme(const std::string& scheme) {
  impl_->setScheme(scheme);
}

const std::string Acl::
getExpression() const {
  return impl_->getExpression();
}

void Acl::
setExpression(const std::string& expression) {
  impl_->setScheme(expression);
}

int32_t Acl::
getPermissions() const {
  return impl_->getPermissions();
}

void Acl::
setPermissions(int32_t permissions) {
  impl_->setPermissions(permissions);
}

namespace ReturnCode {

const std::string toString(type rc) {
  switch (rc) {
    case Ok:
      return "Ok";
    case SystemError:
      return "SystemError";
    case RuntimeInconsistency:
      return "RuntimeInconsistency";
    case DataInconsistency:
      return "DataInconsistency";
    case ConnectionLoss:
      return "ConnectionLoss";
    case MarshallingError:
      return "MarshallingError";
    case Unimplemented:
      return "Unimplemented";
    case OperationTimeout:
      return "OperationTimeout";
    case BadArguments:
      return "BadArguments";
    case ApiError:
      return "ApiError";
    case NoNode:
      return "NoNode";
    case NoAuth:
      return "NoAuth";
    case BadVersion:
      return "BadVersion";
    case NoChildrenForEphemerals:
      return "NoChildrenForEphemerals";
    case NodeExists:
      return "NodeExists";
    case NotEmpty:
      return "NotEmpty";
    case SessionExpired:
      return "SessionExpired";
    case InvalidCallback:
      return "InvalidCallback";
    case InvalidAcl:
      return "InvalidAcl";
    case AuthFailed:
      return "AuthFailed";
    case SessionMoved:
      return "SessionMoved";
    case CppError:
      return "CppError";
    case InvalidState:
      return "InvalidState";
    case Error:
      return "Error";
  }
  return str(boost::format("UnknownError(%d)") % rc);
}
}  // namespace ReturnCode

namespace SessionState {

const std::string toString(type state) {
  switch (state) {
    case Expired:
      return "Expired";
    case AuthFailed:
      return "AuthFailed";
    case Connecting:
      return "Connecting";
    case Connected:
      return "Connected";
  }
  LOG_ERROR("Unknown session state: " << state);
  return str(boost::format("UnknownSessionState(%d)") % state);
}

}  // namespace SessionState

namespace WatchEvent {

const std::string toString(type eventType) {
  switch (eventType) {
    case SessionStateChanged:
      return "SessionStateChanged";
    case NodeCreated:
      return "NodeCreated";
    case NodeDeleted:
      return "NodeDeleted";
    case NodeDataChanged:
      return "NodeDataChanged";
    case NodeChildrenChanged:
      return "NodeChildrenChanged";
  }
  LOG_ERROR("Unknown watch event: " << eventType);
  return str(boost::format("UnknownWatchEvent(%d)") % eventType);
}

}  // namespace WatchEvent

namespace Permission {

const std::string toString(int32_t permType) {
  std::vector<std::string> permissions;
  if (permType & Read) {
    permissions.push_back("Read");
  }
  if (permType & Write) {
    permissions.push_back("Write");
  }
  if (permType & Create) {
    permissions.push_back("Create");
  }
  if (permType & Delete) {
    permissions.push_back("Delete");
  }
  if (permType & Admin) {
    permissions.push_back("Admin");
  }
  return boost::algorithm::join(permissions, " | ");
}

}  // namespace Permission

}}}  // namespace org::apache::zookeeper
