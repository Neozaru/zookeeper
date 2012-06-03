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
#include <algorithm>
#include "zookeeper.hh"
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
create(const std::string& path, const std::string& data,
       const std::vector<Acl>& acl, CreateMode::type mode,
       boost::shared_ptr<CreateCallback> callback) {
  return impl_->create(path, data, acl, mode, callback);
}

ReturnCode::type ZooKeeper::
create(const std::string& path, const std::string& data,
       const std::vector<Acl>& acl, CreateMode::type mode,
       std::string& pathCreated) {
  return impl_->create(path, data, acl, mode, pathCreated);
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
       ZnodeStat& stat) {
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
Acl(const Acl& orig) :
  impl_(new AclImpl(orig.getScheme(), orig.getExpression(),
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

class ZnodeStatImpl {
  public:
    int64_t getCzxid() {
      return czxid_;
    }

    void setCzxid(int64_t czxid) {
      czxid_ = czxid;
    }

    int64_t getMzxid() {
      return mzxid_;
    }

    void setMzxid(int64_t mzxid) {
      mzxid_ = mzxid;
    }

    int64_t getCtime() {
      return ctime_;
    }

    void setCtime(int64_t ctime) {
      ctime_ = ctime;
    }

    int64_t getMtime() {
      return mtime_;
    }

    void setMtime(int64_t mtime) {
      mtime_ = mtime;
    }

    int32_t getVersion() {
      return version_;
    }

    void setVersion(int32_t version) {
      version_ = version;
    }

    int32_t getCversion() {
      return cversion_;
    }

    void setCversion(int32_t cversion) {
      cversion_ = cversion;
    }

    int32_t getAversion() {
      return aversion_;
    }

    void setAversion(int32_t aversion) {
      aversion_ = aversion;
    }

    int64_t getEphemeralOwner() {
      return ephemeralOwner_;
    }

    void setEphemeralOwner(int64_t ephemeralOwner) {
      ephemeralOwner_ = ephemeralOwner;
    }

    int32_t getDataLength() {
      return dataLength_;
    }

    void setDataLength(int32_t dataLength) {
      dataLength_ = dataLength;
    }

    int32_t getNumChildren() {
      return numChildren_;
    }

    void setNumChildren(int32_t numChildren) {
      numChildren_ = numChildren;
    }

    int64_t getPzxid() {
      return pzxid_;
    }

    void setPzxid(int64_t pzxid) {
      pzxid_ = pzxid;
    }

  private:
    int64_t czxid_;
    int64_t mzxid_;
    int64_t ctime_;
    int64_t mtime_;
    int32_t version_;
    int32_t cversion_;
    int32_t aversion_;
    int64_t ephemeralOwner_;
    int32_t dataLength_;
    int32_t numChildren_;
    int64_t pzxid_;
};

ZnodeStat::
ZnodeStat() : impl_(new ZnodeStatImpl()) {
}

void copyStat(const ZnodeStat& src, ZnodeStat& dst) {
  dst.setCzxid(src.getCzxid());
  dst.setMzxid(src.getMzxid());
  dst.setCtime(src.getCtime());
  dst.setMtime(src.getMtime());
  dst.setVersion(src.getVersion());
  dst.setCversion(src.getCversion());
  dst.setAversion(src.getAversion());
  dst.setEphemeralOwner(src.getEphemeralOwner());
  dst.setDataLength(src.getDataLength());
  dst.setNumChildren(src.getNumChildren());
  dst.setPzxid(src.getPzxid());
}

ZnodeStat::
ZnodeStat(const ZnodeStat& orig) : impl_(new ZnodeStatImpl()) {
  copyStat(orig, *this);
}

ZnodeStat& ZnodeStat::
operator=(const ZnodeStat& orig) {
  copyStat(orig, *this);
}

ZnodeStat::
~ZnodeStat() {
  assert(impl_);
  delete impl_;
  impl_ = NULL;
}

int64_t ZnodeStat::
getCzxid() const {
  return impl_->getCzxid();
}

void ZnodeStat::
setCzxid(int64_t czxid) {
  impl_->setCzxid(czxid);
}

int64_t ZnodeStat::
getMzxid() const {
  return impl_->getMzxid();
}

void ZnodeStat::
setMzxid(int64_t mzxid) {
  impl_->setMzxid(mzxid);
}

int64_t ZnodeStat::
getCtime() const {
  return impl_->getCtime();
}

void ZnodeStat::
setCtime(int64_t ctime) {
  impl_->setCtime(ctime);
}

int64_t ZnodeStat::
getMtime() const {
  return impl_->getMtime();
}

void ZnodeStat::
setMtime(int64_t mtime) {
  impl_->setMtime(mtime);
}

int32_t ZnodeStat::
getVersion() const {
  return impl_->getVersion();
}

void ZnodeStat::
setVersion(int32_t version) {
  impl_->setVersion(version);
}

int32_t ZnodeStat::
getCversion() const {
  return impl_->getCversion();
}

void ZnodeStat::
setCversion(int32_t cversion) {
  impl_->setCversion(cversion);
}

int32_t ZnodeStat::
getAversion() const {
  return impl_->getAversion();
}

void ZnodeStat::
setAversion(int32_t aversion) {
  impl_->setAversion(aversion);
}

int64_t ZnodeStat::
getEphemeralOwner() const {
  impl_->getEphemeralOwner();
}

void ZnodeStat::
setEphemeralOwner(int64_t ephemeralOwner) {
  impl_->setEphemeralOwner(ephemeralOwner);
}

int32_t ZnodeStat::
getDataLength() const {
  return impl_->getDataLength();
}

void ZnodeStat::
setDataLength(int32_t dataLength) {
  impl_->setDataLength(dataLength);
}

int32_t ZnodeStat::
getNumChildren() const {
  return impl_->getNumChildren();
}

void ZnodeStat::
setNumChildren(int32_t numChildren) {
  impl_->setNumChildren(numChildren);
}

int64_t ZnodeStat::
getPzxid() const {
  return impl_->getPzxid();
}

void ZnodeStat::
setPzxid(int64_t pzxid) {
  impl_->setPzxid(pzxid);
}

namespace ReturnCode {

const std::string toString(type rc) {
  switch(rc) {
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
}
}  // namespace ReturnCode

namespace SessionState {

const std::string toString(type state) {
  switch(state) {
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
  switch(eventType) {
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

}}}  // namespace org::apache::zookeeper
