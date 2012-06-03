/*
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

#ifndef SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_
#define SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include "zookeeper.h"
#include "zookeeper.jute.h"

namespace org {

namespace apache {

/** ZooKeeper namespace. */
namespace zookeeper {

/**
 * ZooKeeper return codes.
 */
namespace ReturnCode {
  enum type {
    /** Everything is OK */
    Ok = 0,

    /**
     * System and server-side errors.
     *
     * SystemError is never returned by ZooKeeper. It is meant to be used
     * to indicate a range. Specifically error codes less than this value,
     * but greater than {@link #ApiError}, are system errors.
     */
    SystemError = -1,

    /** A runtime inconsistency was found */
    RuntimeInconsistency = -2,

    /** A data inconsistency was found */
    DataInconsistency = -3,

    /** Connection to the server has been lost */
    ConnectionLoss = -4,

    /** Error while marshalling or unmarshalling data */
    MarshallingError = -5,

    /** Operation is unimplemented */
    Unimplemented = -6,

    /** Operation timeout */
    OperationTimeout = -7,

    /** Invalid arguments */
    BadArguments = -8,

    /**
     * API errors.
     *
     * ApiError is never returned by the ZooKeeper. It is meant to be used
     * to indicate a range. Specifically error codes less than this
     * value are API errors (while values greater than this and less than
     * {@link #SystemError} indicate a {@link #SystemError}).
     */
    ApiError = -100,

    /** Node does not exist */
    NoNode = -101,

    /** Not authenticated */
    NoAuth = -102,

    /** Version conflict */
    BadVersion = -103,

    /** Ephemeral nodes may not have children */
    NoChildrenForEphemerals = -108,

    /** The node already exists */
    NodeExists = -110,

    /** The node has children */
    NotEmpty = -111,

    /** The session has been expired by the server */
    SessionExpired = -112,

    /** Invalid callback specified */
    InvalidCallback = -113,

    /** Invalid ACL specified */
    InvalidAcl = -114,

    /** Client authentication failed */
    AuthFailed = -115,

    /** Session moved to another server, so operation is ignored */
    SessionMoved = -118,

    /**
     * C++ library specific errors.
     *
     * CppError is never returned by the ZooKeeper. It is meant to be used
     * to indicate a range. Specifically error codes greater than this
     * value are C++ library specific errors.
     */
    CppError = 1,

    /** The session is in an invalid state for a given operation. */
    InvalidState = 2,

    /** Generic error */
    Error = 3,
  };

  const std::string toString(type rc);
};

/**
 * Namespace for session state enums.
 */
namespace SessionState {
  /**
   * ZooKeeper session states.
   *
   * A positive value indicates that the session is in a "recoverable" state.
   * A negative value indicates that the session is in an "unrecoverable" state.
   * Once the client is in an unrecoverable state, the session is no longer
   * valid. You need to call ZooKeeper::init() to establish a new session.
   *
   * For more details about recoverable and unrecoverable states, see:
   *
   * http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling
   */
  enum type {
    /**
     * The session has been expired and is no longer valid.
     */
    Expired = -112,

    /**
     * Session Authentication failed.
     */
    AuthFailed = -113,

    /**
     * The session is not connected to any ZooKeeper server.
     */
    Connecting = 1,

    /**
     * The session is connected to a ZooKeeper server.
     */
    Connected = 3,
  };

  /**
   * Returns SessionState::type enum in human-readable string format.
   */
  const std::string toString(type state);
};

/**
 * These constants indicate the event that caused a watch to trigger. They
 * are possible values of the first parameter of the watcher callback.
 */
namespace WatchEvent {
  enum type {
    /**
     * Session state has changed.
     *
     * This is generated when a client loses contact or reconnects with a
     * server.
     */
    SessionStateChanged = -1,

    /**
     * Node has been created.
     *
     * This event is triggered for watches that were set using
     * ZooKeeper::exists(), but not for those that were set using
     * ZooKeeper::get() or ZooKeeper::getChildren().
     */
    NodeCreated = 1,

    /**
     * Node has been deleted.
     *
     * This event is triggered for watches that were set using any of
     * ZooKeeper::exists(), ZooKeeper::get(), or ZooKeeper::getChildren().
     */
    NodeDeleted = 2,

    /**
     * Node data has changed.
     *
     * This event is triggered for watches that were set using
     * ZooKeeper::exists() or ZooKeeper::get(), but not for those that were
     * set using ZooKeeper::getChildren().
     */
    NodeDataChanged = 3,

    /**
     * A change has occurred in the list of children.
     *
     * This event is triggered for watches that were set using
     * ZooKeeper::getChildren(), but not for those that were set using
     * ZooKeeper::get() or ZooKeeper::exists().
     */
    NodeChildrenChanged = 4,
  };

  /**
   * Returns WatchEvent::type enum in human-readable string format.
   */
  const std::string toString(type eventType);
};

/**
 * Namespace for create mode enums.
 */
namespace CreateMode {
  /**
   * Create modes.
   *
   * In seuqential mode (either PersistentSequential of rEphemeralSequential),
   * ZooKeeper appends a monotonicly increasing counter to the end of path. This
   * counter is unique to the parent znode. The counter has a format of "%010d"
   * -- that is 10 digits with 0 (zero) padding (e.g. "<path>0000000001"). The
   * counter used to store the next sequence number is a signed int (4 bytes)
   * maintained by the parent znode, the counter will overflow when incremented
   * beyond 2147483647 (resulting in a name "<path>-2147483647").
   */
  enum type {
    /**
     * The znode will not be automatically deleted upon client's disconnect.
     */
    Persistent = 0,

    /**
     * The znode will not be automatically deleted upon client's disconnect,
     * and its name will be appended with a monotonically increasing number.
     */
    PersistentSequential = 2,

    /**
     * The znode will be deleted upon the client's disconnect.
     */
    Ephemeral = 1,

    /**
     * The znode will be deleted upon the client's disconnect, and its name
     * will be appended with a monotonically increasing number.
     */
    EphemeralSequential = 3,
  };

  const std::string toString(int32_t flags);
};

/**
 * Namespace for znode permission enum.
 */
namespace Permission {
  enum type {
    /** Permission to read data on a znode. */
    Read = 1 << 0,

    /** Permission to write data on a znode. */
    Write = 1 << 1,

    /** Permission to create children. */
    Create = 1 << 2,

    /** Permission to delete children. */
    Delete = 1 << 3,

    /** Permission to set permissions on a znode. */
    Admin = 1 << 4,
    All = Read | Write | Create | Delete | Admin,
  };

  const std::string toString(int32_t flags);
};

class AclImpl;
class Acl {
  public:
    Acl();
    Acl(const std::string& scheme, const std::string& expression,
        int32_t permissions);
    Acl(const Acl& orig);
    Acl& operator=(const Acl& orig);
    ~Acl();
    const std::string getScheme() const;
    void setScheme(const std::string& scheme);

    const std::string getExpression() const;
    void setExpression(const std::string& expression);

    int32_t getPermissions() const;
    void setPermissions(int32_t permissions);

  private:
    AclImpl* impl_;
};

class ZnodeStatImpl;

/**
 * Metadata of a znode.
 */
class ZnodeStat {
  public:
    ZnodeStat();
    ZnodeStat(const ZnodeStat& orig);
    ZnodeStat& operator=(const ZnodeStat& orig);
    ~ZnodeStat();

    /**
     * Gets the zxid of the transaction that caused this znode to be created.
     */
    int64_t getCzxid() const;

    /**
     * Gets the zxid of the transaction that last modified this znode.
     */
    int64_t getMzxid() const;

    /**
     * Gets the time in milliseconds from epoch when this znode was created.
     */
    int64_t getCtime() const;

    /**
     * Gets the time in milliseconds from epoch when this znode was last
     * modified.
     */
    int64_t getMtime() const;

    /**
     * Gets the number of transactions applied to the data of this znode.
     */
    int32_t getVersion() const;

    /**
     * Gets the number of transactions that added or removed the children of
     * this znode.
     */
    int32_t getCversion() const;

    /**
     * Gets the number of transactions that modified the ACL of this znode.
     */
    int32_t getAversion() const;

    /**
     * Gets the session id of the owner of this znode if the znode is an
     * ephemeral node.
     *
     * If it is not an ephemeral node, it will be zero.
     */
    int64_t getEphemeralOwner() const;

    /**
     * Gets the data length this znode.
     */
    int32_t getDataLength() const;

    /**
     * Gets the number of children this znode has.
     */
    int32_t getNumChildren() const;
    int64_t getPzxid() const;

    void setCzxid(int64_t czxid);
    void setMzxid(int64_t mzxid);
    void setCtime(int64_t ctime);
    void setMtime(int64_t mtime);
    void setVersion(int32_t version);
    void setCversion(int32_t cversion);
    void setAversion(int32_t aversion);
    void setEphemeralOwner(int64_t ephemeralOwner);
    void setDataLength(int32_t dataLength);
    void setNumChildren(int32_t numChildren);
    void setPzxid(int64_t pzxid);

  private:
    ZnodeStatImpl* impl_;
};

/**
 * Callback interface for watch event.
 */
class Watch {
  public:
    /**
     * @param event Event type that caused this watch to trigger.
     * @param state State of the zookeeper session.
     * @param path Znode path where this watch was set to.
     */
    virtual void process(WatchEvent::type event, SessionState::type state,
                         const std::string& path) = 0;
};

/**
 * Callback interface for ZooKeeper::set() operation.
 */
class SetCallback {
  public:
    /**
     * @param rc Ok if this set() operation was successful.
     * @param path The path of the znode this set() operation was for
     * @param stat Stat of the resulting znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode::type rc, const std::string& path,
                         const ZnodeStat& stat) = 0;
};

/**
 * Callback interface for ZooKeeper::exists() operation.
 */
class ExistsCallback {
  public:
    /**
     * @param rc Ok if this znode exists.
     * @param path The path of the znode this exists() operation was for
     * @param stat stat of the znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode::type rc, const std::string& path,
                         const ZnodeStat& stat) = 0;
};

/**
 * Callback interface for ZooKeeper::get() operation.
 */
class GetCallback {
  public:
    /**
     * @param rc Ok if this get() operation was successful.
     * @param path The path of the znode this get() operation was for
     * @param data Data associated with this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode::type rc, const std::string& path,
                         const std::string& data, const ZnodeStat& stat) = 0;
};

/**
 * Callback interface for ZooKeeper::getAcl() operation.
 */
class GetAclCallback {
  public:
    /**
     * @param rc Ok if this getAcl() operation was successful.
     * @param path The path of the znode this getAcl() operation was for
     * @param acl The list of ACLs for this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode::type rc, const std::string& path,
                         const std::vector<Acl>& acl, const ZnodeStat& stat) = 0;
};

/**
 * Callback interface for ZooKeeper::getChildren() operation.
 */
class GetChildrenCallback {
  public:
    /**
     * @param rc Ok if this getChildren() operation was successful.
     * @param path The path of the znode this getChildren() operation was for
     * @param children The list of children for this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode::type rc, const std::string& path,
                         const std::vector<std::string>& children,
                         const ZnodeStat& stat) = 0;
};

/**
 * Callback interface for ZooKeeper::create() operation.
 */
class CreateCallback {
  public:
    /**
     * @param rc One of the ReturnCode::type enums. Most common values are:
     *        <ul>
     *          <li>ReturnCode::Ok If the znode was created successfully.</li>
     *          <li>ReturnCode::NodeExists If the znode already exists.</li>
     *        </ul>
     *
     * @param pathRequested The path of the znode this operation was for.
     * @param pathCreated The path of the znode that was created by this
     *                    request. Valid iff rc == Ok. This is useful only
     *                    for sequential znode, in which the path of the
     *                    resulting znode is different from the path originally
     *                    specified in the request. For non-seuquential znode,
     *                    this is equal to pathRequested. See CreateMode::type
     *                    for more detail about sequential znode path names.
     */
    virtual void process(ReturnCode::type rc, const std::string& pathRequested,
                         const std::string& pathCreated) = 0;
};

/**
 * Callback interface for ZooKeeper::remove() operation.
 */
class RemoveCallback {
  public:
    /**
     * @param rc ReturnCode::Ok if this remove() operation was successful.
     * @param path The path of the znode this operation was for.
     */
    virtual void process(ReturnCode::type rc, const std::string& path) = 0;
};

/**
 * Callback interface for ZooKeeper::setAcl() operation.
 */
class SetAclCallback {
  public:
    /**
     * @param rc ReturnCode::Ok if this setAcl() operation was successful.
     * @param path The path of the znode this operation was for.
     */
    virtual void process(ReturnCode::type rc, const std::string& path) = 0;
};

/**
 * Callback interface for ZooKeeper::sync() operation.
 */
class SyncCallback {
  public:
    /**
     * @param rc ReturnCode::Ok if this sync() operation was successful.
     * @param path The path of the znode this operation was for.
     */
    virtual void process(ReturnCode::type rc, const std::string& path) = 0;
};

/**
 * Callback interface for ZooKeeper::addAuth() operation.
 */
class AddAuthCallback {
  public:
    /**
     * @param rc Ok if this addAuth() operation was successful.
     * @param scheme The scheme used for this operation.
     * @param cert The certificate used for this operation.
     */
    virtual void process(ReturnCode::type rc, const std::string& scheme,
                         const std::string& cert) = 0;
};

class ZooKeeperImpl;
class ZooKeeper : boost::noncopyable {
  public:
    ZooKeeper();
    ~ZooKeeper();

    /**
     * Initializes ZooKeeper session asynchronously.
     */
    ReturnCode::type init(const std::string& hosts, int32_t sessionTimeoutMs,
                    boost::shared_ptr<Watch> watch);

    /**
     * Adds authentication info for this session asynchronously.
     *
     * The application calls this function to specify its credentials for
     * purposes of authentication. The server will use the security provider
     * specified by the scheme parameter to authenticate the client connection.
     * If the authentication request has failed:
     * <ul>
     *   <li>The server connection is dropped, and the session state becomes
     *       SessionState::AuthFailed</li>
     *   <li>All the existing watchers are called for WatchEvent::Session
     *       event with SessionState::AuthFailed as the state parameter.</li>
     * </ul>
     *
     * @param scheme the id of authentication scheme. Natively supported:
     * "digest" password-based authentication
     * @param cert authentification certificate.
     * @param callback The callback to invoke when the request completes.
     *
     * @return Ok on successfu or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     * ZSYSTEMERROR - a system error occured
     */
    ReturnCode::type addAuth(const std::string& scheme, const std::string& cert,
                       boost::shared_ptr<AddAuthCallback> callback);

    /**
     * Adds authentication info for this session synchronously.
     */
    ReturnCode::type addAuth(const std::string& scheme,
                             const std::string& cert) {
      return ReturnCode::Unimplemented;
    }


    /**
     * Create a znode asynchronously.
     *
     * A znode can only be created if it does not already exists. The CreateMode::type
     * affect the creation of nodes. In Ephemeral mode, the node will
     * automatically get removed if the client session goes away. If the
     * Sequential mode is set, a unique monotonically increasing sequence
     * number is appended to the path name. The sequence number is always
     * fixed length of 10 digits, 0 padded.
     *
     * @param path The name of the znode.
     * @param data The data to be stored in the node.
     * @param acl The initial ACL of the node. The ACL must not be null or
     *            empty.
     * @param mode
     * @param callback the routine to invoke when the request completes.
     *                 The completion will be triggered with one of the
     *                 following codes passed in as the rc argument:
     *
     * ZOK operation completed successfully
     * ZNONODE the parent node does not exist.
     * ZNODEEXISTS the node already exists
     * ZNOAUTH the client does not have permission.
     * ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
     *
     * @return Ok on success
     *         BadArguments invalid input parameters
     *         InvalidState - zhandle state is either Expired or SessionAuthFailed
     *         MarshallingError - failed to marshall a request; possibly, out of memory
     */
    ReturnCode::type create(const std::string& path, const std::string& data,
                      const std::vector<Acl>& acl, CreateMode::type mode,
                      boost::shared_ptr<CreateCallback> callback);

    /**
     * Synchronously create a znode.
     */
    ReturnCode::type create(const std::string& path, const std::string& data,
                      const std::vector<Acl>& acl, CreateMode::type mode,
                      std::string& pathCreated);

    /**
     * Removes a znode asynchronously.
     *
     * @param path the name of the znode.
     * @param version the expected version of the znode. The function will fail
     *                if the actual version of the znode does not match the
     *                expected version. If -1 is used the version check will not
     *                take place.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type remove(const std::string& path, int32_t version,
                      boost::shared_ptr<RemoveCallback> callback);

    /**
     * Removes a znode synchronously.
     */
    ReturnCode::type remove(const std::string& path, int32_t version) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Checks the existence of a znode in zookeeper asynchronously.
     *
     * This function allows one to specify a watch, a callback object. The callback
     * will be called once the watch has fired. The associated context data will be 
     * passed to the function as the watcher context parameter. 
     * 
     * @param path The name of the node.
     * @param watch if non-null a watch will set on the specified znode on the server.
     * The watch will be set even if the node does not exist. This allows clients 
     * to watch for nodes to appear.
     * @param callback The callback to invoke when the request completes. If you don't
     *                 need to get called back, pass an empty pointer (i.e. 
     *                 boost::shared_ptr<ExistsCallback>()). 
     *
     * @return One of the ReturnCode::type enums. Most common values are:
     * <ul>
     *   <li>ReturnCode::Ok if this request has been enqueued successfully.</li>
     *   <li>ReturnCode::InvalidState - The session state is either in
     *       SessionState::Expired or in SessionState::AuthFailed.</li>
     * </ul>
     */
    ReturnCode::type exists(const std::string& path,
            boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ExistsCallback> callback);

    /**
     * A synchronous version of exists().
     *
     * @param path The name of the node.
     * @param watch if non-null a watch will set on the specified znode on the server.
     * The watch will be set even if the node does not exist. This allows clients
     * to watch for nodes to appear.
     * @param[out] stat The stat of this znode. Valid iff rc == ReturnCode::Ok.
     *
     * @return One of the ReturnCode::type enums. Most common values are:
     * <ul>
     *   <li>ReturnCode::Ok The znode exists.
     *   <li>ReturnCode::NoNode The znode does not exist.
     * </ul>
     */
    ReturnCode::type exists(const std::string& path, boost::shared_ptr<Watch> watch,
                            ZnodeStat& stat);

    /**
     * Gets the data associated with a znode.
     *
     * @param path The name of the znode.
     * @param watch If non-null, a watch will be set at the server to notify
     * the client if the znode changes.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type get(const std::string& path,
                         boost::shared_ptr<Watch> watch,
			 boost::shared_ptr<GetCallback> callback);

    /**
     * Gets the data associated with a znode synchronously.
     */
    ReturnCode::type get(const std::string& path,
                         boost::shared_ptr<Watch> watch,
                         ZnodeStat& stat) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Sets the data associated with a znode.
     *
     * @param path The name of the znode.
     * @param data Data to set.
     * @param version The expected version of the znode. This operation will fail if
     *                the actual version of the node does not match the expected
     *                version. If -1 is used the version check will not take place.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type set(const std::string& path, const std::string& data,
                   int32_t version, boost::shared_ptr<SetCallback> callback);

    /**
     * Sets the data associated with a znode synchronously.
     */
    ReturnCode::type set(const std::string& path, const std::string& data,
                         int32_t version, ZnodeStat& stat) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Gets the children and the stat of a znode.
     *
     * @param path The name of the znode.
     * @param watch If non-null, a watch will be set at the server to notify
     *              the client if the node changes.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type getChildren(const std::string& path,
                           boost::shared_ptr<Watch> watch,
                           boost::shared_ptr<GetChildrenCallback> callback);

    /**
     * Gets the children and the stat of a znode synchronously.
     *
     * @param path The name of the znode.
     * @param watch If non-null, a watch will be set at the server to notify
     *              the client if the node changes.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type getChildren(const std::string& path,
                           boost::shared_ptr<Watch> watch,
                           std::vector<std::string>& children,
                           ZnodeStat& stat) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Gets the acl associated with a znode.
     *
     * @param path The name of the znode.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type getAcl(const std::string& path,
                      boost::shared_ptr<GetAclCallback> callback);

    /**
     * Gets the acl associated with a znode synchronously.
     */
    ReturnCode::type getAcl(const std::string& path,
                            std::vector<Acl>& acl, ZnodeStat& stat) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Sets the Acl associated with a znode.
     *
     * @param path The name of the znode.
     * @param version The expected version of the znode. This operation will fail if
     *                the actual version of the node does not match the expected
     *                version. If -1 is used the version check will not take place.
     * @param acl Acl to set.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type setAcl(const std::string& path, int32_t version,
                      const std::vector<Acl>& acl,
                      boost::shared_ptr<SetAclCallback> callback);

    /**
     * Sets the Acl associated with a znode synchronously.
     *
     * @param path The name of the znode.
     * @param version The expected version of the znode. This operation will fail if
     *                the actual version of the node does not match the expected
     *                version. If -1 is used the version check will not take place.
     * @param acl Acl to set.
     * @param callback The callback to invoke when the request completes.
     *
     * @return ReturnCode::Ok if the request has been enqueued successfully.
     */
    ReturnCode::type setAcl(const std::string& path, int32_t version,
                      const std::vector<Acl>& acl) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Asynchronously flushes the channel between process and leader.
     *
     * When the callback gets invoked with ReturnCode Ok, ZooKeeper guarantees
     * that all the operations requested before calling sync() have been
     * completed and their callbacks have been invoked.
     *
     * @param path Currently unused. You can pass any value and this method will
     *             simply ignore it, except that you'll get this value back in
     *             the callback. This parameter might get used in the future
     *             when ZooKeeper supports multiple namespaces to only sync the
     *             namespace that contains the path.
     * @param callback the routine to invoke when the request completes.
     *
     * @return Ok If the request was enqueued successfully. One of the errors in
     *         the ReturnCode in case of failure.
     */
    ReturnCode::type sync(const std::string& path,
                    boost::shared_ptr<SyncCallback> callback);

    /**
     * Closes this ZooKeeper session.
     *
     * After this call, the client session will no longer be valid.
     * The function will flush any outstanding send requests before return.
     * As a result it may block.
     *
     * Ok success
     * BadArguments Invalid input parameters.
     * MarshallingError Failed to marshall a request; possibly out of memory.
     * OperationTimeout Failed to flush the buffers within the specified timeout.
     * ConnectionLoss A network error occured while attempting to send request to server
     */
    ReturnCode::type close();

    /**
     * Gets the current state of this ZooKeeper object.
     *
     * @see State
     */
    SessionState::type getState();

    /**
     * Gets the ZooKeeper session ID.
     *
     * This ZooKeeper object must be in "Connected" state for this operation
     * to succeed.
     *
     * @param[out] id Session ID.
     */
    ReturnCode::type getSessionId(int64_t& id) {
      return ReturnCode::Unimplemented;
    }

    /**
     * Gets the ZooKeeper session password.
     *
     * This ZooKeeper object must be in "Connected" state for this operation
     * to succeed.
     *
     * @param[out] password Session password.
     */
    ReturnCode::type getSessionPassword(std::string& password) {
      return ReturnCode::Unimplemented;
    }

  private:
    ZooKeeperImpl* impl_;
};
}}}

#endif  // SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_
