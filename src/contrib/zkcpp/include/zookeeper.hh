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

class ZooKeeperImpl;

/**
 * ZooKeeper return codes.
 */
enum ReturnCode {
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

/**
 * These constants represent the states of a zookeeper connection. They are
 * possible parameters of the watcher callback.
 */
enum State {
  Expired = -112,

  /**
   * Session Authentication failed. The client is no longer connected to the
   * server. You must call ZooKeeper::init() to re-establish the connection.
   */
  SessionAuthFailed = -113,
  Connecting = 1,
  Connected = 3,
};

/**
 * These constants indicate the event that caused a watch to trigger. They
 * are possible values of the first parameter of the watcher callback.
 */
enum Event {
  /**
   * Session state has changed.
   *
   * This is generated when a client loses contact or reconnects with a
   * server.
   */
  Session = -1,

  /**
   * Node has been created.
   *
   * This is only generated by watches on non-existent nodes. These watches
   * are set using \ref zoo_exists.
   */
  NodeCreated = 1,

  /**
   * Node has been deleted.
   *
   * This is only generated by watches on nodes. These watches
   * are set using \ref zoo_exists and \ref zoo_get.
   */
  NodeDeleted = 2,

  /**
   * Node data has changed.
   *
   * This is only generated by watches on nodes. These watches
   * are set using \ref zoo_exists and \ref zoo_get.
   */
  NodeDataChanged = 3,

  /**
   * A change has occurred in the list of children.
   *
   * This is only generated by watches on the child list of a node. These
   * watches are set using \ref zoo_get_children or \ref zoo_get_children2.
   */
  NodeChildrenChanged = 4,
};

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
enum CreateMode {
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

/**
 * TODO use C++ jute.
 */
static struct ACL __OPEN_ACL_UNSAFE_ACL[] = {{0x1f, {(char*)"world", (char*)"anyone"}}};
static struct ACL_vector OPEN_ACL_UNSAFE = { 1, __OPEN_ACL_UNSAFE_ACL};

class Watch {
  public:
    virtual void process(Event event, State state, const std::string& path) = 0;
};

/**
 * Callback interface for set() operation.
 */
class SetCallback {
  public:
    /**
     * @param rc Ok if this set() operation was successful.
     * @param path The path of the znode this set() operation was for
     * @param stat Stat stat of the resulting znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode rc, const std::string& path,
                         const Stat& stat) = 0;
};

/**
 * Callback interface for exists() operation.
 */
class ExistsCallback {
  public:
    /**
     * @param rc Ok if this znode exists.
     * @param path The path of the znode this exists() operation was for
     * @param stat Stat stat of the znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode rc, const std::string& path,
                         const Stat& stat) = 0;
};

/**
 * Callback interface for get() operation.
 */
class GetCallback {
  public:
    /**
     * @param rc Ok if this get() operation was successful.
     * @param path The path of the znode this get() operation was for
     * @param data Data associated with this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode rc, const std::string& path,
                         const std::string& data, const Stat& stat) = 0;
};

/**
 * Callback interface for getAcl() operation.
 */
class GetAclCallback {
  public:
    /**
     * @param rc Ok if this getAcl() operation was successful.
     * @param path The path of the znode this getAcl() operation was for
     * @param acl The list of ACLs for this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode rc, const std::string& path,
                         const ACL_vector& acl, const Stat& stat) = 0;
};

/**
 * Callback interface for getChildren() operation.
 */
class GetChildrenCallback {
  public:
    /**
     * @param rc Ok if this getChildren() operation was successful.
     * @param path The path of the znode this getChildren() operation was for
     * @param acl The list of children for this znode. Valid iff rc == Ok.
     * @param stat Stat associated with this znode. Valid iff rc == Ok.
     */
    virtual void process(ReturnCode rc, const std::string& path,
                         const std::vector<std::string>& children,
                         const Stat& stat) = 0;
};

/**
 * Callback interface for create() operation.
 */
class CreateCallback {
  public:
    /**
     * @param rc Ok if this create() operation was successful.
     * @param pathRequested The path of the znode this operation was for.
     * @param pathCreated The path of the znode that was created by this
     *                    request. Valid iff rc == Ok. This is useful only
     *                    for sequential znode, in which the path of the
     *                    resulting znode is different from the path originally
     *                    specified in the request. For non-seuquential znode,
     *                    this is equal to pathRequested. See ::CreateMode
     *                    for more detail about sequential znode path names.
     */
    virtual void process(ReturnCode rc, const std::string& pathRequested,
                         const std::string& pathCreated) = 0;
};

class VoidCallback {
  public:
    virtual void processResult(ReturnCode rc, std::string path) = 0;
};

/**
 * Callback interface for addAuth() operation.
 */
class AddAuthCallback {
  public:
    /**
     * @param rc Ok if this addAuth() operation was successful.
     * @param scheme The scheme used for this operation.
     * @param cert The certificate used for this operation.
     */
    virtual void process(ReturnCode rc, const std::string& scheme,
                         const std::string& cert) = 0;
};

class ZooKeeper : boost::noncopyable {
  public:
    ZooKeeper();
    ~ZooKeeper();

    /**
     * Initializes ZooKeeper session asynchronously.
     */
    ReturnCode init(const std::string& hosts, int32_t sessionTimeoutMs,
                    boost::shared_ptr<Watch> watch);

    /**
     * Adds authentication info for this session.
     *
     * The application calls this function to specify its credentials for
     * purposes of authentication. The server will use the security provider
     * specified by the scheme parameter to authenticate the client connection.
     * If the authentication request has failed:
     * <ul>
     *   <li>The server connection is dropped, and the session state becomes
     *       ::SessionAuthFailed</li>
     *   <li>All the existing watchers are called with for ::Session event with
     *       the ::SessionAuthFailed value as the state parameter.</li>
     * </ul>
     *
     * @param scheme the id of authentication scheme. Natively supported:
     * "digest" password-based authentication
     * @param authentification certificate.
     * @param callback The callback to invoke when the request completes.
     *
     * @return Ok on successfu or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     * ZSYSTEMERROR - a system error occured
     */
    ReturnCode addAuth(const std::string& scheme, const std::string& cert,
                       boost::shared_ptr<AddAuthCallback> callback);

    /**
     * Create a znode asynchronously.
     *
     * A znode can only be created if it does not already exists. The ::CreateMode
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
    ReturnCode create(const std::string& path, const std::string& data,
                      const struct ACL_vector *acl, CreateMode mode,
                      boost::shared_ptr<CreateCallback> callback);

    /**
     * Removes a znode asynchronously.
     *
     * @param path the name of the znode.
     * @param version the expected version of the znode. The function will fail
     *                if the actual version of the znode does not match the
     *                expected version. If -1 is used the version check will not
     *                take place.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * ZBADVERSION expected version does not match actual version.
     * ZNOTEMPTY children are present; node cannot be deleted.
     * \param data the data that will be passed to the completion routine when 
     * the function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode remove(const std::string& path, int version,
                      boost::shared_ptr<VoidCallback> callback);

    /**
     * Checks the existence of a node in zookeeper.
     *
     * This function is similar to \ref zoo_axists except it allows one specify 
     * a watcher object - a function pointer and associated context. The function
     * will be called once the watch has fired. The associated context data will be 
     * passed to the function as the watcher context parameter. 
     * 
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param path the name of the node. Expressed as a file name with slashes 
     * separating ancestors of the node.
     * \param watcher if non-null a watch will set on the specified znode on the server.
     * The watch will be set even if the node does not exist. This allows clients 
     * to watch for nodes to appear.
     * \param watcherCtx user specific data, will be passed to the watcher callback.
     * Unlike the global context set by \ref zookeeper_init, this watcher context
     * is associated with the given instance of the watcher only.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * \param data the data that will be passed to the completion routine when the 
     * function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode exists(const std::string& path,
            boost::shared_ptr<Watch> watch,
            boost::shared_ptr<ExistsCallback> callback);

    /**
     * Synchronous version of exists().
     *
     * @param path The name of the node.
     * @param watcher if non-null a watch will set on the specified znode on the server.
     * The watch will be set even if the node does not exist. This allows clients 
     * to watch for nodes to appear.
     *
     * @returns
     * <ul>
     * <li>Ok The node exists
     * <li>NoNode The node does not exist.
     * <li>ZNOAUTH the client does not have permission.
     * <li>ZBADARGUMENTS - invalid input parameters
     * <li>InvalidState - zhandle state is either Expired or SessionAuthFailed
     * <li>ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     * </ul>
     */
    ReturnCode exists(const std::string& path, boost::shared_ptr<Watch> watch,
                      Stat& stat);

    /**
     * Gets the data associated with a node.
     *
     * This function is similar to \ref zoo_aget except it allows one specify 
     * a watcher object rather than a boolean watch flag.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param path the name of the node. Expressed as a file name with slashes 
     * separating ancestors of the node.
     * \param watcher if non-null, a watch will be set at the server to notify 
     * the client if the node changes.
     * \param watcherCtx user specific data, will be passed to the watcher callback.
     * Unlike the global context set by \ref zookeeper_init, this watcher context
     * is associated with the given instance of the watcher only.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * \param data the data that will be passed to the completion routine when 
     * the function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either in Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode get(const std::string& path,
                   boost::shared_ptr<Watch>,
                   boost::shared_ptr<GetCallback> callback);

    /**
     * Sets the data associated with a znode.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param path the name of the node. Expressed as a file name with slashes 
     * separating ancestors of the node.
     * \param buffer the buffer holding data to be written to the node.
     * \param buflen the number of bytes from buffer to write.
     * \param version the expected version of the node. The function will fail if 
     * the actual version of the node does not match the expected version. If -1 is 
     * used the version check will not take place. * completion: If null, 
     * the function will execute synchronously. Otherwise, the function will return 
     * immediately and invoke the completion routine when the request completes.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * ZBADVERSION expected version does not match actual version.
     * \param data the data that will be passed to the completion routine when 
     * the function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode set(const std::string& path, const std::string& data,
                   int version, boost::shared_ptr<SetCallback> callback);

    /**
     * Gets the children and the stat of a znode.
     *
     * @param path the name of the znode.
     * @param watcher if non-null, a watch will be set at the server to notify
     * the client if the node changes.
     * @param completion the routine to invoke when the request completes. The
     * completion will be triggered with one of the following codes passed in as
     * the rc argument:
     *   Ok operation completed successfully
     *   NoNode the node does not exist.
     *   ZNOAUTH the client does not have permission.
     *
     * @return Ok on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode getChildren(const std::string& path,
                           boost::shared_ptr<Watch> watch,
                           boost::shared_ptr<GetChildrenCallback> callback);

    /**
     * Gets the acl associated with a znode.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param path the name of the node. Expressed as a file name with slashes 
     * separating ancestors of the node.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * \param data the data that will be passed to the completion routine when 
     * the function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     */
    ReturnCode getAcl(const std::string& path,
                      boost::shared_ptr<GetAclCallback> callback);

    /**
     * \brief sets the acl associated with a node.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param path the name of the node. Expressed as a file name with slashes 
     * separating ancestors of the node.
     * \param buffer the buffer holding the acls to be written to the node.
     * \param buflen the number of bytes from buffer to write.
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with one of the following codes passed in as the rc argument:
     * ZOK operation completed successfully
     * ZNONODE the node does not exist.
     * ZNOAUTH the client does not have permission.
     * ZINVALIDACL invalid ACL specified
     * ZBADVERSION expected version does not match actual version.
     * \param data the data that will be passed to the completion routine when 
     * the function completes.
     * \return ZOK on success or one of the following errcodes on failure:
     * ZBADARGUMENTS - invalid input parameters
     * InvalidState - zhandle state is either Expired or SessionAuthFailed
     * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
     * TODO use C++ jute
     */
    ReturnCode setAcl(const std::string& path, int version,
            struct ACL_vector *acl, boost::shared_ptr<VoidCallback> callback);

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
    ReturnCode sync(const std::string& path,
                    boost::shared_ptr<VoidCallback> callback);

    /**
     * \brief atomically commits multiple zookeeper operations.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param count the number of operations
     * \param ops an array of operations to commit
     * \param results an array to hold the results of the operations
     * \param completion the routine to invoke when the request completes. The completion
     * will be triggered with any of the error codes that can that can be returned by the 
     * ops supported by a multi op (see \ref zoo_acreate, \ref zoo_adelete, \ref zoo_aset).
     * \param data the data that will be passed to the completion routine when
     * the function completes.
     * \return the return code for the function call. This can be any of the
     * values that can be returned by the ops supported by a multi op (see
     * \ref zoo_acreate, \ref zoo_adelete, \ref zoo_aset).
    ReturnCode multi(int count, const zoo_op_t *ops,
            zoo_op_result_t *results, boost::shared_ptr<VoidCallback> callback);
     */

    /**
     * \brief atomically commits multiple zookeeper operations synchronously.
     *
     * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
     * \param count the number of operations
     * \param ops an array of operations to commit
     * \param results an array to hold the results of the operations
     * \return the return code for the function call. This can be any of the
     * values that can be returned by the ops supported by a multi op (see
     * \ref zoo_acreate, \ref zoo_adelete, \ref zoo_aset).
    ReturnCode multi(int count, const zoo_op_t *ops, zoo_op_result_t *results);
     */

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
    ReturnCode close();

    /**
     * Gets the current state of this ZooKeeper object.
     *
     * @see State
     */
    State getState();

    /**
     * Gets the ZooKeeper session ID.
     *
     * This ZooKeeper object must be in "Connected" state for this operation
     * to succeed.
     *
     * @param(OUT) id Session ID.
     */
    ReturnCode getSessionId(int64_t& id) {
      return Unimplemented;
    }

    /**
     * Gets the ZooKeeper session password.
     *
     * This ZooKeeper object must be in "Connected" state for this operation
     * to succeed.
     *
     * @param(OUT) password Session password.
     */
    ReturnCode getSessionPassword(std::string& password) {
      return Unimplemented;
    }

  private:
    ZooKeeperImpl* impl_;
};
}}}

#endif  // SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_
