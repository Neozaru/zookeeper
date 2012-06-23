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

#include <cppunit/extensions/HelperMacros.h>
#include "CppAssertHelper.h"

#include <boost/thread/mutex.hpp>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/select.h>

#include "CollectionUtil.h"
#include "ThreadingUtil.h"

using namespace Util;

#include "Vector.h"
using namespace std;

#include <cstring>
#include <list>

#include "zookeeper.h"
#include <zookeeper/zookeeper.hh>
#include <errno.h>
#include <recordio.h>
#include "Util.h"

using boost::shared_ptr;
using namespace org::apache::zookeeper;

struct buff_struct_2 {
    int32_t len;
    int32_t off;
    char *buffer;
};

static int Stat_eq(struct Stat* a, struct Stat* b)
{
    if (a->czxid != b->czxid) return 0;
    if (a->mzxid != b->mzxid) return 0;
    if (a->ctime != b->ctime) return 0;
    if (a->mtime != b->mtime) return 0;
    if (a->version != b->version) return 0;
    if (a->cversion != b->cversion) return 0;
    if (a->aversion != b->aversion) return 0;
    if (a->ephemeralOwner != b->ephemeralOwner) return 0;
    if (a->dataLength != b->dataLength) return 0;
    if (a->numChildren != b->numChildren) return 0;
    if (a->pzxid != b->pzxid) return 0;
    return 1;
}

static void yield(zhandle_t *zh, int i)
{
  sleep(i);
}

typedef struct evt {
    string path;
    int type;
} evt_t;

typedef struct watchCtx {
private:
    list<evt_t> events;
    watchCtx(const watchCtx&);
    watchCtx& operator=(const watchCtx&);
public:
    bool connected;
    zhandle_t *zh;
    boost::mutex mutex;

    watchCtx() {
        connected = false;
        zh = 0;
    }
    ~watchCtx() {
        if (zh) {
            zookeeper_close(zh);
            zh = 0;
        }
    }

    evt_t getEvent() {
        evt_t evt;
        mutex.lock();
        CPPUNIT_ASSERT( events.size() > 0);
        evt = events.front();
        events.pop_front();
        mutex.unlock();
        return evt;
    }

    int countEvents() {
        int count;
        mutex.lock();
        count = events.size();
        mutex.unlock();
        return count;
    }

    void putEvent(evt_t evt) {
        mutex.lock();
        events.push_back(evt);
        mutex.unlock();
    }

    bool waitForConnected(zhandle_t *zh) {
        time_t expires = time(0) + 10;
        while(!connected && time(0) < expires) {
            yield(zh, 1);
        }
        return connected;
    }
    bool waitForDisconnected(zhandle_t *zh) {
        time_t expires = time(0) + 15;
        while(connected && time(0) < expires) {
            yield(zh, 1);
        }
        return !connected;
    }
} watchctx_t;

std::vector<data::ACL> openAcl;

class TestClient : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(TestClient);
    CPPUNIT_TEST(testAcl);
    CPPUNIT_TEST(testChroot);
    CPPUNIT_TEST(testIPV6);
    #if 0
    CPPUNIT_TEST(testAsyncWatcherAutoReset);
    CPPUNIT_TEST(testPath);
    CPPUNIT_TEST(testPathValidation);
    CPPUNIT_TEST(testAuth);
    CPPUNIT_TEST(testHangingClient);
    CPPUNIT_TEST(testWatcherAutoResetWithGlobal);
    CPPUNIT_TEST(testWatcherAutoResetWithLocal);
    CPPUNIT_TEST(testGetChildren2);
    #endif
    CPPUNIT_TEST_SUITE_END();

    static void watcher(zhandle_t *, int type, int state, const char *path,void*v){
        watchctx_t *ctx = (watchctx_t*)v;

        if (state == ZOO_CONNECTED_STATE) {
            ctx->connected = true;
        } else {
            ctx->connected = false;
        }
        if (type != ZOO_SESSION_EVENT) {
            evt_t evt;
            evt.path = path;
            evt.type = type;
            ctx->putEvent(evt);
        }
    }

    static const char hostPorts[];

    const char *getHostPorts() {
        return hostPorts;
    }
    
    zhandle_t *createClient(watchctx_t *ctx) {
        return createClient(hostPorts, ctx);
    }

    zhandle_t *createClient(const char *hp, watchctx_t *ctx) {
        zhandle_t *zk = zookeeper_init(hp, watcher, 10000, 0, ctx, 0);
        ctx->zh = zk;
        sleep(1);
        return zk;
    }
    
    zhandle_t *createchClient(watchctx_t *ctx, const char* chroot) {
        zhandle_t *zk = zookeeper_init(chroot, watcher, 10000, 0, ctx, 0);
        ctx->zh = zk;
        sleep(1);
        return zk;
    }
        
public:
    void startServer() {
        char cmd[1024];
        sprintf(cmd, "%s start %s", ZKSERVER_CMD, getHostPorts());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void stopServer() {
        char cmd[1024];
        sprintf(cmd, "%s stop %s", ZKSERVER_CMD, getHostPorts());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void setUp() {
      openAcl.clear();
      data::ACL temp;
      temp.getid().getscheme() = "world";
      temp.getid().getid() = "anyone";
      temp.setperms(Permission::All);
      openAcl.push_back(temp);
    }

    void tearDown()
    {
    }
    
    #if 0
    /** have a callback in the default watcher **/
    static void default_zoo_watcher(zhandle_t *zzh, int type, int state, const char *path, void *context){
        int zrc = 0;
        struct String_vector str_vec = {0, NULL};
        zrc = zoo_wget_children(zzh, "/mytest", default_zoo_watcher, NULL, &str_vec);
    }
    
    /** this checks for a deadlock in calling zookeeper_close and calls from a default watcher that might get triggered just when zookeeper_close() is in progress **/
    void testHangingClient() {
        int zrc = 0;
        char buff[10] = "testall";
        char path[512];
        watchctx_t *ctx = NULL;
        struct String_vector str_vec = {0, NULL};
        zhandle_t *zh = zookeeper_init(hostPorts, NULL, 10000, 0, ctx, 0);
        sleep(1);
        zrc = zoo_create(zh, "/mytest", buff, 10, openAcl, 0, path, 512);
        zrc = zoo_wget_children(zh, "/mytest", default_zoo_watcher, NULL, &str_vec);
        zrc = zoo_create(zh, "/mytest/test1", buff, 10, openAcl, 0, path, 512);
        zrc = zoo_wget_children(zh, "/mytest", default_zoo_watcher, NULL, &str_vec);
        zrc = zoo_delete(zh, "/mytest/test1", -1);
        zookeeper_close(zh);
    }
    #endif

    bool waitForEvent(zhandle_t *zh, watchctx_t *ctx, int seconds) {
        time_t expires = time(0) + seconds;
        while(ctx->countEvents() == 0 && time(0) < expires) {
            yield(zh, 1);
        }
        return ctx->countEvents() > 0;
    }

#define COUNT 100

    static zhandle_t *async_zk;
    static volatile int count;
    static const char* hp_chroot;

#if 0
    static void statCompletion(int rc, const data::Stat& stat, const void *data) {
        int tmp = (int) (long) data;
        CPPUNIT_ASSERT_EQUAL(tmp, rc);
    }

    static void stringCompletion(int rc, const std::string& value, const void *data) {
        char *path = (char*)data;

        if (rc == ZCONNECTIONLOSS && path) {
            // Try again
            rc = zoo_acreate(async_zk, path, "", 0,  openAcl, 0, stringCompletion, 0);
        } else if (rc != ZOK) {
            // fprintf(stderr, "rc = %d with path = %s\n", rc, (path ? path : "null"));
        }
        if (path) {
            free(path);
        }
    }
#endif
    static void create_completion_fn(int rc, const std::string& value,
                                     const void *data) {
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        count++;
    }

    static void waitForCreateCompletion(int seconds) {
        time_t expires = time(0) + seconds;
        while(count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void waitForChrootWatch(int seconds) {
        time_t expires = time(0) + seconds;
        while (count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void waitForVoidCompletion(int seconds) {
        time_t expires = time(0) + seconds;
        while(count == 0 && time(0) < expires) {
            sleep(1);
        }
        count--;
    }

    static void voidCompletion(int rc, const void *data) {
        int tmp = (int) (long) data;
        CPPUNIT_ASSERT_EQUAL(tmp, rc);
        count++;
    }

#if 0
    static void verifyCreateFails(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZBADARGUMENTS, zoo_create(zk,
          path, "", 0, openAcl, 0, 0, 0));
    }

    static void verifyCreateOk(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZOK, zoo_create(zk,
          path, "", 0, openAcl, 0, 0, 0));
    }

    static void verifyCreateFailsSeq(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZBADARGUMENTS, zoo_create(zk,
          path, "", 0, openAcl, ZOO_SEQUENCE, 0, 0));
    }

    static void verifyCreateOkSeq(const char *path, zhandle_t *zk) {
      CPPUNIT_ASSERT_EQUAL((int)ZOK, zoo_create(zk,
          path, "", 0, openAcl, ZOO_SEQUENCE, 0, 0));
    }
#endif

    /**
       returns false if the vectors dont match
    **/
    bool compareAcl(struct ACL_vector acl1, struct ACL_vector acl2) {
        if (acl1.count != acl2.count) {
            return false;
        }
        struct ACL *aclval1 = acl1.data;
        struct ACL *aclval2 = acl2.data;
        if (aclval1->perms != aclval2->perms) {
            return false;
        }
        struct Id id1 = aclval1->id;
        struct Id id2 = aclval2->id;
        if (strcmp(id1.scheme, id2.scheme) != 0) {
            return false;
        }
        if (strcmp(id1.id, id2.id) != 0) {
            return false;
        }
        return true;
    }

    void testIPV6() {
      ZooKeeper zk;
      std::string pathCreated;
      zk.init("::1:22181", 10000, boost::shared_ptr<Watch>());
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.create("/ipv6", "", openAcl, CreateMode::Persistent, pathCreated));
    }

    void testAcl() {
      ZooKeeper zk;
      std::string pathCreated;
      zk.init(hostPorts, 10000, boost::shared_ptr<Watch>());
      data::Stat stat;
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zk.create("/acl", "", openAcl, CreateMode::Persistent, pathCreated));
      std::vector<data::ACL> acl, aclOut;

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.getAcl("/acl", aclOut, stat));
      CPPUNIT_ASSERT_EQUAL(1, (int)aclOut.size());
      CPPUNIT_ASSERT_EQUAL(std::string("world"), aclOut[0].getid().getscheme());
      CPPUNIT_ASSERT_EQUAL(std::string("anyone"), aclOut[0].getid().getid());

      data::ACL readPerm;
      readPerm.getid().getscheme() = "world";
      readPerm.getid().getid() = "anyone";
      readPerm.setperms(Permission::Read);
      acl.push_back(readPerm);
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.setAcl("/acl", -1, acl));

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.getAcl("/acl", aclOut, stat));
      CPPUNIT_ASSERT_EQUAL(1, (int)aclOut.size());
      CPPUNIT_ASSERT_EQUAL(std::string("world"), readPerm.getid().getscheme());
      CPPUNIT_ASSERT_EQUAL(std::string("anyone"), readPerm.getid().getid());
      CPPUNIT_ASSERT_EQUAL((int)Permission::Read, readPerm.getperms());
    }

    static void watcher_chroot_fn(zhandle_t *zh, int type,
                                    int state, const char *path,void *watcherCtx) {
        // check for path
        char *client_path = (char *) watcherCtx;
        CPPUNIT_ASSERT(strcmp(client_path, path) == 0);
        count ++;
    }
    class ChrootWatch : public Watch {
    public:
      ChrootWatch(const std::string& pathExpected) :
          pathExpected_(pathExpected), watchTriggered_(false) {
      }
      void process(WatchEvent::type event, SessionState::type state,
                   const std::string& path) {
        CPPUNIT_ASSERT_EQUAL(pathExpected_, path);
        watchTriggered_ = true;
      }
      std::string pathExpected_;
      bool watchTriggered_;
    };

    void testChroot() {
      ZooKeeper zk, zkChroot;
      std::string path, pathOut, dataOut;
      zk.init("127.0.0.1:22181", 10000, boost::shared_ptr<Watch>());
      zkChroot.init("127.0.0.1:22181/test/chroot", 10000,
                    boost::shared_ptr<Watch>());
      data::Stat statOut;
      std::string data = "hello";

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.create("/test", "", openAcl, CreateMode::Persistent, pathOut));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.create("/test/chroot", data, openAcl, CreateMode::Persistent,
                  pathOut));

      // get
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.get("/", boost::shared_ptr<Watch>(), dataOut, statOut));
      CPPUNIT_ASSERT_EQUAL(data, dataOut);

      //check for watches
      path = "/hello";
      zkChroot.exists(path, boost::shared_ptr<Watch>(new ChrootWatch(path)),
                      statOut);

      //check create
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.create(path, "", openAcl, CreateMode::Persistent, pathOut));
      CPPUNIT_ASSERT_EQUAL(path, pathOut);

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.create("/hello/child", "", openAcl, CreateMode::Persistent,
                        pathOut));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zkChroot.exists("/hello/child", boost::shared_ptr<Watch>(), statOut));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zk.exists("/test/chroot/hello/child", boost::shared_ptr<Watch>(),
                    statOut));

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zkChroot.remove("/hello/child", -1));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
          zkChroot.exists("/hello/child", boost::shared_ptr<Watch>(), statOut));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
          zk.exists("/test/chroot/hello/child", boost::shared_ptr<Watch>(),
                    statOut));

      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.get("/hello", shared_ptr<Watch>(new ChrootWatch("/hello")),
                     dataOut, statOut));
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zkChroot.set("/hello", "new data", -1, statOut));

      // check for getchildren
      std::vector<std::string> children;
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.getChildren("/", boost::shared_ptr<Watch>(), children,
                             statOut));
      CPPUNIT_ASSERT_EQUAL(1, (int)children.size());
      CPPUNIT_ASSERT_EQUAL(std::string("hello"), children[0]);

      // check for get/set acl
      std::vector<data::ACL> acl;
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zkChroot.getAcl("/", acl, statOut));
      CPPUNIT_ASSERT_EQUAL(1, (int)acl.size());
      CPPUNIT_ASSERT_EQUAL((int)Permission::All, (int)acl[0].getperms());

      // set acl
      acl.clear();
      data::ACL readPerm;
      readPerm.getid().getscheme() = "world";
      readPerm.getid().getid() = "anyone";
      readPerm.setperms(Permission::Read);
      acl.push_back(readPerm);
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zkChroot.setAcl("/hello", -1, acl));
      // see if you add children
      CPPUNIT_ASSERT_EQUAL(ReturnCode::NoAuth,
          zkChroot.create("/hello/child1", "", openAcl, CreateMode::Persistent,
                          pathOut));

      //add wget children test
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.getChildren("/", shared_ptr<Watch>(new ChrootWatch("/")),
                             children, statOut));
      CPPUNIT_ASSERT_EQUAL(1, (int)children.size());
      CPPUNIT_ASSERT_EQUAL(std::string("hello"), children[0]);

      //now create a node
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zkChroot.create("/child2", "", openAcl, CreateMode::Persistent,
                          pathOut));

      //ZOOKEEPER-1027 correctly return path_buffer without prefixed chroot
      path = "/zookeeper1027";
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zkChroot.create(path, "", openAcl, CreateMode::Persistent, pathOut));
      CPPUNIT_ASSERT_EQUAL(path, pathOut);
    }

#if 0
    void testAuth() {
        int rc;
        count = 0;
        watchctx_t ctx1, ctx2, ctx3, ctx4, ctx5;
        zhandle_t *zk = createClient(&ctx1);
        rc = zoo_add_auth(0, "", 0, 0, voidCompletion, (void*)-1);
        CPPUNIT_ASSERT_EQUAL((int) ZBADARGUMENTS, rc);

        rc = zoo_add_auth(zk, 0, 0, 0, voidCompletion, (void*)-1);
        CPPUNIT_ASSERT_EQUAL((int) ZBADARGUMENTS, rc);

        // auth as pat, create /tauth1, close session
        rc = zoo_add_auth(zk, "digest", "pat:passwd", 10, voidCompletion,
                          (void*)ZOK);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        waitForVoidCompletion(3);
        CPPUNIT_ASSERT(count == 0);

        std::vector<data::ACL> creatorAcl;
        data::ACL temp;
        temp.getid().getscheme() = "auth";
        temp.getid().getid() = "";
        temp.setperms(Permission::All);
        creatorAcl.push_back(temp);
        rc = zoo_create(zk, "/tauth1", "", 0, creatorAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        {
            //create a new client
            zk = createClient(&ctx4);
            rc = zoo_add_auth(zk, "digest", "", 0, voidCompletion, (void*)ZOK);
            CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
            waitForVoidCompletion(3);
            CPPUNIT_ASSERT(count == 0);

            rc = zoo_add_auth(zk, "digest", "", 0, voidCompletion, (void*)ZOK);
            CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
            waitForVoidCompletion(3);
            CPPUNIT_ASSERT(count == 0);
        }

        //create a new client
        zk = createClient(&ctx2);

        rc = zoo_add_auth(zk, "digest", "pat:passwd2", 11, voidCompletion,
                          (void*)ZOK);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        waitForVoidCompletion(3);
        CPPUNIT_ASSERT(count == 0);

        char buf[1024];
        int blen = sizeof(buf);
        struct Stat stat;
        rc = zoo_get(zk, "/tauth1", 0, buf, &blen, &stat);
        CPPUNIT_ASSERT_EQUAL((int)ZNOAUTH, rc);
        // add auth pat w/correct pass verify success
        rc = zoo_add_auth(zk, "digest", "pat:passwd", 10, voidCompletion,
                          (void*)ZOK);

        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        rc = zoo_get(zk, "/tauth1", 0, buf, &blen, &stat);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        waitForVoidCompletion(3);
        CPPUNIT_ASSERT(count == 0);
        //create a new client
        zk = createClient(&ctx3);
        rc = zoo_add_auth(zk, "digest", "pat:passwd", 10, voidCompletion, (void*) ZOK);
        waitForVoidCompletion(3);
        CPPUNIT_ASSERT(count == 0);
        rc = zoo_add_auth(zk, "ip", "none", 4, voidCompletion, (void*)ZOK);
        //make the server forget the auths
        waitForVoidCompletion(3);
        CPPUNIT_ASSERT(count == 0);

        stopServer();
        CPPUNIT_ASSERT(ctx3.waitForDisconnected(zk));
        startServer();
        CPPUNIT_ASSERT(ctx3.waitForConnected(zk));
        // now try getting the data
        rc = zoo_get(zk, "/tauth1", 0, buf, &blen, &stat);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        // also check for get
        using namespace org::apache::zookeeper;
        std::vector<data::ACL> acl, aclOut;


        rc = zoo_get_acl(zk, "/", aclOut, &stat);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        // check if the acl has all the perms
        CPPUNIT_ASSERT_EQUAL((int)1, (int)aclOut.size());
        CPPUNIT_ASSERT_EQUAL((int)ZOO_PERM_ALL, aclOut[0].getperms());

        // verify on root node
        data::ACL readPerm;
        readPerm.getid().getscheme() = "auth";
        readPerm.getid().getid() = "";
        readPerm.setperms(Permission::All);
        acl.push_back(readPerm);
        rc = zoo_set_acl(zk, "/", -1, acl);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);

        acl.clear();
        readPerm.getid().getscheme() = "world";
        readPerm.getid().getid() = "anyone";
        readPerm.setperms(Permission::All);
        acl.push_back(readPerm);
        rc = zoo_set_acl(zk, "/", -1, acl);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);

        //[ZOOKEEPER-1108], test that auth info is sent to server, if client is not
        //connected to server when zoo_add_auth was called.
        zhandle_t *zk_auth = zookeeper_init(hostPorts, NULL, 10000, 0, NULL, 0);
        rc = zoo_add_auth(zk_auth, "digest", "pat:passwd", 10, voidCompletion, (void*)ZOK);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        sleep(2);
        CPPUNIT_ASSERT(count == 1);
        count  = 0;
        CPPUNIT_ASSERT_EQUAL((int) ZOK, zookeeper_close(zk_auth));
        
        // [ZOOKEEPER-800] zoo_add_auth should return ZINVALIDSTATE if
        // the connection is closed. 
        zhandle_t *zk2 = zookeeper_init(hostPorts, NULL, 10000, 0, NULL, 0);
        sleep(1);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, zookeeper_close(zk2));
        CPPUNIT_ASSERT_EQUAL(0, zoo_state(zk2)); // 0 ==> ZOO_CLOSED_STATE
        rc = zoo_add_auth(zk2, "digest", "pat:passwd", 10, voidCompletion, (void*)ZOK);
        CPPUNIT_ASSERT_EQUAL((int) ZINVALIDSTATE, rc);

        struct sockaddr addr;
        socklen_t addr_len = sizeof(addr);
        zk = createClient(&ctx5);
        stopServer();
        CPPUNIT_ASSERT(ctx5.waitForDisconnected(zk));
        CPPUNIT_ASSERT(zookeeper_get_connected_host(zk, &addr, &addr_len) == NULL);
        addr_len = sizeof(addr);
        startServer();
        CPPUNIT_ASSERT(ctx5.waitForConnected(zk));
        CPPUNIT_ASSERT(zookeeper_get_connected_host(zk, &addr, &addr_len) != NULL);
    }

    void testGetChildren2() {
        int rc;
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);

        rc = zoo_create(zk, "/parent", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        rc = zoo_create(zk, "/parent/child_a", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        rc = zoo_create(zk, "/parent/child_b", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        rc = zoo_create(zk, "/parent/child_c", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        rc = zoo_create(zk, "/parent/child_d", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        struct String_vector strings;
        struct Stat stat_a, stat_b;

        rc = zoo_get_children2(zk, "/parent", 0, &strings, &stat_a);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        rc = zoo_exists(zk, "/parent", 0, &stat_b);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        CPPUNIT_ASSERT(Stat_eq(&stat_a, &stat_b));
        CPPUNIT_ASSERT(stat_a.numChildren == 4);
    }

    void testPath() {
        watchctx_t ctx;
        char pathbuf[20];
        zhandle_t *zk = createClient(&ctx);
        CPPUNIT_ASSERT(zk);
        int rc = 0;

        memset(pathbuf, 'X', 20);
        rc = zoo_create(zk, "/testpathpath0", "", 0,
                        openAcl, 0, pathbuf, 0);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT_EQUAL('X', pathbuf[0]);

        rc = zoo_create(zk, "/testpathpath1", "", 0,
                        openAcl, 0, pathbuf, 1);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT(strlen(pathbuf) == 0);

        rc = zoo_create(zk, "/testpathpath2", "", 0,
                        openAcl, 0, pathbuf, 2);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT(strcmp(pathbuf, "/") == 0);

        rc = zoo_create(zk, "/testpathpath3", "", 0,
                        openAcl, 0, pathbuf, 3);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT(strcmp(pathbuf, "/t") == 0);

        rc = zoo_create(zk, "/testpathpath7", "", 0,
                        openAcl, 0, pathbuf, 15);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT(strcmp(pathbuf, "/testpathpath7") == 0);

        rc = zoo_create(zk, "/testpathpath8", "", 0,
                        openAcl, 0, pathbuf, 16);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        CPPUNIT_ASSERT(strcmp(pathbuf, "/testpathpath8") == 0);
    }

    void testPathValidation() {
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        CPPUNIT_ASSERT(zk);

        verifyCreateFails(0, zk);
        verifyCreateFails("", zk);
        verifyCreateFails("//", zk);
        verifyCreateFails("///", zk);
        verifyCreateFails("////", zk);
        verifyCreateFails("/.", zk);
        verifyCreateFails("/..", zk);
        verifyCreateFails("/./", zk);
        verifyCreateFails("/../", zk);
        verifyCreateFails("/foo/./", zk);
        verifyCreateFails("/foo/../", zk);
        verifyCreateFails("/foo/.", zk);
        verifyCreateFails("/foo/..", zk);
        verifyCreateFails("/./.", zk);
        verifyCreateFails("/../..", zk);
        verifyCreateFails("/foo/bar/", zk);
        verifyCreateFails("/foo//bar", zk);
        verifyCreateFails("/foo/bar//", zk);

        verifyCreateFails("foo", zk);
        verifyCreateFails("a", zk);

        // verify that trailing fails, except for seq which adds suffix
        verifyCreateOk("/createseq", zk);
        verifyCreateFails("/createseq/", zk);
        verifyCreateOkSeq("/createseq/", zk);
        verifyCreateOkSeq("/createseq/.", zk);
        verifyCreateOkSeq("/createseq/..", zk);
        verifyCreateFailsSeq("/createseq//", zk);
        verifyCreateFailsSeq("/createseq/./", zk);
        verifyCreateFailsSeq("/createseq/../", zk);

        verifyCreateOk("/.foo", zk);
        verifyCreateOk("/.f.", zk);
        verifyCreateOk("/..f", zk);
        verifyCreateOk("/..f..", zk);
        verifyCreateOk("/f.c", zk);
        verifyCreateOk("/f", zk);
        verifyCreateOk("/f/.f", zk);
        verifyCreateOk("/f/f.", zk);
        verifyCreateOk("/f/..f", zk);
        verifyCreateOk("/f/f..", zk);
        verifyCreateOk("/f/.f/f", zk);
        verifyCreateOk("/f/f./f", zk);
    }


    void testAsyncWatcherAutoReset()
    {
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        watchctx_t lctx[COUNT];
        int i;
        char path[80];
        int rc;
        evt_t evt;

        async_zk = zk;
        for(i = 0; i < COUNT; i++) {
            sprintf(path, "/awar%d", i);
            rc = zoo_awexists(zk, path, watcher, &lctx[i], statCompletion, (void*)ZNONODE);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        }

        yield(zk, 0);

        for(i = 0; i < COUNT/2; i++) {
            sprintf(path, "/awar%d", i);
            rc = zoo_acreate(zk, path, "", 0,  openAcl, 0, stringCompletion, strdup(path));
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        }

        yield(zk, 3);
        for(i = 0; i < COUNT/2; i++) {
            sprintf(path, "/awar%d", i);
            CPPUNIT_ASSERT_MESSAGE(path, waitForEvent(zk, &lctx[i], 5));
            evt = lctx[i].getEvent();
            CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path.c_str(), ZOO_CREATED_EVENT, evt.type);
            CPPUNIT_ASSERT_EQUAL(string(path), evt.path);
        }

        for(i = COUNT/2 + 1; i < COUNT*10; i++) {
            sprintf(path, "/awar%d", i);
            rc = zoo_acreate(zk, path, "", 0,  openAcl, 0, stringCompletion, strdup(path));
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        }

        yield(zk, 1);
        stopServer();
        CPPUNIT_ASSERT(ctx.waitForDisconnected(zk));
        startServer();
        CPPUNIT_ASSERT(ctx.waitForConnected(zk));
        yield(zk, 3);
        for(i = COUNT/2+1; i < COUNT; i++) {
            sprintf(path, "/awar%d", i);
            CPPUNIT_ASSERT_MESSAGE(path, waitForEvent(zk, &lctx[i], 5));
            evt = lctx[i].getEvent();
            CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_CREATED_EVENT, evt.type);
            CPPUNIT_ASSERT_EQUAL(string(path), evt.path);
        }
    }

    void testWatcherAutoReset(zhandle_t *zk, watchctx_t *ctxGlobal,
                              watchctx_t *ctxLocal)
    {
        bool isGlobal = (ctxGlobal == ctxLocal);
        int rc;
        struct Stat stat;
        char buf[1024];
        int blen;
        struct String_vector strings;
        const char *testName;

        rc = zoo_create(zk, "/watchtest", "", 0,
                        openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        rc = zoo_create(zk, "/watchtest/child", "", 0,
                        openAcl, ZOO_EPHEMERAL, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        if (isGlobal) {
            testName = "GlobalTest";
            rc = zoo_get_children(zk, "/watchtest", 1, &strings);
            deallocate_String_vector(&strings);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            blen = sizeof(buf);
            rc = zoo_get(zk, "/watchtest/child", 1, buf, &blen, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            rc = zoo_exists(zk, "/watchtest/child2", 1, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
        } else {
            testName = "LocalTest";
            rc = zoo_wget_children(zk, "/watchtest", watcher, ctxLocal,
                                 &strings);
            deallocate_String_vector(&strings);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            blen = sizeof(buf);
            rc = zoo_wget(zk, "/watchtest/child", watcher, ctxLocal,
                         buf, &blen, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            rc = zoo_wexists(zk, "/watchtest/child2", watcher, ctxLocal,
                            &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZNONODE, rc);
        }

        CPPUNIT_ASSERT(ctxLocal->countEvents() == 0);

        stopServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxGlobal->waitForDisconnected(zk));
        startServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxLocal->waitForConnected(zk));

        CPPUNIT_ASSERT(ctxLocal->countEvents() == 0);

        rc = zoo_set(zk, "/watchtest/child", "1", 1, -1);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        struct Stat stat1, stat2;
        rc = zoo_set2(zk, "/watchtest/child", "1", 1, -1, &stat1);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        CPPUNIT_ASSERT(stat1.version >= 0);
        rc = zoo_set2(zk, "/watchtest/child", "1", 1, stat1.version, &stat2);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        rc = zoo_set(zk, "/watchtest/child", "1", 1, stat2.version);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        rc = zoo_create(zk, "/watchtest/child2", "", 0,
                        openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);

        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));

        evt_t evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_CHANGED_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest/child"), evt.path);

        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));
        // The create will trigget the get children and the
        // exists watches
        evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_CREATED_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest/child2"), evt.path);
        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));
        evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_CHILD_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest"), evt.path);

        // Make sure Pings are giving us problems
        sleep(5);

        CPPUNIT_ASSERT(ctxLocal->countEvents() == 0);

        stopServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxGlobal->waitForDisconnected(zk));
        startServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxGlobal->waitForConnected(zk));

        if (isGlobal) {
            testName = "GlobalTest";
            rc = zoo_get_children(zk, "/watchtest", 1, &strings);
            deallocate_String_vector(&strings);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            blen = sizeof(buf);
            rc = zoo_get(zk, "/watchtest/child", 1, buf, &blen, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            rc = zoo_exists(zk, "/watchtest/child2", 1, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        } else {
            testName = "LocalTest";
            rc = zoo_wget_children(zk, "/watchtest", watcher, ctxLocal,
                                 &strings);
            deallocate_String_vector(&strings);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            blen = sizeof(buf);
            rc = zoo_wget(zk, "/watchtest/child", watcher, ctxLocal,
                         buf, &blen, &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
            rc = zoo_wexists(zk, "/watchtest/child2", watcher, ctxLocal,
                            &stat);
            CPPUNIT_ASSERT_EQUAL((int)ZOK, rc);
        }

        zoo_delete(zk, "/watchtest/child2", -1);

        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));

        evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_DELETED_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest/child2"), evt.path);

        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));
        evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_CHILD_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest"), evt.path);

        stopServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxGlobal->waitForDisconnected(zk));
        startServer();
        CPPUNIT_ASSERT_MESSAGE(testName, ctxLocal->waitForConnected(zk));

        zoo_delete(zk, "/watchtest/child", -1);
        zoo_delete(zk, "/watchtest", -1);

        CPPUNIT_ASSERT_MESSAGE(testName, waitForEvent(zk, ctxLocal, 5));

        evt = ctxLocal->getEvent();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(evt.path, ZOO_DELETED_EVENT, evt.type);
        CPPUNIT_ASSERT_EQUAL(string("/watchtest/child"), evt.path);

        // Make sure nothing is straggling
        sleep(1);
        CPPUNIT_ASSERT(ctxLocal->countEvents() == 0);
    }

    void testWatcherAutoResetWithGlobal()
    {
      {
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int rc = zoo_create(zk, "/testarwg", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        rc = zoo_create(zk, "/testarwg/arwg", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
      }

      {
        watchctx_t ctx;
        zhandle_t *zk = createchClient(&ctx, "127.0.0.1:22181/testarwg/arwg");

        testWatcherAutoReset(zk, &ctx, &ctx);
      }
    }

    void testWatcherAutoResetWithLocal()
    {
      {
        watchctx_t ctx;
        zhandle_t *zk = createClient(&ctx);
        int rc = zoo_create(zk, "/testarwl", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
        rc = zoo_create(zk, "/testarwl/arwl", "", 0, openAcl, 0, 0, 0);
        CPPUNIT_ASSERT_EQUAL((int) ZOK, rc);
      }

      {
        watchctx_t ctx;
        watchctx_t lctx;
        zhandle_t *zk = createchClient(&ctx, "127.0.0.1:22181/testarwl/arwl");
        testWatcherAutoReset(zk, &ctx, &lctx);
      }
    }
    #endif
};

volatile int TestClient::count;
zhandle_t *TestClient::async_zk;
const char TestClient::hostPorts[] = "127.0.0.1:22181";
CPPUNIT_TEST_SUITE_REGISTRATION(TestClient);
