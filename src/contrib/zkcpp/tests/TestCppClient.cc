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
#include <boost/thread/condition.hpp>
#include <cppunit/extensions/HelperMacros.h>
#include "CppAssertHelper.h"
#include "logging.hh"
ENABLE_LOGGING;

#include <stdlib.h>
#include <unistd.h>

#include "CollectionUtil.h"
#include "ThreadingUtil.h"

using namespace Util;

#include "Vector.h"
using namespace std;

#include <cstring>
#include <list>

#include <zookeeper.h>
#include <zookeeper.hh>
#include <errno.h>
#include <recordio.h>
#include "Util.h"
using namespace boost;
using namespace org::apache::zookeeper;

class TestInitWatch : public Watch {
  public:
    void process(WatchEvent::type event, SessionState::type state,
                 const std::string& path) {
        if (event == WatchEvent::SessionStateChanged) {
          if (state == SessionState::Connected) {
            {
                boost::lock_guard<boost::mutex> lock(mutex);
                connected = true;
            }
            cond.notify_all();
          } else if (state = SessionState::AuthFailed) {
            {
                boost::lock_guard<boost::mutex> lock(mutex);
                authFailed_ = true;
            }
            cond.notify_all();
          }
        }
    }

    bool waitForConnected(uint32_t timeoutMs) {
        boost::system_time const timeout=boost::get_system_time() +
            boost::posix_time::milliseconds(timeoutMs);

        boost::mutex::scoped_lock lock(mutex);
        while (!connected) {
            if(!cond.timed_wait(lock,timeout)) {
                return false;
            }
        }
        return true;
    }

    bool waitForAuthFailed(uint32_t timeoutMs) {
        boost::system_time const timeout=boost::get_system_time() +
            boost::posix_time::milliseconds(timeoutMs);

        boost::mutex::scoped_lock lock(mutex);
        while (!authFailed_) {
            if(!cond.timed_wait(lock,timeout)) {
                return false;
            }
        }
        return true;
    }

    boost::condition_variable cond;
    boost::mutex mutex;
    bool connected;
    bool authFailed_;
};

class TestCppClient : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(TestCppClient);
    CPPUNIT_TEST(testInit);
    CPPUNIT_TEST(testCreate);
    CPPUNIT_TEST(testBasic);
    CPPUNIT_TEST(testAcl);
    CPPUNIT_TEST(testAddAuth);
    CPPUNIT_TEST_SUITE_END();
    FILE *logfile;
    const std::string HOSTPORT;

public:

    TestCppClient() : HOSTPORT("127.0.0.1:22181") {
        logfile = openlogfile("TestCppClient");
    }

    ~TestCppClient() {
      if (logfile) {
        fflush(logfile);
        fclose(logfile);
        logfile = 0;
      }
    }

    void setUp()
    {
        //zoo_set_log_stream(logfile);
        //zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    }

    void startServer() {
        char cmd[1024];
        sprintf(cmd, "%s start %s", ZKSERVER_CMD, HOSTPORT.c_str());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void stopServer() {
        char cmd[1024];
        sprintf(cmd, "%s stop %s", ZKSERVER_CMD, HOSTPORT.c_str());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void tearDown()
    {
    }

    void testInit() {
        startServer();
        ZooKeeper zk;
        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000, watch));
        CPPUNIT_ASSERT(watch->waitForConnected(1000));
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk.getState());
        stopServer();
        CPPUNIT_ASSERT_EQUAL(SessionState::Connecting, zk.getState());
    }

    void testCreate() {
        startServer();
        ZooKeeper zk, zk2;
        ZnodeStat stat;
        std::string pathCreated;
        std::vector<Acl> acls;
        acls.push_back(Acl("world", "anyone", Permission::All));

        CPPUNIT_ASSERT_EQUAL(SessionState::Expired, zk.getState());
        ReturnCode::type rc = zk.create("/hello", "world",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(rc, ReturnCode::InvalidState);

        shared_ptr<TestInitWatch> watch(new TestInitWatch());

        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk2.init("localhost:12346", 30000,
                             watch));
        CPPUNIT_ASSERT_EQUAL(rc, ReturnCode::InvalidState);

        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000, watch));

        rc = zk.exists("/hello", boost::shared_ptr<Watch>(), stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        rc = zk.create("/hello", "world",  acls, CreateMode::Persistent,
                 pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(std::string("/hello"), pathCreated);

        rc = zk.exists("/hello", boost::shared_ptr<Watch>(new TestInitWatch()),
                       stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        rc = zk.exists("/hello", boost::shared_ptr<Watch>(new TestInitWatch()),
                       stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(stat.getCzxid(), stat.getMzxid());
        CPPUNIT_ASSERT_EQUAL(0, stat.getVersion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getCversion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
        CPPUNIT_ASSERT_EQUAL(5, stat.getDataLength());
        CPPUNIT_ASSERT_EQUAL(0, stat.getNumChildren());

        stopServer();
    }

    void testBasic() {
        startServer();
        ZooKeeper zk;
        ZnodeStat stat;
        std::string znodeName = "/testBasic";
        std::string dataInput = "hello";
        std::string dataInput2 = "goodbye";
        std::string dataOutput;
        std::string pathCreated;
        std::vector<std::string> children;
        std::vector<Acl> acls;
        acls.push_back(Acl("world", "anyone", Permission::All));

        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000, watch));

        // exists() on nonexistent znode.
        ReturnCode::type rc = zk.exists(znodeName, boost::shared_ptr<Watch>(),
                                        stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        // get() on onexistent znode.
        rc = zk.get(znodeName, boost::shared_ptr<Watch>(), dataOutput, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        // getChildren() on onexistent znode.
        rc = zk.getChildren(znodeName, boost::shared_ptr<Watch>(),
                            children, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        // set() on onexistent znode.
        rc = zk.set(znodeName, dataInput, -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        // create()
        rc = zk.create(znodeName, dataInput,  acls, CreateMode::Persistent,
                 pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(znodeName, pathCreated);

        // create() on existing znode.
        rc = zk.create(znodeName, dataInput,  acls, CreateMode::Persistent,
                 pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NodeExists, rc);

        // exists()
        rc = zk.exists(znodeName, boost::shared_ptr<Watch>(), stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(stat.getCzxid(), stat.getMzxid());
        CPPUNIT_ASSERT_EQUAL(0, stat.getVersion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getCversion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
        CPPUNIT_ASSERT_EQUAL(5, stat.getDataLength());
        CPPUNIT_ASSERT_EQUAL(0, stat.getNumChildren());

        // get()
        rc = zk.get(znodeName, boost::shared_ptr<Watch>(), dataOutput, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(dataInput, dataOutput);
        CPPUNIT_ASSERT_EQUAL(stat.getCzxid(), stat.getMzxid());
        CPPUNIT_ASSERT_EQUAL(0, stat.getVersion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getCversion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
        CPPUNIT_ASSERT_EQUAL(5, stat.getDataLength());
        CPPUNIT_ASSERT_EQUAL(0, stat.getNumChildren());

       // set() with bad version
        rc = zk.set(znodeName, dataInput2, 10, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, rc);

        // set()
        rc = zk.set(znodeName, dataInput2, 0, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(dataInput, dataOutput);
        CPPUNIT_ASSERT(stat.getCzxid() < stat.getMzxid());
        CPPUNIT_ASSERT_EQUAL(1, stat.getVersion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getCversion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
        CPPUNIT_ASSERT_EQUAL(7, stat.getDataLength());
        CPPUNIT_ASSERT_EQUAL(0, stat.getNumChildren());

        // add some children
        int numChildren = 10;
        for (int i = 0; i < numChildren; i++) {
          std::string child = str(boost::format("%s/child%d") % znodeName % i);
          rc = zk.create(child, dataInput,  acls, CreateMode::Persistent,
                         pathCreated);
          CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
          CPPUNIT_ASSERT_EQUAL(child, pathCreated);
        }

        // getChildren()
        rc = zk.getChildren(znodeName, boost::shared_ptr<Watch>(),
                            children, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        CPPUNIT_ASSERT_EQUAL(10, (int)children.size());
        for (int i = 0; i < numChildren; i++) {
          std::string child = str(boost::format("child%d") % i);
          std::vector<std::string>::iterator itr;
          itr = find(children.begin(), children.end(), child);
          CPPUNIT_ASSERT(itr != children.end());
        }

        CPPUNIT_ASSERT(stat.getCzxid() < stat.getMzxid());
        CPPUNIT_ASSERT_EQUAL(1, stat.getVersion());
        CPPUNIT_ASSERT_EQUAL(numChildren, stat.getCversion());
        CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
        CPPUNIT_ASSERT_EQUAL(7, stat.getDataLength());
        CPPUNIT_ASSERT_EQUAL(numChildren, stat.getNumChildren());

        // remove() with children
        rc = zk.remove(znodeName, 1);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NotEmpty, rc);

        // remove all the children
        for (int i = 0; i < numChildren; i++) {
          std::string child = str(boost::format("%s/child%d") % znodeName % i);
          rc = zk.remove(child, -1);
          CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
          rc = zk.exists(znodeName, boost::shared_ptr<Watch>(), stat);
          CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
          CPPUNIT_ASSERT(stat.getCzxid() < stat.getMzxid());
          CPPUNIT_ASSERT_EQUAL(1, stat.getVersion());
          CPPUNIT_ASSERT_EQUAL(numChildren + i + 1, stat.getCversion());
          CPPUNIT_ASSERT_EQUAL(0, stat.getAversion());
          CPPUNIT_ASSERT_EQUAL(0, (int)stat.getEphemeralOwner());
          CPPUNIT_ASSERT_EQUAL(7, stat.getDataLength());
          CPPUNIT_ASSERT_EQUAL(numChildren - i - 1, stat.getNumChildren());
        }

        // remove() with bad version
        rc = zk.remove(znodeName, 10);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, rc);

        // remove()
        rc = zk.remove(znodeName, 1);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        // remove() nonexistent znode.
        rc = zk.remove(znodeName, 1);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, rc);

        stopServer();
    }

    void testAcl() {
        startServer();
        ZooKeeper zk;
        ZnodeStat stat;
        std::vector<Acl> acl, aclOut;

        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000, watch));
        CPPUNIT_ASSERT(watch->waitForConnected(1000));

        // get acl for root ("/")
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.getAcl("/", acl, stat));
        CPPUNIT_ASSERT_EQUAL(1, (int)acl.size());
        CPPUNIT_ASSERT_EQUAL(std::string("world"), acl[0].getScheme());
        CPPUNIT_ASSERT_EQUAL(std::string("anyone"), acl[0].getExpression());
        zk.set("/", "test", -1, stat);
        acl.clear();
        // echo -n user1:password1 |openssl dgst -sha1 -binary | base64
        acl.push_back(Acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                           Permission::All));
        // echo -n user2:password2 |openssl dgst -sha1 -binary | base64
        acl.push_back(Acl("digest", "user2:lo/iTtNMP+gEZlpUNaCqLYO3i5U=",
                           Permission::All));
        // echo -n user3:password3 |openssl dgst -sha1 -binary | base64
        acl.push_back(Acl("digest", "user3:wr5Y0kEs9nFX3bKrTMKxrlcFeWo=",
                           Permission::All));

        // setAcl() with bad version
        CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, zk.setAcl("/", 10, acl));

        // setAcl()
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.setAcl("/", -1, acl));
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.getAcl("/", aclOut, stat));

        // ACL of "/" has been modified once.
        CPPUNIT_ASSERT_EQUAL(1, stat.getAversion());
        CPPUNIT_ASSERT_EQUAL(acl.size(), aclOut.size());
        for (int i = 0; i < acl.size(); i++) {
          CPPUNIT_ASSERT(std::find(aclOut.begin(), aclOut.end(), acl[i]) !=
                         aclOut.end());
        }
        stopServer();
    }

    void testAddAuth() {
        startServer();
        ZooKeeper zk, zk2;
        std::string pathCreated;
        std::vector<Acl> acls;
        ZnodeStat stat;
        ReturnCode::type rc;

        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        shared_ptr<TestInitWatch> watch2(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000, watch));
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk2.init(HOSTPORT, 30000, watch2));
        CPPUNIT_ASSERT(watch->waitForConnected(1000));
        CPPUNIT_ASSERT(watch2->waitForConnected(1000));

        // Test authentication.
        std::string scheme = "digest";
        std::string cert = "user1:password1";
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.addAuth(scheme, cert));
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk.getState());

        // A ssession can have multiple identities.
        cert = "user2:password2";
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk2.addAuth(scheme, cert));
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk2.getState());
        cert = "user3:password3";
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk2.addAuth(scheme, cert));
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk2.getState());

        // echo -n user1:password1 |openssl dgst -sha1 -binary | base64
        acls.clear();
        acls.push_back(Acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                           Permission::All));
        rc = zk.create("/user1", "hello",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        // echo -n user2:password2 |openssl dgst -sha1 -binary | base64
        acls.clear();
        acls.push_back(Acl("digest", "user2:lo/iTtNMP+gEZlpUNaCqLYO3i5U=",
                           Permission::All));
        rc = zk.create("/user2", "hello",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        // echo -n user3:password3 |openssl dgst -sha1 -binary | base64
        acls.clear();
        acls.push_back(Acl("digest", "user3:wr5Y0kEs9nFX3bKrTMKxrlcFeWo=",
                           Permission::All));
        rc = zk.create("/user3", "hello",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        acls.clear();
        acls.push_back(Acl("auth", "", Permission::All));
        rc = zk2.create("/auth", "hello",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        rc = zk2.set("/auth", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        zk2.getAcl("/auth", acls, stat);

        rc = zk.set("/user1", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        rc = zk.set("/user2", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoAuth, rc);
        rc = zk.set("/user3", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoAuth, rc);

        rc = zk2.set("/user1", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::NoAuth, rc);
        rc = zk2.set("/user2", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        rc = zk2.set("/user3", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        acls.clear();
        acls.push_back(Acl("ip", "127.0.0.1", Permission::All));
        rc = zk2.create("/ip", "hello",  acls,
                                        CreateMode::Persistent, pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);
        zk2.getAcl("/ip", acls, stat);
        rc = zk2.set("/ip", "new data", -1, stat);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, rc);

        stopServer();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestCppClient);
