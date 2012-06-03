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

#include <boost/thread/condition.hpp>
#include <cppunit/extensions/HelperMacros.h>
#include "CppAssertHelper.h"

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
        printf("event %d, state %d path '%s'\n", event, state, path.c_str());
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

class MyAuthCallback : public AddAuthCallback {
  public:
    MyAuthCallback() : completed_(false), scheme_(""), cert_("") {}
    void process(ReturnCode::type rc, const std::string& scheme,
                 const std::string& cert) {
      rc_ = rc;
      scheme_ = scheme;
      cert_ = cert;
      {
        boost::lock_guard<boost::mutex> lock(mutex);
        completed_ = true;
      }
      cond.notify_all();
    }

    bool waitForCompleted(uint32_t timeoutMs) {
        boost::system_time const timeout=boost::get_system_time() +
            boost::posix_time::milliseconds(timeoutMs);

        boost::mutex::scoped_lock lock(mutex);
        while (!completed_) {
            if(!cond.timed_wait(lock,timeout)) {
                return false;
            }
        }
        return true;
    }

    boost::condition_variable cond;
    boost::mutex mutex;
    bool completed_;
    ReturnCode::type rc_;
    std::string scheme_;
    std::string cert_;
};

class TestCppClient : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(TestCppClient);
    CPPUNIT_TEST(testInit);
    CPPUNIT_TEST(testCreate);
    CPPUNIT_TEST_SUITE_END();
    FILE *logfile;
    const std::string HOST_PORT;

public:

    TestCppClient() : HOST_PORT("127.0.0.1:22181") {
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
        sprintf(cmd, "%s start %s", ZKSERVER_CMD, HOST_PORT.c_str());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void stopServer() {
        char cmd[1024];
        sprintf(cmd, "%s stop %s", ZKSERVER_CMD, HOST_PORT.c_str());
        CPPUNIT_ASSERT(system(cmd) == 0);
    }

    void tearDown()
    {
    }

    void testInit() {
        startServer();
        ZooKeeper zk;
        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOST_PORT, 30000, watch));
        CPPUNIT_ASSERT(watch->waitForConnected(1000));
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk.getState());
        stopServer();
        CPPUNIT_ASSERT_EQUAL(SessionState::Connecting, zk.getState());
    }

    void testCreate() {
        startServer();
        ZooKeeper zk;
        ZnodeStat stat;
        std::string pathCreated;
        std::vector<Acl> acls;
        acls.push_back(Acl("world", "anyone", Permission::All));

        CPPUNIT_ASSERT_EQUAL(SessionState::Expired, zk.getState());
        ReturnCode::type rc = zk.create("/hello", "world",  acls,
                                        CreateMode::Persistent, pathCreated);

        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOST_PORT, 30000, watch));

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

        // Test authentication.
        boost::shared_ptr<MyAuthCallback> authCallback(new MyAuthCallback());
        std::string scheme = "digest";
        std::string cert = "user:password";

        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.addAuth(scheme, cert, authCallback));
        CPPUNIT_ASSERT(authCallback->waitForCompleted(30000));
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, authCallback->rc_);
        CPPUNIT_ASSERT_EQUAL(scheme, authCallback->scheme_);
        CPPUNIT_ASSERT_EQUAL(cert, authCallback->cert_);
        CPPUNIT_ASSERT_EQUAL(SessionState::Connected, zk.getState());

        scheme = "bogus";
        cert = "cert";
        authCallback.reset(new MyAuthCallback());
        watch->authFailed_= false;
        CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.addAuth(scheme, cert, authCallback));
        CPPUNIT_ASSERT(authCallback->waitForCompleted(30000));
        CPPUNIT_ASSERT_EQUAL(ReturnCode::AuthFailed, authCallback->rc_);
        CPPUNIT_ASSERT_EQUAL(scheme, authCallback->scheme_);
        CPPUNIT_ASSERT_EQUAL(cert, authCallback->cert_);
        CPPUNIT_ASSERT(watch->waitForAuthFailed(30000));
        CPPUNIT_ASSERT_EQUAL(SessionState::AuthFailed, zk.getState());

        // Now requests will fail with InvalidState.
        rc = zk.create("/hello", "world",  acls, CreateMode::Persistent,
                 pathCreated);
        CPPUNIT_ASSERT_EQUAL(ReturnCode::InvalidState, rc);
        stopServer();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestCppClient);
