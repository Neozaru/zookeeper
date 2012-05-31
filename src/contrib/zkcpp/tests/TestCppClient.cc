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
#include <ZooKeeper.h>
#include <errno.h>
#include <recordio.h>
#include "Util.h"
using namespace boost;
using namespace org::apache::zookeeper;

class TestInitWatch : public Watch {
  public:
    void process(Event event, State state, const std::string& path) {
        printf("event %d, state %d path '%s'\n", event, state, path.c_str());
        if (event == Session) {
          if (state == Connected) {
            {
                boost::lock_guard<boost::mutex> lock(mutex);
                connected = true;
            }
            cond.notify_all();
          } else if (state = SessionAuthFailed) {
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

class MyAuthCallback : public AuthCallback {
  public:
    MyAuthCallback() : completed_(false), scheme_(""), cert_("") {}
    void processResult(ReturnCode rc, const std::string& scheme,
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
    ReturnCode rc_;
    std::string scheme_;
    std::string cert_;
};

class MyCreateCallback : public CreateCallback {
  public:
    void process(ReturnCode rc, const std::string& pathRequested,
                 const std::string& pathCreated) {
      printf("%d %s %s\n", rc, pathRequested.c_str(),
             pathCreated.c_str());
      if (rc == Ok) {
        {
          boost::lock_guard<boost::mutex> lock(mutex);
          created = true;
        }
        cond.notify_all();
      }
    }

    bool waitForCreated(uint32_t timeoutMs) {
        boost::system_time const timeout=boost::get_system_time() +
            boost::posix_time::milliseconds(timeoutMs);

        boost::mutex::scoped_lock lock(mutex);
        while (!created) {
            if(!cond.timed_wait(lock,timeout)) {
                return false;
            }
        }
        return true;
    }

    boost::condition_variable cond;
    boost::mutex mutex;
    bool created;
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
        CPPUNIT_ASSERT_EQUAL(Ok, zk.init(HOST_PORT, 30000, watch));
        CPPUNIT_ASSERT(watch->waitForConnected(1000));
        CPPUNIT_ASSERT_EQUAL(Connected, zk.getState());
        stopServer();
        CPPUNIT_ASSERT_EQUAL(Connecting, zk.getState());
    }

    void testCreate() {
        startServer();
        ZooKeeper zk;
        struct Stat stat;

        shared_ptr<TestInitWatch> watch(new TestInitWatch());
        CPPUNIT_ASSERT_EQUAL(Ok, zk.init(HOST_PORT, 30000, watch));

        shared_ptr<MyCreateCallback> callback(new MyCreateCallback());
        ReturnCode rc = zk.exists("/hello", boost::shared_ptr<Watch>(), &stat);
        CPPUNIT_ASSERT_EQUAL(NoNode, rc);

        zk.create("/hello", "world",  (const ACL_vector*)&OPEN_ACL_UNSAFE,
                  Persistent, callback);
        CPPUNIT_ASSERT(callback->waitForCreated(1000));

        rc = zk.exists("/hello", boost::shared_ptr<Watch>(new TestInitWatch()),
                       NULL);
        CPPUNIT_ASSERT_EQUAL(Ok, rc);

        rc = zk.exists("/hello", boost::shared_ptr<Watch>(new TestInitWatch()),
                       &stat);
        CPPUNIT_ASSERT_EQUAL(Ok, rc);
        CPPUNIT_ASSERT_EQUAL(stat.czxid, stat.mzxid);
        CPPUNIT_ASSERT_EQUAL(0, stat.version);
        CPPUNIT_ASSERT_EQUAL(0, stat.cversion);
        CPPUNIT_ASSERT_EQUAL(0, stat.aversion);
        CPPUNIT_ASSERT_EQUAL(0, (int)stat.ephemeralOwner);
        CPPUNIT_ASSERT_EQUAL(5, stat.dataLength);
        CPPUNIT_ASSERT_EQUAL(0, stat.numChildren);

        // Test authentication.
        boost::shared_ptr<MyAuthCallback> authCallback(new MyAuthCallback());
        std::string scheme = "digest";
        std::string cert = "user:password";

        CPPUNIT_ASSERT_EQUAL(Ok, zk.addAuthInfo(scheme, cert, authCallback));
        CPPUNIT_ASSERT(authCallback->waitForCompleted(30000));
        CPPUNIT_ASSERT_EQUAL(Ok, authCallback->rc_);
        CPPUNIT_ASSERT_EQUAL(scheme, authCallback->scheme_);
        CPPUNIT_ASSERT_EQUAL(cert, authCallback->cert_);
        CPPUNIT_ASSERT_EQUAL(Connected, zk.getState());

        scheme = "bogus";
        cert = "cert";
        authCallback.reset(new MyAuthCallback());
        watch->authFailed_= false;
        CPPUNIT_ASSERT_EQUAL(Ok, zk.addAuthInfo(scheme, cert, authCallback));
        CPPUNIT_ASSERT(authCallback->waitForCompleted(30000));
        CPPUNIT_ASSERT_EQUAL(AuthFailed, authCallback->rc_);
        CPPUNIT_ASSERT_EQUAL(scheme, authCallback->scheme_);
        CPPUNIT_ASSERT_EQUAL(cert, authCallback->cert_);
        CPPUNIT_ASSERT(watch->waitForAuthFailed(30000));
        CPPUNIT_ASSERT_EQUAL(SessionAuthFailed, zk.getState());

        stopServer();
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(TestCppClient);
