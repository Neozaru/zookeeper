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
    void process(Event event, State state, const std::string& path) {
        printf("event %d, state %d\n");
    }
};

class TestCppClient : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(TestCppClient);
    CPPUNIT_TEST(testInit);
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
        zoo_set_log_stream(logfile);
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
        CPPUNIT_ASSERT_EQUAL(OK, zk.init(HOST_PORT, 30000, watch));
        stopServer();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestCppClient);
