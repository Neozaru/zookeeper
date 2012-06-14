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
#include <string>
#include <vector>
#include <cppunit/extensions/HelperMacros.h>
#include <zookeeper_multi.hh>

using namespace org::apache::zookeeper;

class TestOp : public CPPUNIT_NS::TestFixture
{
  public:
    CPPUNIT_TEST_SUITE(TestOp);
    CPPUNIT_TEST(testOp);
    CPPUNIT_TEST_SUITE_END();

    void testOp() {
      Op::Check checkOp("/path", -1);
      Op::Check checkOp2(checkOp);
      Op::Check checkOp3 = checkOp;
      Op test = checkOp;

      std::vector<Op> ops;
      ops.push_back(Op::Check("/path", -1));
      ops.push_back(Op::Remove("/path", -1));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestOp);
