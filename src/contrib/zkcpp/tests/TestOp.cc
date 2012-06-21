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
#include <boost/ptr_container/ptr_vector.hpp>
#include <cppunit/extensions/HelperMacros.h>
#include <zookeeper/zookeeper_multi.hh>

using namespace org::apache::zookeeper;

class TestOp : public CPPUNIT_NS::TestFixture
{
  public:
    CPPUNIT_TEST_SUITE(TestOp);
    CPPUNIT_TEST(testOp);
    CPPUNIT_TEST(testOpResult);
    CPPUNIT_TEST_SUITE_END();

    void testOp() {
      boost::ptr_vector<Op> ops;
      ops.push_back(new Op::Check("/path", -1));
      ops.push_back(new Op::Remove("/path", -1));
    }

    void testOpResult() {
      boost::ptr_vector<OpResult> results;
      results.push_back(new OpResult::Create(ReturnCode::Ok, "/path"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestOp);
