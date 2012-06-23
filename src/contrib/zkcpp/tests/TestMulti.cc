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
#include "zookeeper/logging.hh"
ENABLE_LOGGING;

#include "zookeeper.h"
#include <zookeeper/zookeeper.hh>
using namespace boost;
using namespace org::apache::zookeeper;

class TestMulti: public CPPUNIT_NS::TestFixture
{
  CPPUNIT_TEST_SUITE(TestMulti);
  CPPUNIT_TEST(testCreate);
  CPPUNIT_TEST(testCreateFailure);
  CPPUNIT_TEST(testCreateDelete);
  CPPUNIT_TEST(testInvalidVersion);
  CPPUNIT_TEST(testNestedCreate);
  CPPUNIT_TEST(testSetData);
  CPPUNIT_TEST(testUpdateConflict);
  CPPUNIT_TEST(testDeleteUpdateConflict);
  CPPUNIT_TEST(testCheck);
  CPPUNIT_TEST(testWatch);
  CPPUNIT_TEST_SUITE_END();
  const std::string HOSTPORT;
  std::vector<data::ACL> acl;
  public:

  TestMulti() : HOSTPORT("127.0.0.1:22181") {
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

  /**
   * Test basic multi-op create functionality
   */
  void testCreate() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi1", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi1/a", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi1/b", "", acl, CreateMode::Persistent));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(3, (int)results.size());

    CPPUNIT_ASSERT_EQUAL(OpCode::Create, results[0].getType());
    OpResult::Create& res = dynamic_cast<OpResult::Create&>(results[0]);
    CPPUNIT_ASSERT_EQUAL(std::string("/multi1"), res.getPathCreated());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, res.getReturnCode());

    CPPUNIT_ASSERT_EQUAL(OpCode::Create, results[1].getType());
    res = dynamic_cast<OpResult::Create&>(results[1]);
    CPPUNIT_ASSERT_EQUAL(std::string("/multi1/a"), res.getPathCreated());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, res.getReturnCode());

    CPPUNIT_ASSERT_EQUAL(OpCode::Create, results[2].getType());
    res = dynamic_cast<OpResult::Create&>(results[2]);
    CPPUNIT_ASSERT_EQUAL(std::string("/multi1/b"), res.getPathCreated());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, res.getReturnCode());
  }

  /**
   * Test create failure.
   */
  void testCreateFailure() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi2", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi2/a", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi2/a", "", acl, CreateMode::Persistent));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NodeExists, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(3, (int)results.size());
  }


  /**
   * Test create followed by delete.
   */
  void testCreateDelete() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi5", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Remove("/multi5", -1));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(2, (int)results.size());

    // '/multi5' should have been deleted
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, zk.exists("/multi5", boost::shared_ptr<Watch>(), stat));
  }

  /**
   * Test nested creates that rely on state in earlier op in multi
   */
  void testNestedCreate() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
      shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi5", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi5/a", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi5/a/1", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Remove("/multi5/a/1", 0));
    ops.push_back(new Op::Remove("/multi5/a", 0));
    ops.push_back(new Op::Remove("/multi5", 0));

    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(6, (int)results.size());

    // '/multi5' should have been deleted
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
        zk.exists("/multi5/a/1", boost::shared_ptr<Watch>(), stat));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
        zk.exists("/multi5/a", boost::shared_ptr<Watch>(), stat));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
        zk.exists("/multi5", boost::shared_ptr<Watch>(), stat));
  }

  /**
   * Test setdata functionality
   */
  void testSetData() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated, data;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi6", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi6/a", "", acl, CreateMode::Persistent));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(2, (int)results.size());

    ops.clear();
    ops.push_back(new Op::SetData("/multi6", "1", 0));
    ops.push_back(new Op::SetData("/multi6/a", "2", 0));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(2, (int)results.size());

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
      zk.get("/multi6", boost::shared_ptr<Watch>(), data, stat));
    CPPUNIT_ASSERT_EQUAL(std::string("1"), data);
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
      zk.get("/multi6/a", boost::shared_ptr<Watch>(), data, stat));
    CPPUNIT_ASSERT_EQUAL(std::string("2"), data);
  }

  /**
   * Test update conflicts
   */
  void testUpdateConflict() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated, data;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi7", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::SetData("/multi7", "X", 0));
    ops.push_back(new Op::SetData("/multi7", "Y", 0));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(3, (int)results.size());

    //Updating version solves conflict -- order matters
    dynamic_cast<Op::SetData&>(ops[2]).setVersion(1);
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(3, (int)results.size());

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
      zk.get("/multi7", boost::shared_ptr<Watch>(), data, stat));
    CPPUNIT_ASSERT_EQUAL(std::string("Y"), data);
  }

  /**
   * Test invalid versions
   */
  void testInvalidVersion() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi3", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Remove("/multi3", 1));
    ops.push_back(new Op::Create("/multi3", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Create("/multi3/a", "", acl, CreateMode::Persistent));

    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(4, (int)results.size());

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[0].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, results[1].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::RuntimeInconsistency,
        results[2].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::RuntimeInconsistency,
        results[3].getReturnCode());
  }

  /**
   * Test delete-update conflicts
   */
  void testDeleteUpdateConflict() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));

    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Create("/multi8", "", acl, CreateMode::Persistent));
    ops.push_back(new Op::Remove("/multi8", 0));
    ops.push_back(new Op::SetData("/multi8", "X", 0));

    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(3, (int)results.size());

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[0].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[1].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode, results[2].getReturnCode());

    // '/multi' should never have been created as entire op should fail
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
        zk.exists("/multi8", boost::shared_ptr<Watch>(), stat));
  }

  /**
   * Test basic multi-op check functionality
   */
  void testCheck() {
    ZooKeeper zk;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
        shared_ptr<Watch>()));
    LOG_DEBUG("INIT DONE");
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.create("/multi0", "", acl, CreateMode::Persistent, pathCreated));
    LOG_DEBUG("CREATE DONE");

    // Conditionally create /multi0/a' only if '/multi0' at version 0
    boost::ptr_vector<Op> ops;
    ops.push_back(new Op::Check("/multi0", 0));
    ops.push_back(new Op::Create("/multi0/a", "", acl, CreateMode::Persistent));
    boost::ptr_vector<OpResult> results;
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(2, (int)results.size());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[0].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[1].getReturnCode());

    // '/multi0/a' should have been created as it passed version check
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.exists("/multi0/a", boost::shared_ptr<Watch>(), stat));

    // Only create '/multi0/b' if '/multi0' at version 10 (which it's not)
    ops.clear();
    ops.push_back(new Op::Check("/multi0", 10));
    ops.push_back(new Op::Create("/multi0/b", "", acl, CreateMode::Persistent));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, zk.multi(ops, results));
    CPPUNIT_ASSERT_EQUAL(2, (int)results.size());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::BadVersion, results[0].getReturnCode());
    CPPUNIT_ASSERT_EQUAL(ReturnCode::RuntimeInconsistency,
        results[1].getReturnCode());

    // '/multi0/b' should NOT have been created
    CPPUNIT_ASSERT_EQUAL(ReturnCode::NoNode,
        zk.exists("/multi0/b", boost::shared_ptr<Watch>(), stat));
  }

  class MultiWatch : public Watch {
    public:
    ZooKeeper& zk_;
    MultiWatch(ZooKeeper& zk) : zk_(zk) {}
    void process(WatchEvent::type event, SessionState::type state,
        const std::string& path) {
      std::string data;
      data::Stat stat;
      boost::ptr_vector<Op> ops;
      ops.push_back(new Op::SetData("/multiwatch", "X", 1));
      boost::ptr_vector<OpResult> results;
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk_.multi(ops, results));
      CPPUNIT_ASSERT_EQUAL(1, (int)results.size());
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, results[0].getReturnCode());
      CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
          zk_.get("/multiwatch", boost::shared_ptr<Watch>(), data, stat));
      CPPUNIT_ASSERT_EQUAL(std::string("X"), data);
    }
  };

  /**
   * Test multi-op called from a watch
   */
  void testWatch() {
    ZooKeeper zk;
    std::string data;
    data::Stat stat;
    std::string pathCreated;
    std::vector<data::ACL> acl;
    data::ACL temp;
    temp.getid().getscheme() = "world";
    temp.getid().getid() = "anyone";
    temp.setperms(Permission::All);
    acl.push_back(temp);

    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.init(HOSTPORT, 30000,
          shared_ptr<Watch>()));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.create("/multiwatch", "", acl, CreateMode::Persistent, pathCreated));

    boost::shared_ptr<MultiWatch> multiWatch(new MultiWatch(zk));
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok, zk.get("/multiwatch", multiWatch, data, stat));

    // setdata on node '/multiwatch' this should trip the watch
    CPPUNIT_ASSERT_EQUAL(ReturnCode::Ok,
        zk.set("/multiwatch", "", -1, stat));
    sleep(1);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(TestMulti);
