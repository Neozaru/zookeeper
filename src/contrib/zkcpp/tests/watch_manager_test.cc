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
#include <gtest/gtest.h>
#include <zookeeper/logging.hh>
ENABLE_LOGGING;

#include "watch_manager.hh"
#include "zk_server.hh"

using namespace boost;
using namespace org::apache::zookeeper;

TEST(WatchManager, negative) {
  std::vector<std::string> paths;
  shared_ptr<WatchManager> manager(new WatchManager());
  ExistsWatchRegistration exists(manager, "/path", shared_ptr<Watch>());
  GetDataWatchRegistration data(manager, "/path", shared_ptr<Watch>());
  GetChildrenWatchRegistration children(manager, "/path", shared_ptr<Watch>());

  // Make sure activate fails on invalid return codes.
  EXPECT_FALSE(exists.activate(ReturnCode::InvalidState));
  EXPECT_FALSE(exists.activate(ReturnCode::BadArguments));
  EXPECT_FALSE(exists.activate(ReturnCode::ConnectionLoss));
  EXPECT_FALSE(exists.activate(ReturnCode::NodeExists));

  EXPECT_FALSE(data.activate(ReturnCode::NoNode));
  EXPECT_FALSE(data.activate(ReturnCode::InvalidState));
  EXPECT_FALSE(data.activate(ReturnCode::BadArguments));
  EXPECT_FALSE(data.activate(ReturnCode::ConnectionLoss));
  EXPECT_FALSE(data.activate(ReturnCode::NodeExists));

  EXPECT_FALSE(children.activate(ReturnCode::NoNode));
  EXPECT_FALSE(children.activate(ReturnCode::InvalidState));
  EXPECT_FALSE(children.activate(ReturnCode::BadArguments));
  EXPECT_FALSE(children.activate(ReturnCode::ConnectionLoss));
  EXPECT_FALSE(children.activate(ReturnCode::NodeExists));

  // No watches should have been activated
  manager->getExistsPaths(paths);
  EXPECT_TRUE(paths.empty());
  manager->getGetDataPaths(paths);
  EXPECT_TRUE(paths.empty());
  manager->getGetChildrenPaths(paths);
  EXPECT_TRUE(paths.empty());
}

TEST(WatchManager, activate) {
  std::vector<std::string> paths;
  shared_ptr<WatchManager> manager(new WatchManager());
  ExistsWatchRegistration exists(manager, "/path", shared_ptr<Watch>());

  // This should get added to getDataWatches_.
  EXPECT_TRUE(exists.activate(ReturnCode::Ok));

  // This should get added to existsWatches_.
  EXPECT_TRUE(exists.activate(ReturnCode::NoNode));

  // getDataPaths should contain 1 path.
  manager->getGetDataPaths(paths);
  EXPECT_EQ(1, paths.size());
  EXPECT_EQ("/path", paths[0]);

  // existsPaths should contain 1 path.
  manager->getExistsPaths(paths);
  EXPECT_EQ(1, paths.size());
  EXPECT_EQ("/path", paths[0]);

  // getChildrenPaths should be empty.
  manager->getGetChildrenPaths(paths);
  EXPECT_TRUE(paths.empty());
}
