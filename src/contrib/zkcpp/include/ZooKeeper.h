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
#ifndef SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_
#define SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_

#include <stdint.h>
#include <string>

namespace org { namespace apache { namespace zookeeper {

class ZooKeeperImpl;

class ZooKeeper {
  public:
    ZooKeeper();
    ~ZooKeeper();
    int64_t getSessionId();
    std::string getSessionPassword();

  private:
    ZooKeeperImpl* impl_;
};
}}}

#endif  // SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_H_
