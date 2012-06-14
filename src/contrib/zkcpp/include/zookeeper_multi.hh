/*
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
#ifndef SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_MULTI_H_
#define SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_MULTI_H_

#include <string>
#include "zookeeper.jute.hh"
#include "zookeeper_const.hh"

namespace org {
namespace apache {

/** ZooKeeper namespace. */
namespace zookeeper {

class Op {
  public:
    virtual ~Op();

    int32_t getType();
    std::string getPath();

    class Create;
    class Remove;
    class SetData;
    class Check;

  protected:
    Op(int32_t type, const std::string& path);

  private:
    Op();
    int32_t type_;
    std::string path_;
};

class Op::Create : public Op {
  public:
    Create(const std::string& path, const std::string& data,
           const std::vector<data::ACL>& acl, CreateMode::type mode);
    Create(const Create& orig);
    Create& operator=(const Create& orig);
    virtual ~Create();

  private:
    Create();
};

class Op::Remove : public Op {
  public:
    Remove(const std::string& path, int32_t version);
    virtual ~Remove();

  private:
    Remove();
};

class Op::SetData : public Op {
  public:
    SetData(const std::string& path, const std::string& data,
            int32_t version);
    virtual ~SetData();

  private:
    SetData();
};

class Op::Check : public Op {
  public:
    Check(const std::string& path, int32_t version);
    virtual ~Check();
  private:
    Check();
};

}}}

#endif  //SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_MULTI_H_
