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
    virtual ~Op() = 0;

    int32_t getType() const;
    std::string getPath() const;

    class Create;
    class Remove;
    class SetData;
    class Check;

  protected:
    Op(int32_t type, const std::string& path);

  private:
    const int32_t type_;
    const std::string path_;
};

class Op::Create : public Op {
  public:
    Create(const std::string& path, const std::string& data,
           const std::vector<data::ACL>& acl, CreateMode::type mode);
    virtual ~Create();
    const std::string& getData() const;
    const std::vector<data::ACL>& getAcl() const;
    CreateMode::type getMode() const;

  private:
    Create();
    const std::string data_;
    const std::vector<data::ACL> acl_;
    const CreateMode::type mode_;
};

class Op::Remove : public Op {
  public:
    Remove(const std::string& path, int32_t version);
    virtual ~Remove();
    int32_t getVersion() const;

  private:
    Remove();
    const int32_t version_;
};

class Op::SetData : public Op {
  public:
    SetData(const std::string& path, const std::string& data,
            int32_t version);
    const std::string& getData() const;
    int32_t getVersion() const;
    virtual ~SetData();

  private:
    SetData();
    const std::string data_;
    const int32_t version_;
};

class Op::Check : public Op {
  public:
    Check(const std::string& path, int32_t version);
    virtual ~Check();
    int32_t getVersion() const;
  private:
    Check();
    const int32_t version_;
};

class OpResult {
  public:
    virtual ~OpResult() = 0;

    int32_t getType() const;

    class Create;
    class Remove;
    class SetData;
    class Check;

  protected:
    OpResult(int32_t type);

  private:
    int32_t type_;
};

class OpResult::Create : public OpResult {
  public:
    Create(const std::string& pathCreated);
    virtual ~Create();
    const std::string getPathCreated() const;

  private:
    Create();
    std::string pathCreated_;
};

class OpResult::Remove : public OpResult {
  public:
    Remove();
    virtual ~Remove();
};

class OpResult::SetData : public OpResult {
  public:
    explicit SetData(const data::Stat& stat);
    virtual ~SetData();

  private:
    SetData();
    data::Stat stat_;
};

class OpResult::Check : public OpResult {
  public:
    Check();
    virtual ~Check();
};

}}}

#endif  //SRC_CONTRIB_ZKCPP_INCLUDE_ZOOKEEPER_MULTI_H_
