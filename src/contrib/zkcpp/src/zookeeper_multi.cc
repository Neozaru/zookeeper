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
#include "zookeeper_multi.hh"

namespace org {
namespace apache {

/** ZooKeeper namespace. */
namespace zookeeper {

Op::
Op(OpCode::type type, const std::string& path) : type_(type), path_(path) {
}

Op::
~Op() {
}

OpCode::type Op::
getType() const {
  return type_;
}

std::string Op::
getPath() const {
  return path_;
}

Op::Create::
Create(const std::string& path, const std::string& data,
       const std::vector<data::ACL>& acl, CreateMode::type mode) :
  Op(OpCode::Create, path), data_(data), acl_(acl), mode_(mode) {
}

Op::Create::
~Create() {
}


const std::string& Op::Create::
getData() const {
  return data_;
}

const std::vector<data::ACL>& Op::Create::
getAcl() const {
  return acl_;
}

CreateMode::type Op::Create::
getMode() const {
  return mode_;
}

Op::Remove::
Remove(const std::string& path, int32_t version) :
  Op(OpCode::Remove, path), version_(version) {
}

Op::Remove::
~Remove() {
}

int32_t Op::Remove::
getVersion() const {
  return version_;
}


Op::SetData::
SetData(const std::string& path, const std::string& data, int32_t version) :
  Op(OpCode::SetData, path), version_(version) {
}

Op::SetData::
~SetData() {
}

const std::string& Op::SetData::
getData() const {
  return data_;
}

int32_t Op::SetData::
getVersion() const {
  return version_;
}

Op::Check::
Check(const std::string& path, int32_t version) :
  Op(OpCode::Check, path), version_(version) {
}

Op::Check::
~Check() {
}

int32_t Op::Check::
getVersion() const {
  return version_;
}

OpResult::
OpResult(OpCode::type type, ReturnCode::type rc) : type_(type), rc_(rc) {
}

OpResult::
~OpResult() {
}

OpCode::type OpResult::
getType() const {
  return type_;
}

ReturnCode::type OpResult::
getReturnCode() const {
  return rc_;
}

void OpResult::
setReturnCode(ReturnCode::type rc) {
  rc_ = rc;
}


OpResult::Create::
Create(ReturnCode::type rc, const std::string& path) :
  OpResult(OpCode::Create, rc), pathCreated_(path) {
}

OpResult::Create::
~Create() {
}

const std::string OpResult::Create::
getPathCreated() const {
  return pathCreated_;
}

OpResult::Remove::
Remove(ReturnCode::type rc) : OpResult(OpCode::Remove, rc) {
}

OpResult::Remove::
~Remove() {
}


}}}  // namespace org::apache::zookeeper
