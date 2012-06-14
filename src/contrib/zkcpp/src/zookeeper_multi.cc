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
Op() {
}

Op::
Op(int32_t type, const std::string& path) : type_(type), path_(path) {
}

Op::
~Op() {
}

int32_t Op::
getType() {
  return type_;
}

std::string Op::
getPath() {
  return path_;
}

Op::Create::
Create(const std::string& path, const std::string& data,
       const std::vector<data::ACL>& acl, CreateMode::type mode) {
}

Op::Create::
~Create() {
}

Op::Remove::
Remove(const std::string& path, int32_t version) {
}

Op::Remove::
~Remove() {
}

Op::SetData::
SetData(const std::string& path, const std::string& data, int32_t version) {
}

Op::SetData::
~SetData() {
}

Op::Check::
Check(const std::string& path, int32_t version) {
}

Op::Check::
~Check() {
}

}}}  // namespace org::apache::zookeeper
