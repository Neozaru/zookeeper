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

#ifndef ZK_HASHTABLE_H_
#define ZK_HASHTABLE_H_

#include <zookeeper.h>
#include <zookeeper/zookeeper.hh>
#include <boost/unordered_map.hpp>
#include <list>

#ifdef __cplusplus
extern "C" {
#endif

class watcher_object_t {
  public:
    watcher_object_t(boost::shared_ptr<Watch> watch) :
      watch_(watch), next_(NULL) {}
    boost::shared_ptr<Watch> watch_;
    watcher_object_t* next_;
};

class watcher_object_list_t {
  public:
    watcher_object_t* head;
};

class zk_hashtable {
  public:
    ~zk_hashtable();
    boost::unordered_map<std::string, watcher_object_list_t*> map;
};

/**
 * The function must return a non-zero value if the watcher object can be activated
 * as a result of the server response. Normally, a watch can only be activated
 * if the server returns a success code (ZOK). However in the case when zoo_exists() 
 * returns a ZNONODE code the watcher should be activated nevertheless.
 */
typedef zk_hashtable *(*result_checker_fn)(zhandle_t *, int rc);

/**
 * A watcher object gets temporarily stored with the completion entry until 
 * the server response comes back at which moment the watcher object is moved
 * to the active watchers map.
 */
class watcher_registration_t {
  public:
    boost::shared_ptr<Watch> watch;
    result_checker_fn checker;
    std::string path;
};

void collectKeys(zk_hashtable *ht, std::vector<std::string>& keys);

/**
 * check if the completion has a watcher object associated
 * with it. If it does, move the watcher object to the map of
 * active watchers (only if the checker allows to do so)
 */
void activateWatcher(zhandle_t *zh, watcher_registration_t* reg, int rc);
void collectWatchers(zhandle_t *zh, int type, const std::string& path,
                     std::list<watcher_object_t*>& watches);
void deliverWatchers(zhandle_t *zh, int type, int state, const char *path,
                     std::list<watcher_object_t*>& watches);

#ifdef __cplusplus
}
#endif

#endif /*ZK_HASHTABLE_H_*/
