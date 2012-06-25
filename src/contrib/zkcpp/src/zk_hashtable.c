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

#include <zookeeper/logging.hh>
ENABLE_LOGGING;
#include "zk_hashtable.h"
#include "zk_adaptor.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <boost/foreach.hpp>

watcher_object_t* getFirstWatcher(zk_hashtable* ht,const char* path)
{
    boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
    itr = ht->map.find(path);
    if (itr == ht->map.end()) {
        return 0;
    }
    return itr->second->head;
}
/* end of testing functions */

watcher_object_t* clone_watcher_object(watcher_object_t* wo)
{
    return new watcher_object_t(wo->watch_);
}

static watcher_object_list_t* create_watcher_object_list(watcher_object_t* head) 
{
    watcher_object_list_t* wl=(watcher_object_list_t*)calloc(1,sizeof(watcher_object_list_t));
    assert(wl);
    wl->head=head;
    return wl;
}

static void destroy_watcher_object_list(watcher_object_list_t* list)
{
    watcher_object_t* e = NULL;

    if(list==0)
        return;
    e=list->head;
    while(e!=0){
        watcher_object_t* temp=e;
        e=e->next_;
        delete temp;
    }
    free(list);
}

static void do_clean_hashtable(zk_hashtable* ht)
{
    boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
    for (itr = ht->map.begin(); itr != ht->map.end(); itr++) {
        LOG_DEBUG("Deleting a watch for path: " << itr->first);
        destroy_watcher_object_list(itr->second);
    }
    ht->map.clear();
}

void destroy_zk_hashtable(zk_hashtable* ht)
{
    if(ht!=0){
        do_clean_hashtable(ht);
    }
}

// searches for a watcher object instance in a watcher object list;
// two watcher objects are equal if their watcher function and context pointers
// are equal
static watcher_object_t* search_watcher(watcher_object_list_t** wl,watcher_object_t* wo)
{
    watcher_object_t* wobj=(*wl)->head;
    while(wobj!=0){
        if(wobj->watch_.get() == wo->watch_.get()) {
            return wobj;
        }
        wobj=wobj->next_;
    }
    return 0;
}

static void
add_to_list(watcher_object_list_t **wl, watcher_object_t *wo) {
  assert(search_watcher(wl, wo) == 0);
  wo->next_ = (*wl)->head;
  (*wl)->head = wo;
}

void
collectKeys(zk_hashtable *ht, std::vector<std::string>& keys) {
  keys.clear();
  boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
  for (itr = ht->map.begin(); itr != ht->map.end(); itr++) {
    keys.push_back(itr->first);
  }
}

static void
insert_watcher_object(zk_hashtable *ht, const std::string& path,
    watcher_object_t* wo) {
  boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
  itr = ht->map.find(path);
  if (itr == ht->map.end()) {
    ht->map[path] = create_watcher_object_list(wo);
  } else {
    add_to_list(&(itr->second), wo);
  }
}

static void
copy_watchers(watcher_object_list_t *from,
    std::list<watcher_object_t*>& to) {
  watcher_object_t* wo = from->head;
  while (wo != NULL) {
    watcher_object_t *next = wo->next_;
    to.push_back(wo);
    wo=next;
  }
}

static void copy_table(zk_hashtable *from,
    std::list<watcher_object_t*>& watches) {
  boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
  for (itr = from->map.begin(); itr != from->map.end(); itr++) {
    assert(itr->second);
    copy_watchers(itr->second, watches);
  }
}

static void collect_session_watchers(zhandle_t *zh,
    std::list<watcher_object_t*>& watches) {
  copy_table(&zh->active_node_watchers, watches);
  copy_table(&zh->active_exist_watchers, watches);
  copy_table(&zh->active_child_watchers, watches);
}

static void add_for_event(zk_hashtable *ht, const std::string& path,
    std::list<watcher_object_t*>& watches) {
  boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
  itr = ht->map.find(path);
  if (itr == ht->map.end()) {
    return;
  }
  copy_watchers(itr->second, watches);
  free(itr->second);
  ht->map.erase(itr);
}


void collectWatchers(zhandle_t *zh, int type, const std::string& path,
    std::list<watcher_object_t*>& watches) {
  if(type==ZOO_SESSION_EVENT){
    if (zh->watch.get() != NULL) {
      watcher_object_t* defWatcher = new watcher_object_t(zh->watch);
      watches.push_back(defWatcher);
    }
    collect_session_watchers(zh, watches);
    return;
  }
  switch(type){
    case CREATED_EVENT_DEF:
    case CHANGED_EVENT_DEF:
      // look up the watchers for the path and move them to a delivery list
      add_for_event(&zh->active_node_watchers, path, watches);
      add_for_event(&zh->active_exist_watchers, path, watches);
      break;
    case CHILD_EVENT_DEF:
      // look up the watchers for the path and move them to a delivery list
      add_for_event(&zh->active_child_watchers, path, watches);
      break;
    case DELETED_EVENT_DEF:
      // look up the watchers for the path and move them to a delivery list
      add_for_event(&zh->active_node_watchers, path, watches);
      add_for_event(&zh->active_exist_watchers, path, watches);
      add_for_event(&zh->active_child_watchers, path, watches);
      break;
  }
}

void deliverWatchers(zhandle_t *zh, int type, int state, const char *path,
                     std::list<watcher_object_t*>& watches) {
  // session event's don't have paths
  std::string client_path =
    type == ZOO_SESSION_EVENT ? path : stripChroot(path, zh->chroot);
  BOOST_FOREACH(watcher_object_t* watch, watches) {
    watch->watch_.get()->process((WatchEvent::type)type,
        (SessionState::type)state, client_path);
    if (type != ZOO_SESSION_EVENT) {
      delete watch;
    }
  }
  // TODO fix this hack.
  if (type == ZOO_SESSION_EVENT && !watches.empty()) {
    delete *(watches.begin());
  }
}

void activateWatcher(zhandle_t* zh, watcher_registration_t* reg, int rc) {
  if (reg != NULL) {
    // This code is executed by the IO thread
    zk_hashtable *ht = reg->checker(zh, rc);
    if (ht != NULL) {
      insert_watcher_object(ht,reg->path,
          new watcher_object_t(reg->watch));
    }
  }
}
