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

class watcher_object_t {
  public:
    watcher_object_t(watcher_fn watcher, void* context) :
      watcher_(watcher), context_(context), next_(NULL) {}
    watcher_fn watcher_;
    void* context_;
    watcher_object_t* next_;
};

struct watcher_object_list {
    watcher_object_t* head;
};

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
    return new watcher_object_t(wo->watcher_, wo->context_);
}

static watcher_object_t* create_watcher_object(watcher_fn watcher,void* ctx)
{
    return new watcher_object_t(watcher, ctx);
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
        if(wobj->watcher_==wo->watcher_ && wobj->context_==wo->context_)
            return wobj;
        wobj=wobj->next_;
    }
    return 0;
}

static int
add_to_list(watcher_object_list_t **wl, watcher_object_t *wo) {
  assert(search_watcher(wl, wo) == 0);
  wo->next_ = (*wl)->head;
  (*wl)->head = wo;
  return 1;
}

static int do_insert_watcher_object(zk_hashtable *ht, const std::string& path, watcher_object_t* wo)
{
    int res=1;
    boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
    itr = ht->map.find(path);
    if (itr == ht->map.end()) {
        ht->map[path] = create_watcher_object_list(wo);
    } else {
        res = add_to_list(&(itr->second), wo);
    }
    return res;
}


void
collectKeys(zk_hashtable *ht, std::vector<std::string>& keys) {
  keys.clear();
  boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
  for (itr = ht->map.begin(); itr != ht->map.end(); itr++) {
    keys.push_back(itr->first);
  }
}

static int insert_watcher_object(zk_hashtable *ht, const std::string& path,
                                 watcher_object_t* wo)
{
    int res;
    res=do_insert_watcher_object(ht,path,wo);
    return res;
}

static void copy_watchers(watcher_object_list_t *from, watcher_object_list_t *to, int clone)
{
    watcher_object_t* wo=from->head;
    while(wo){
        watcher_object_t *next = wo->next_;
        add_to_list(&to, wo);
        wo=next;
    }
}

static void copy_table(zk_hashtable *from, watcher_object_list_t *to) {
    boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
    for (itr = from->map.begin(); itr != from->map.end(); itr++) {
        assert(itr->second);
        assert(to);
        copy_watchers(itr->second, to, 1);
    }
}

static void collect_session_watchers(zhandle_t *zh,
                                     watcher_object_list_t **list)
{
    copy_table(&zh->active_node_watchers, *list);
    copy_table(&zh->active_exist_watchers, *list);
    copy_table(&zh->active_child_watchers, *list);
}

static void add_for_event(zk_hashtable *ht, const std::string& path,
                          watcher_object_list_t **list)
{

    watcher_object_list_t* wl;

    boost::unordered_map<std::string, watcher_object_list_t*>::iterator itr;
    itr = ht->map.find(path);
    if (itr == ht->map.end()) {
        return;
    }
    wl = itr->second;
    ht->map.erase(itr);
    copy_watchers(wl, *list, 0);
    free(wl);
}

static void
do_foreach_watcher(watcher_object_t* wo, zhandle_t* zh,
                   const std::string& path, int type, int state) {
  // session event's don't have paths
  std::string client_path =
    type == ZOO_SESSION_EVENT ? path : stripChroot(path, zh->chroot);
  while(wo != NULL) {
    wo->watcher_(zh, type, state, client_path.c_str(), wo->context_);
    wo=wo->next_;
  }
}

watcher_object_list_t *collectWatchers(zhandle_t *zh,int type,
                                       const std::string& path) {
    struct watcher_object_list *list = create_watcher_object_list(0); 

    if(type==ZOO_SESSION_EVENT){
        watcher_object_t* defWatcher = new watcher_object_t(zh->watcher, zh->context);
        add_to_list(&list, defWatcher);
        collect_session_watchers(zh, &list);
        return list;
    }
    switch(type){
    case CREATED_EVENT_DEF:
    case CHANGED_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(&zh->active_node_watchers,path.c_str(),&list);
        add_for_event(&zh->active_exist_watchers,path.c_str(),&list);
        break;
    case CHILD_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(&zh->active_child_watchers,path.c_str(),&list);
        break;
    case DELETED_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(&zh->active_node_watchers,path.c_str(),&list);
        add_for_event(&zh->active_exist_watchers,path.c_str(),&list);
        add_for_event(&zh->active_child_watchers,path.c_str(),&list);
        break;
    }
    return list;
}

void deliverWatchers(zhandle_t *zh, int type,int state, const char* path,
                     watcher_object_list_t **list)
{
    if (!list || !(*list)) return;
    do_foreach_watcher((*list)->head, zh, path, type, state);
    destroy_watcher_object_list(*list);
    *list = 0;
}

void activateWatcher(zhandle_t *zh, watcher_registration_t* reg, int rc)
{
    if(reg){
        /* in multithreaded lib, this code is executed 
         * by the IO thread */
        zk_hashtable *ht = reg->checker(zh, rc);
        if(ht){
            insert_watcher_object(ht,reg->path,
                    create_watcher_object(reg->watcher, reg->context));
        }
    }    
}
