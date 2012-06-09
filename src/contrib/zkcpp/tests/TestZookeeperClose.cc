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

#include <cppunit/extensions/HelperMacros.h>

#include "ZKMocks.h"

using namespace std;

class Zookeeper_close : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(Zookeeper_close);
    CPPUNIT_TEST(testIOThreadStoppedOnExpire);
    CPPUNIT_TEST(testCloseUnconnected);
    CPPUNIT_TEST(testCloseFromWatcher1);
    CPPUNIT_TEST(testCloseUnconnected1);
    CPPUNIT_TEST(testCloseConnected1);
    CPPUNIT_TEST_SUITE_END();
    zhandle_t *zh;
    static void watcher(zhandle_t *, int, int, const char *,void*){}
public:
    void setUp()
    {

        zoo_deterministic_conn_order(0);
        zh=0;
    }

    void tearDown()
    {
        zookeeper_close(zh);
    }

    class CloseOnSessionExpired: public WatcherAction{
    public:
        CloseOnSessionExpired(bool callClose=true):
            callClose_(callClose),rc(ZOK){}
        virtual void onSessionExpired(zhandle_t* zh){
            memcpy(&lzh,zh,sizeof(lzh));
            if(callClose_)
                rc=zookeeper_close(zh);
        }
        zhandle_t lzh;
        bool callClose_;
        int rc;
    };
    
    void testCloseUnconnected()
    {
        // disable threading
        zh=zookeeper_init("localhost:2121",watcher,10000,0,0,0); 
        
        CPPUNIT_ASSERT(zh!=0);
        adaptor_threads* adaptor=(adaptor_threads*)zh->adaptor_priv;
        CPPUNIT_ASSERT(adaptor!=0);

        // do not actually free the memory while in zookeeper_close()
        Mock_free_noop freeMock;
        // make a copy of zhandle before close() overwrites some of 
        // it members with NULLs
        zhandle_t lzh;
        memcpy(&lzh,zh,sizeof(lzh));
        int rc=zookeeper_close(zh);
        zhandle_t* savezh=zh; zh=0;
        // we're done, disable mock's fake free(), use libc's free() instead
        freeMock.disable();
        
        // verify that zookeeper_close has done its job
        CPPUNIT_ASSERT_EQUAL((int)ZOK,rc);
        // memory
        CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(savezh));
        CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh.hostname));
        CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh.addrs));
        CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(adaptor));
        // Cannot be maintained accurately: CPPUNIT_ASSERT_EQUAL(10,freeMock.callCounter);
    }
    void testCloseUnconnected1()
    {
        for(int i=0; i<100;i++){
            zh=zookeeper_init("localhost:2121",watcher,10000,0,0,0); 
            CPPUNIT_ASSERT(zh!=0);
            adaptor_threads* adaptor=(adaptor_threads*)zh->adaptor_priv;
            CPPUNIT_ASSERT(adaptor!=0);
            int rc=zookeeper_close(zh);
            zh=0;
            CPPUNIT_ASSERT_EQUAL((int)ZOK,rc);
        }
    }
    void testCloseConnected1()
    {
        // frozen time -- no timeouts and no pings
        Mock_gettimeofday timeMock;

        for(int i=0;i<100;i++){
            ZookeeperServer zkServer;
            Mock_poll pollMock(&zkServer,ZookeeperServer::FD);
            // do not actually free the memory while in zookeeper_close()
            Mock_free_noop freeMock;
            
            zh=zookeeper_init("localhost:2121",watcher,10000,TEST_CLIENT_ID,0,0); 
            CPPUNIT_ASSERT(zh!=0);
            // make sure the client has connected
            CPPUNIT_ASSERT(ensureCondition(ClientConnected(zh),1000)<1000);
            // make a copy of zhandle before close() overwrites some of 
            // its members with NULLs
            zhandle_t lzh;
            memcpy(&lzh,zh,sizeof(lzh));
            int rc=zookeeper_close(zh);
            zhandle_t* savezh=zh; zh=0;
            // we're done, disable mock's fake free(), use libc's free() instead
            freeMock.disable();
            
            CPPUNIT_ASSERT_EQUAL((int)ZOK,rc);            
            adaptor_threads* adaptor=(adaptor_threads*)lzh.adaptor_priv;
            // memory
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(savezh));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh.hostname));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh.addrs));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(adaptor));
        }
    }
    
    struct PointerFreed{
        PointerFreed(Mock_free_noop& freeMock,void* ptr):
            freeMock_(freeMock),ptr_(ptr){}
        bool operator()() const{return freeMock_.isFreed(ptr_); }
        Mock_free_noop& freeMock_;
        void* ptr_;
    };
    // test if zookeeper_close may be called from a watcher callback on
    // SESSION_EXPIRED event
    void testCloseFromWatcher1()
    {
        // frozen time -- no timeouts and no pings
        Mock_gettimeofday timeMock;
        
        for(int i=0;i<100;i++){
            ZookeeperServer zkServer;
            // make the server return a non-matching session id
            zkServer.returnSessionExpired();
            
            Mock_poll pollMock(&zkServer,ZookeeperServer::FD);
            // do not actually free the memory while in zookeeper_close()
            Mock_free_noop freeMock;

            CloseOnSessionExpired closeAction;
            zh=zookeeper_init("localhost:2121",activeWatcher,10000,
                    TEST_CLIENT_ID,&closeAction,0);
            
            CPPUNIT_ASSERT(zh!=0);
            // we rely on the fact that zh is freed the last right before
            // zookeeper_close() returns...
            CPPUNIT_ASSERT(ensureCondition(PointerFreed(freeMock,zh),1000)<1000);
            zhandle_t* lzh=zh;
            zh=0;
            // we're done, disable mock's fake free(), use libc's free() instead
            freeMock.disable();
            
            CPPUNIT_ASSERT_EQUAL((int)ZOK,closeAction.rc);          
            adaptor_threads* adaptor=(adaptor_threads*)closeAction.lzh.adaptor_priv;
            // memory
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(closeAction.lzh.hostname));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(closeAction.lzh.addrs));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(adaptor));
        }
    }

    void testIOThreadStoppedOnExpire()
    {
        // frozen time -- no timeouts and no pings
        Mock_gettimeofday timeMock;
        
        for(int i=0;i<100;i++){
            ZookeeperServer zkServer;
            // make the server return a non-matching session id
            zkServer.returnSessionExpired();
            
            Mock_poll pollMock(&zkServer,ZookeeperServer::FD);
            // do not call zookeeper_close() from the watcher
            CloseOnSessionExpired closeAction(false);
            zh=zookeeper_init("localhost:2121",activeWatcher,10000,
                    &testClientId,&closeAction,0);
            
            // this is to ensure that if any assert fires, zookeeper_close() 
            // will still be called while all the mocks are in the scope!
            CloseFinally guard(&zh);

            CPPUNIT_ASSERT(zh!=0);
            CPPUNIT_ASSERT(ensureCondition(SessionExpired(zh),1000)<1000);
            // TODO(michim) check if the thread is stopped
            //CPPUNIT_ASSERT(ensureCondition(IOThreadStopped(zh),1000)<1000);
            // make sure the watcher has been processed
            CPPUNIT_ASSERT(ensureCondition(closeAction.isWatcherTriggered(),1000)<1000);
            // make sure the threads have not been destroyed yet
            adaptor_threads* adaptor=(adaptor_threads*)zh->adaptor_priv;
            // about to call zookeeper_close() -- no longer need the guard
            guard.disarm();
            
            // do not actually free the memory while in zookeeper_close()
            Mock_free_noop freeMock;
            zookeeper_close(zh);
            zhandle_t* lzh=zh; zh=0;
            // we're done, disable mock's fake free(), use libc's free() instead
            freeMock.disable();
            
            // memory
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(lzh));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(closeAction.lzh.hostname));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(closeAction.lzh.addrs));
            CPPUNIT_ASSERT_EQUAL(1,freeMock.getFreeCount(adaptor));
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(Zookeeper_close);
