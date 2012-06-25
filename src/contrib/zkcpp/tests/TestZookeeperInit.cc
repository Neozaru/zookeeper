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
#include <sys/types.h>
#include <netinet/in.h>
#include <errno.h>

#include "Util.h"
#include "zk_adaptor.h"

using namespace std;

class Zookeeper_init : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(Zookeeper_init);
    CPPUNIT_TEST(testBasic);
    CPPUNIT_TEST(testAddressResolution);
    CPPUNIT_TEST(testMultipleAddressResolution);
    CPPUNIT_TEST(testNullAddressString);
    CPPUNIT_TEST(testEmptyAddressString);
    CPPUNIT_TEST(testOneSpaceAddressString);
    CPPUNIT_TEST(testTwoSpacesAddressString);
    CPPUNIT_TEST(testInvalidAddressString1);
    CPPUNIT_TEST(testInvalidAddressString2);
    CPPUNIT_TEST(testNonexistentHost);
    CPPUNIT_TEST_SUITE_END();
    zhandle_t *zh;
public:
    Zookeeper_init():zh(0) {
    }

    ~Zookeeper_init() {
    }

    void setUp()
    {
        zoo_deterministic_conn_order(0);
        zh=0;
    }

    void tearDown()
    {
        zookeeper_close(zh);
    }

    void testBasic()
    {
        const string EXPECTED_HOST("127.0.0.1:21212");
        const int EXPECTED_ADDRS_COUNT =1;
        const int EXPECTED_RECV_TIMEOUT=10000;
        clientid_t cid;
        memset(&cid,0xFE,sizeof(cid));

        zh=zookeeper_init(EXPECTED_HOST.c_str(),boost::shared_ptr<Watch>(),
                          EXPECTED_RECV_TIMEOUT, &cid,(void*)1,0);

        CPPUNIT_ASSERT(zh!=0);
        // TODO fix timing.
        //CPPUNIT_ASSERT_EQUAL(-1, zh->fd);
        CPPUNIT_ASSERT(zh->hostname!=0);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_ADDRS_COUNT,zh->addrs_count);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_HOST,string(zh->hostname));
        CPPUNIT_ASSERT(zh->context == (void*)1);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_RECV_TIMEOUT,zh->recv_timeout);
        CPPUNIT_ASSERT(zh->connect_index==0);
        CPPUNIT_ASSERT(zh->last_zxid ==0);
        CPPUNIT_ASSERT(memcmp(&zh->client_id,&cid,sizeof(cid))==0);
    }
    void testAddressResolution()
    {
        const char EXPECTED_IPS[][4]={{127,0,0,1}};
        const int EXPECTED_ADDRS_COUNT =COUNTOF(EXPECTED_IPS);

        zoo_deterministic_conn_order(1);
        zh=zookeeper_init("127.0.0.1:2121",0,10000,0,0,0);

        CPPUNIT_ASSERT(zh!=0);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_ADDRS_COUNT,zh->addrs_count);
        for(int i=0;i<zh->addrs_count;i++){
            sockaddr_in* addr=(struct sockaddr_in*)&zh->addrs[i];
            CPPUNIT_ASSERT(memcmp(EXPECTED_IPS[i],&addr->sin_addr,sizeof(addr->sin_addr))==0);
            CPPUNIT_ASSERT_EQUAL(2121,(int)ntohs(addr->sin_port));
        }
    }
    void testMultipleAddressResolution()
    {
        const string EXPECTED_HOST("127.0.0.1:2121,127.0.0.2:3434");
        const char EXPECTED_IPS[][4]={{127,0,0,1},{127,0,0,2}};
        const int EXPECTED_ADDRS_COUNT =COUNTOF(EXPECTED_IPS);

        zoo_deterministic_conn_order(1);
        zh=zookeeper_init(EXPECTED_HOST.c_str(),0,1000,0,0,0);

        CPPUNIT_ASSERT(zh!=0);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_ADDRS_COUNT,zh->addrs_count);

        for(int i=0;i<zh->addrs_count;i++){
            sockaddr_in* addr=(struct sockaddr_in*)&zh->addrs[i];
            CPPUNIT_ASSERT(memcmp(EXPECTED_IPS[i],&addr->sin_addr,sizeof(addr->sin_addr))==0);
            if(i<1)
                CPPUNIT_ASSERT_EQUAL(2121,(int)ntohs(addr->sin_port));
            else
                CPPUNIT_ASSERT_EQUAL(3434,(int)ntohs(addr->sin_port));
        }
    }
    void testMultipleAddressWithSpace()
    { 
        const string EXPECTED_HOST("127.0.0.1:2121,  127.0.0.2:3434");
        const char EXPECTED_IPS[][4]={{127,0,0,1},{127,0,0,2}};
        const int EXPECTED_ADDRS_COUNT =COUNTOF(EXPECTED_IPS);

        zoo_deterministic_conn_order(1);
        zh=zookeeper_init(EXPECTED_HOST.c_str(),0,1000,0,0,0);

        CPPUNIT_ASSERT(zh!=0);
        CPPUNIT_ASSERT_EQUAL(EXPECTED_ADDRS_COUNT,zh->addrs_count);

        for(int i=0;i<zh->addrs_count;i++){
            sockaddr_in* addr=(struct sockaddr_in*)&zh->addrs[i];
            CPPUNIT_ASSERT(memcmp(EXPECTED_IPS[i],&addr->sin_addr,sizeof(addr->sin_addr))==0);
            if(i<1)
                CPPUNIT_ASSERT_EQUAL(2121,(int)ntohs(addr->sin_port));
            else
                CPPUNIT_ASSERT_EQUAL(3434,(int)ntohs(addr->sin_port));
        }
    }
    void testNullAddressString()
    {
        zh=zookeeper_init(NULL,0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
    }
    void testEmptyAddressString()
    {
        const string INVALID_HOST("");
        zh=zookeeper_init(INVALID_HOST.c_str(),0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
    }
    void testOneSpaceAddressString()
    {
        const string INVALID_HOST(" ");
        zh=zookeeper_init(INVALID_HOST.c_str(),0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
    }
    void testTwoSpacesAddressString()
    {
        const string INVALID_HOST("  ");
        zh=zookeeper_init(INVALID_HOST.c_str(),0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
    }
    void testInvalidAddressString1()
    {
        const string INVALID_HOST("host1");
        zh=zookeeper_init(INVALID_HOST.c_str(),0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
    }
    void testInvalidAddressString2()
    {
        const string INVALID_HOST("host1:1111+host:123");
        zh=zookeeper_init(INVALID_HOST.c_str(),0,0,0,0,0);
        CPPUNIT_ASSERT(zh==0);
        CPPUNIT_ASSERT((ENOENT|EINVAL) & errno);
    }
    void testNonexistentHost()
    {
        const string EXPECTED_HOST("host1.blabadibla.bla.:1111");

        zh=zookeeper_init(EXPECTED_HOST.c_str(),0,0,0,0,0);

        CPPUNIT_ASSERT(zh==0);
        //With the switch to thread safe getaddrinfo, we don't get
        //these global variables
        //CPPUNIT_ASSERT_EQUAL(EINVAL,errno);
        //CPPUNIT_ASSERT_EQUAL(HOST_NOT_FOUND,h_errno);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(Zookeeper_init);
