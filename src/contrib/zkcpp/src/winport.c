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

#ifdef WIN32
#include "winport.h"
#include <winsock2.h>
#include <ws2tcpip.h>

int close(SOCKET fd) {
        return closesocket(fd);
}

int Win32WSAStartup()
{
       WORD    wVersionRq;
       WSADATA wsaData;
       int             err;

       wVersionRq = MAKEWORD(2,0);
       err = WSAStartup(wVersionRq, &wsaData);
       if (err != 0)
               return 1;
       
       // confirm the version information
       if ((LOBYTE(wsaData.wVersion) != 2) ||
           (HIBYTE(wsaData.wVersion) != 0))
       {
               Win32WSACleanup();              
               return 1;
       }
       return 0;
}

void Win32WSACleanup()
{
       WSACleanup();
}

#endif //WIN32



