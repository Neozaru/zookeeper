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

/**
 * This header file is to port sockets and other utility methods on windows.
 * Specifically the threads function, mutexes, keys, and socket initialization.
 */

#ifndef WINPORT_H_
#define WINPORT_H_

#ifdef WIN32
#include <winconfig.h>
#include <errno.h>
#include <process.h>
#include <stdlib.h>
#include <malloc.h>

typedef int ssize_t;

inline int gettimeofday(struct timeval *tp, void *tzp) {
        int64_t now = 0;
        if (tzp != 0) { errno = EINVAL; return -1; }
        GetSystemTimeAsFileTime( (LPFILETIME)&now );
        tp->tv_sec = (long)(now / 10000000 - 11644473600LL);
        tp->tv_usec = (now / 10) % 1000000;
        return 0;
}
int close(SOCKET fd);
int Win32WSAStartup();
void Win32WSACleanup();
#endif //WIN32

#endif //WINPORT_H_
