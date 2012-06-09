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

#ifndef THREADINGUTIL_H_
#define THREADINGUTIL_H_

#include <vector>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

// *****************************************************************************
// Threading primitives

// atomic post-increment; returns the previous value of the operand
int32_t atomic_post_incr(volatile int32_t* operand, int32_t incr);
// atomic fetch&store; returns the previous value of the operand
int32_t atomic_fetch_store(volatile int32_t *operand, int32_t value);

// a partial implementation of an atomic integer type
class AtomicInt{
public:
    explicit AtomicInt(int32_t init=0):v_(init){}
    AtomicInt(const AtomicInt& other):v_(other){}
    // assigment
    AtomicInt& operator=(const AtomicInt& lhs){
        atomic_fetch_store(&v_,lhs);
        return *this;
    }
    AtomicInt& operator=(int32_t i){
        atomic_fetch_store(&v_,i);
        return *this;
    }
    // pre-increment
    AtomicInt& operator++() {
        atomic_post_incr(&v_,1);
        return *this;
    }
    // pre-decrement
    AtomicInt& operator--() {
        atomic_post_incr(&v_,-1);
        return *this;
    }
    // post-increment
    AtomicInt operator++(int){
        return AtomicInt(atomic_post_incr(&v_,1));
    }
    // post-decrement
    AtomicInt operator--(int){
        return AtomicInt(atomic_post_incr(&v_,-1));
    }
    
    operator int() const{
        return atomic_post_incr(&v_,0);
    }
    int get() const{
        return atomic_post_incr(&v_,0);
    }
private:
    mutable int32_t v_;
};

// ****************************************************************************
#define VALIDATE_JOBS(jm) jm.validateJobs(__FILE__,__LINE__)
#define VALIDATE_JOB(j) j.validate(__FILE__,__LINE__)

class MTLock{
public:
    MTLock(boost::mutex& m):m_(m){m.lock();}
    ~MTLock(){m_.unlock();}
    boost::mutex& m_;
};

#define synchronized(m) MTLock __lock(m)

// ****************************************************************************
class Latch {
public:
    virtual ~Latch() {}
    virtual void await() const =0;
    virtual void signalAndWait() =0;
    virtual void signal() =0;
};

class CountDownLatch: public Latch {
public:
    CountDownLatch(int count):count_(count) {
    }
    virtual ~CountDownLatch() {
        boost::unique_lock<boost::mutex> lock(mut_);
        if(count_!=0) {
            count_=0;
            cond_.notify_all();
        }
    }

    virtual void await() const {
        boost::unique_lock<boost::mutex> lock(mut_);
        while(count_!=0)
          cond_.wait(lock);
    }
    virtual void signalAndWait() {
        boost::unique_lock<boost::mutex> lock(mut_);
        signalImpl();
        while(count_!=0)
          cond_.wait(lock);
    }
    virtual void signal() {
        boost::unique_lock<boost::mutex> lock(mut_);
        signalImpl();
    }
private:
    void signalImpl() {
        if(count_>0) {
            count_--;
            cond_.notify_all();
        }
    }
    int count_;
    mutable boost::mutex mut_;
    mutable boost::condition_variable cond_;
};

class TestJob {
public:
    typedef long JobId;
    TestJob():hasRun_(false),startLatch_(0),endLatch_(0) {}
    virtual ~TestJob() {
        join();
    }
    virtual TestJob* clone() const =0;

    virtual void run() =0;
    virtual void validate(const char* file, int line) const =0;

    virtual void start(Latch* startLatch=0,Latch* endLatch=0) {
        startLatch_=startLatch;endLatch_=endLatch;
        hasRun_=true;
        thread_ = boost::thread(thread, this);
    }
    virtual void join() {
        if(!hasRun_)
        return;
        if(boost::this_thread::get_id() == thread_.get_id()) {
            thread_.join();
        } else {
            thread_.detach();
        }
    }
private:
    void awaitStart() {
        if(startLatch_==0) return;
        startLatch_->signalAndWait();
    }
    void signalFinished() {
        if(endLatch_==0) return;
        endLatch_->signal();
    }
    static void* thread(void* p) {
        TestJob* j=(TestJob*)p;
        j->awaitStart(); // wait for the start command
        j->run();
        j->signalFinished();
        return 0;
    }
    bool hasRun_;
    Latch* startLatch_;
    Latch* endLatch_;
    boost::thread thread_;
};

class TestJobManager {
    typedef std::vector<TestJob*> JobList;
public:
    TestJobManager(const TestJob& tj,int threadCount=1):
        startLatch_(threadCount),endLatch_(threadCount)
    {
        for(int i=0;i<threadCount;++i)
            jobs_.push_back(tj.clone());
    }
    virtual ~TestJobManager(){
        for(unsigned  i=0;i<jobs_.size();++i)
            delete jobs_[i];
    }
    
    virtual void startAllJobs() {
        for(unsigned i=0;i<jobs_.size();++i)
            jobs_[i]->start(&startLatch_,&endLatch_);
    }
    virtual void startJobsImmediately() {
        for(unsigned i=0;i<jobs_.size();++i)
            jobs_[i]->start(0,&endLatch_);
    }
    virtual void wait() const {
        endLatch_.await();
    }
    virtual void validateJobs(const char* file, int line) const{
        for(unsigned i=0;i<jobs_.size();++i)
            jobs_[i]->validate(file,line);        
    }
private:
    JobList jobs_;
    CountDownLatch startLatch_;
    CountDownLatch endLatch_;
};

#endif /*THREADINGUTIL_H_*/
