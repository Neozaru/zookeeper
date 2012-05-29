# ZooKeeper C++ Client

This is a fork of the Apache ZooKeeper project. The goal of this fork is to add
support for C++ client library. The source code for the C++ client library is
located under [`src/contrib/zkcpp`](https://github.com/m1ch1/zookeeper/tree/trunk/src/contrib/zkcpp).

Currently the library is a thin wrapper around the C client, but the intention
is to port the code to C++ over time. More specifically:

* Make it more portable. Replace pthread with boost::thread and socket with
  boost::ip::tcp.
* Use better logging system (either google-glog or log4cxx).
* Use C++ version of jute generated code.
* Use unordered_map instead of hashtable.
