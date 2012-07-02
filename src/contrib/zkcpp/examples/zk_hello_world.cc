#include <zookeeper/zookeeper.hh>
#include <iostream>

using namespace org::apache::zookeeper;

int main() {
  ZooKeeper zk;
  std::vector<data::ACL> acl;

  data::ACL perm;
  data::Stat stat;
  perm.getid().getscheme() = "world";
  perm.getid().getid() = "anyone";
  perm.setperms(Permission::All);
  acl.push_back(perm);

  std::string data, pathCreated;
  zk.init("localhost:2181", 30000, boost::shared_ptr<Watch>());
  zk.create("/hello", "world", acl, CreateMode::Persistent, pathCreated);
  zk.get("/hello", boost::shared_ptr<Watch>(), data, stat);
  std::cout << pathCreated << " " << data << std::endl;
  zk.remove("/hello", -1);
  return 0;
}

