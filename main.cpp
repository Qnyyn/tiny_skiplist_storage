#include"./skiplist.h"
#include<unistd.h>
int main(){
    SkipList<int, std::string> skiplist(18);
    skiplist.start_background_delete_thread();
    skiplist.start_background_write_thread();
    // skiplist.insert_element(1,"你好");
    // skiplist.insert_element(2,"世界");
    // skiplist.insert_element(8,"哈哈");
    // skiplist.insert_element(12,"欧克");
    // skiplist.insert_element(4,"古德");
    // skiplist.insert_element(6,"委屈");

    // skiplist.load_file();
    // skiplist.display_list();
    // skiplist.search_element(8);
    // std::cout<<"元素个数: "<<skiplist.skiplist_size()<<std::endl;
    // skiplist.insert_element(6,"委屈");

    skiplist.insert_element(8,"hello");
    skiplist.insert_element(4,"hello1");
    skiplist.insert_element(5,"hello2");
    skiplist.insert_element(6,"hello3");
    //! 现有问题，删除内存单用没问题，但需要处理和缓冲区同步，数据一致性的问题
    //! 包括删除节点，那么数据一致性有问题
    //! 双写机制暂时解决
    //skiplist.insert_element(7,"hello4",std::chrono::seconds(5));
    skiplist.display_list();
    sleep(12);
    skiplist.delete_element(4);
    // skiplist.delete_element(8);
    // skiplist.display_list();
    // std::cout<<"元素个数: "<<skiplist.skiplist_size()<<std::endl;

    //skiplist.dump_file();
    //periodic_write_thread.join(); 
    while(1) 
        sleep(1);
    return 0;
}