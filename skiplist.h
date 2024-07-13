#include<iostream>
#include<cstdlib>
#include<cmath>
#include<cstring>
#include<mutex>
#include<fstream>
#include<vector>
#include<thread>
#include<chrono>
#include<random>
#include <algorithm>

#define STORE_FILE "store/dumpFile"

//using namespace std;

std::mutex mtx;
std::string delimiter = ":";

template<typename K,typename V>
class Node{
public:
    Node(){}
    //创建具有给定键值对和层级的节点
    Node(K k,V v,int);

    ~Node();

    K get_key() const;

    V get_value() const;

    void set_value(V);

    // 指针数组，横向保存指针数组，通过forward[i]来找不同层的那个数组
    //todo 可以用vecotr<Node<K,V> *> 改写
    Node<K,V>* *forward;

    //表示节点的层级，即该节点在跳表中的层次
    int mode_level;

    //todo 可选的过期时间
    std::chrono::steady_clock::time_point expiration_time;
private:
    K key;
    V value;

};

//类外实现Node的函数
template<typename K,typename V>
Node<K,V>::Node(const K k,const V v,int level){
    this->key = k;
    this->value = v;
    this->mode_level = level;

    //申请指针数组的空间
    //todo 如果改写成vector应该无需分配

    this->forward = new Node<K,V>*[level + 1];//假如层级为4，其实是5层所以level + 1

    //memset(this->forward,0,sizeof(Node<K,V>*) * (level + 1));
    std::fill(forward, forward + level + 1, nullptr);
}

template<typename K,typename V>
Node<K,V>::~Node(){
    delete []this->forward;
}

template<typename K,typename V>
K Node<K,V>::get_key() const{
    return this->key;
}


template<typename K,typename V>
V Node<K,V>::get_value() const{
    return this->value;
}

template<typename K,typename V>
void Node<K,V>::set_value(V value){
    this->value = value;
}

template <typename K,typename V>
class SkipList{
public:
    SkipList(int);
    ~SkipList();
    //清空跳表
    //void clear(Node<K,V> *);
    void clear();
    //创建一个新节点
    Node<K,V> *create_node(K,V,int);
    //生成随机层级
    int get_random_level();
    //插入元素
    int insert_element(K,V,std::chrono::seconds expire_after = std::chrono::seconds::max());
    //跳表显示
    void display_list();
    //元素个数
    int skiplist_size();
    //搜索元素
    bool search_element(const K);
    //删除元素
    void delete_element(const K);
    //写入文件
    void dump_file();
    //加载文件
    void load_file();
    //从一行中提取key和value
    void get_key_value_from_string(const std::string &str,std::string &key,std::string &value);
    //判断一行是否有效
    bool is_valid_string(const std::string &str);

    //todo 添加写入缓冲和定期写入到磁盘的函数
    //写缓冲区
    void buffer_write(const K& key, const V& value);
    //取出缓冲区内容写入磁盘
    void periodic_write_to_disk();
    //后台线程执行函数，定期写入
    void write_thread_function();
    //启动后台写入线程
    void start_background_write_thread();

    //todo 后台定时检查删除线程
    //启动定期删除线程
    void start_background_delete_thread();

    //停止定期删除线程
    void stop_background_delete_thread();
    
    //定期检查删除，线程函数
    void periodic_delete_function();

private:
    //跳表最高层级
    int _max_level;

    //跳表当前层级
    int _skip_list_level;

    //头节点指针
    Node<K,V> *_header;

    //文件操作
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    //跳表当前元素个数
    int _element_count;

    //todo 写入缓冲
    std::vector<std::pair<K, V>> write_buffer;

    //后台写缓冲线程退出标志,为true才执行
    bool m_stop;
    //后台定期删除线程退出标志,为false才执行
    bool m_stop_delete_thread;

    // 后台写缓冲线程
    std::thread _write_thread;

    //后台定期删除线程
    std::thread _delete_thread;

};

template<typename K,typename V>
SkipList<K,V>::SkipList(int max_level):m_stop(false){
    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;
    K k;
    V v;

    //头节点作为跳表的入口，应该能够覆盖所有可能的层级
    this->_header = new Node<K,V>(k,v,_max_level);
}

//! 节点太多回收可能栈溢出，用迭代
// template<typename K,typename V>
// void SkipList<K,V>::clear(Node<K,V> *cur){
//     if(cur->forward[0] != nullptr){
//         //当前节点的后继第一个
//         //不存在就是最后了
//         clear(cur->forward[0]);
//     }
//     delete(cur);
// }

template<typename K,typename V>
void SkipList<K,V>::clear(){
    std::unique_lock<std::mutex> ulk(mtx);

    Node<K,V> *cur = _header->forward[0];
    while(cur != nullptr){
        Node<K,V> *tmp = cur;
        if(cur->forward[0])
            cur = cur->forward[0];
        else
            cur = nullptr;
        delete tmp;
        _header->forward[0] = cur;
    }

    //所有层级回收完毕
    _skip_list_level = 0;
    _element_count = 0;
}


template<typename K,typename V>
SkipList<K,V>::~SkipList(){
    //todo 写入缓冲区的内容到磁盘,确保程序退出时也能正确写入
    periodic_write_to_disk();
    //todo 清空写入缓冲
    write_buffer.clear();

    if(_file_writer.is_open()){
        _file_writer.close();
    }

    if(_file_reader.is_open()){
        _file_reader.close();
    }

    //回收写缓冲线程
    m_stop = false;
    if(_write_thread.joinable())
        _write_thread.join();

    //回收定期删除线程
    stop_background_delete_thread();

    //while更保险，多层检查
    while(_header->forward[0]!=nullptr){
        clear();
    }

    delete(_header);
    _header = nullptr;

}

template<typename K,typename V>
Node<K,V>* SkipList<K,V>::create_node(const K k,const V v,int level){
    Node<K,V> *n = new Node<K,V>(k,v,level);
    return n;
}

// Insert given key and value in skip list 
// return 1 means element exists  
// return 0 means insert successfully
//比较虚拟头节点forward[最高层]，大则继续比较，下一个节点的forward[maxlevel]
//如果小了，层数下降,(记得记录之前的那个1节点,pre)forward[maxlevel-1]
/* 
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
*/

//todo 写缓冲
template<typename K, typename V>
void SkipList<K, V>::buffer_write(const K& key, const V& value) {
    write_buffer.push_back(std::make_pair(key, value)); // 将键值对放入写入缓冲
}

//这里讲一下为什么需要随机数，因为正常来说，从第0层开始，往上会一层比一层少一倍，也就是n/2k
//也就是说，下面的节点到上面简历索引的概率是1/2,随机数保证奇数偶数随机，如果是奇数就加1层，偶数就停止，确保n/2k随机
template<typename K, typename V>
int SkipList<K, V>::get_random_level(){

    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};

//todo c++11完全随机
// template<typename K,typename V>
// int SkipList<K,V>::get_random_level(){
//     //至少层数也要一层
//     int k = 1;
//     //随机数引擎
//     static std::default_random_engine e{
//         static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count())
//     };
//     //随机数分布类
//     static std::uniform_int_distribution<unsigned> u(0, 1);
    
//     while(u(e)){
//         k++;
//     }
//     //确保层数不超过最大
//     k = (k < _max_level)?k:_max_level;
//     return k;
// }


//插入成功返回0，失败返回1
template<typename K,typename V>
int SkipList<K,V>::insert_element(const K key,const V value,std::chrono::seconds expire_after){  
    //1.使用unique_lock来保证在多线程环境下的线程安全，避免多个线程同时修改跳表结构。
    std::unique_lock<std::mutex> ulk(mtx);
    Node<K,V> *current = this->_header;

    //2.创建 update 数组并初始化,update 数组中存放的是需要在后续操作的节点的前驱节点
    Node<K,V>* update[this->_max_level + 1];
    //memset(update,0,sizeof(Node<K,V> *) * (_max_level + 1));
    std::fill(update,update + _max_level + 1,nullptr);

    //3.搜索插入位置,从跳表的最高层开始，逐层向下搜索

    for(int i = _skip_list_level;i >= 0;i--){
        //大于后移
        while(current->forward[i] != nullptr && key > current->forward[i]->get_key()){
            current = current->forward[i];
        }
        //小于记录
        update[i] = current;
    }

    //到达第0层，并将 current 指针指向右侧节点，这是希望插入键的位置
    current = current->forward[0];

    //4.节点存在
    if(current != nullptr&&current->get_key() == key){
        std::cout<<"key :"<<key<<",exists"<<std::endl;
        //uniquelock自动解锁了
        return 1;
    }

    //5.如果当前节点为空，表示已经到达该层的末尾
    //  如果当前节点的键值不等于键值，表示需要在 update[0] 和 current 节点之间插入新的节点
    if(current == nullptr || current->get_key() != key){
        //生成随机层级
        int random_level = get_random_level();

        //随机层级大于当前最大层级，需要更新skip_list_level + 1 到 random_level + 1之间的update
        //例如当前层级为3，最大为6,随机层级为6，那么forward[4]到forward[6]需要更新,所以条件是<random_level + 1
        if(random_level > _skip_list_level){
            for(int i = _skip_list_level + 1;i < random_level + 1;++i){
                //前驱保存为头节点
                update[i] = _header;
            }
            //6.更新跳表层级
            _skip_list_level = random_level;
        }

        //创建一个具有随机层级的节点
        Node<K,V> *inserted_node = create_node(key,value,random_level);
        //todo 过期时间初始化
        if (expire_after != std::chrono::seconds::max()) {
            // 如果不是永不过期，则设置过期时间
            inserted_node->expiration_time = std::chrono::steady_clock::now() + expire_after;
        } else {
            // 如果是永不过期，则可以设置为任意值，例如 std::chrono::steady_clock::time_point::max()
            inserted_node->expiration_time = std::chrono::steady_clock::time_point::max();
        }
        
        //7.插入节点
        for(int i = 0;i <= random_level;++i){
            //连接后面的节点
            inserted_node->forward[i] = update[i]->forward[i];
            //把原来的前面节点连到新结点上
            update[i]->forward[i] = inserted_node;
        }

        std::cout<<"Success insert key:"<<key<<", value:"<<value<<std::endl;
        //8.跳表内元素数量++
        _element_count++;
    }

    //todo ------------------
    buffer_write(key, value);

    return 0;
}


//todo ------------------
template<typename K, typename V>
void SkipList<K, V>::periodic_write_to_disk() {
    std::unique_lock<std::mutex> ulk(mtx);
    if (!write_buffer.empty()) {
        //每次进行写入操作的时候都会重新定位到文件的末尾
        _file_writer.open(STORE_FILE,std::ios_base::app);
        for (const auto& p : this->write_buffer) {
            _file_writer << p.first << delimiter << p.second << "\n"; // 写入磁盘文件
        }
        _file_writer.close();
        write_buffer.clear(); // 清空写入缓冲
    }
}

template<typename K,typename V>
void SkipList<K,V>::display_list(){
    std::cout<<"\n*****Skip List*****"<<"\n";
    for(int i = _skip_list_level;i >= 0; --i){
        Node<K,V> *cur = this->_header->forward[i];
        std::cout << "Level " << i << ": ";
        while(cur != nullptr){
            std::cout<<cur->get_key()<<":"<<cur->get_value()<<";";
            cur = cur->forward[i];
        }
        std::cout<<std::endl;
    }

    // std::cout<<"\n*****Skip List*****"<<"\n";
    // for(int i = 0;i <= _skip_list_level; ++i){
    //     Node<K,V> *cur = this->_header->forward[i];
    //     std::cout << "Level " << i << ": ";
    //     while(cur != nullptr){
    //         std::cout<<cur->get_key()<<":"<<cur->get_value()<<";";
    //         cur = cur->forward[i];
    //     }
    //     std::cout<<std::endl;
    // }
    return;
}

template<typename K,typename V>
int SkipList<K,V>::skiplist_size(){
    return this->_element_count;
}


template<typename K,typename V>
bool SkipList<K,V>::search_element(const K key){
    std::cout<<"search_element----------------"<<std::endl;
    Node<K,V> *cur = _header;

    //从最高层开始搜索
    for(int i = _skip_list_level;i >= 0;--i){
        while(cur->forward[i] && key > cur->forward[i]->get_key()){
            cur = cur->forward[i];
        }
    }
    //到达第0层，移到我们想找的那个元素上
    cur = cur->forward[0];
    //判断那个元素的键值是否等于key
    if(cur and cur->get_key() == key){
        std::cout<<"Found key:"<<key<<", value:"<<cur->get_value()<<std::endl;
        return true;
    }

    std::cout<<"Not Found Key:"<<key<<std::endl;
    return false;
}


template<typename K,typename V>
void SkipList<K,V>::delete_element(const K key){
    //unique_lock锁定
    std::unique_lock<std::mutex> ulk(mtx);
    
    //创建前向指针数组
    Node<K,V> *update[_max_level + 1];
    //memset(update,0,sizeof(Node<K,V>*) * (_max_level + 1));
    std::fill(update, update + _max_level + 1, nullptr);
    
    Node<K,V> *cur = this->_header;

    for(int i = _skip_list_level;i >= 0;--i){
        while(cur->forward[i] &&key > cur->forward[i]->get_key()){
            cur = cur->forward[i];
        }
        
        //如果小于等于的话就下移层数
        //保存前向指针数组
        update[i] = cur;
    }

    //移动到正常该删除的位置
    cur = cur->forward[0];

    //判断是否相等
    if(cur == nullptr||cur->get_key() != key){
        std::cout<<"Not this element,cannot delete it"<<std::endl;
        return;
    }

    //此时该删除了
    for(int i = 0;i <= _skip_list_level;++i){
        if(update[i]->forward[i] != cur){
            //说明索引层数到头了，退出就行
            break;
        }
        update[i]->forward[i] = cur->forward[i];
    }

    //删除完有可能当前顶层没有元素了，需要压缩层级
    while(_skip_list_level > 0 && _header->forward[_skip_list_level] == nullptr){
        //如果头节点的最上层next不存在
       --_skip_list_level;
    }

    //减少当前节点数
    --_element_count;


    std::cout<<"Successfully deleted key "<<key<<std::endl;

    if(cur)
        delete cur;

    //todo 目前方案是 删除完元素需要同步磁盘（待优化）
    //todo 二号方案，每次删除操作都记录到日志文件中，重启时可以根据日志文件恢复删除操作
    dump_file();
    write_buffer.clear();//清空缓冲区，确保协同一致
    return;
}

template<typename K,typename V>
void SkipList<K,V>::dump_file(){
    //todo 建议上锁
    std::cout<<"dump_file-------------"<<std::endl;
    _file_writer.open(STORE_FILE);
    Node<K,V> *cur = this->_header->forward[0];

    while(cur != nullptr){
        _file_writer<<cur->get_key()<<":"<<cur->get_value()<<"\n";
        std::cout<<cur->get_key()<<":"<<cur->get_value()<<";\n";
        cur = cur->forward[0];
    }

    _file_writer.flush();
    _file_writer.close();

    return;
}



template<typename K,typename V>
void SkipList<K,V>::get_key_value_from_string(const std::string &str,std::string &key,std::string &value){
    if(!is_valid_string(str)){
        return;
    }

    key = str.substr(0,str.find(delimiter));
    value = str.substr(str.find(delimiter)+1,str.length());
    
    return;
}

template<typename K,typename V>
bool SkipList<K,V>::is_valid_string(const std::string &str){
    if(str.empty()){
        return false;
    }

    if(str.find(delimiter) == std::string::npos){
        return false;
    }

    return true;
}



template<typename K,typename V>
void SkipList<K,V>::load_file(){
    _file_reader.open(STORE_FILE);
    std::cout<<"load file-----------------"<<std::endl;
    std::string line;
    std::string key;
    std::string value;
    while(getline(_file_reader,line)){
        get_key_value_from_string(line,key,value);

        if(key.empty()||value.empty()) continue;

        insert_element(stoi(key),value);
        std::cout<<"key:"<<key<<"value:"<<value<<std::endl;
    }
    //加载完之后，由于插入操作缓冲区会有值，而我们不想要这个，需要清空
    this->write_buffer.clear();

    _file_reader.close();
    return;
}

template<typename K,typename V>
void SkipList<K,V>::write_thread_function(){
    while (this->m_stop) {
        std::this_thread::sleep_for(std::chrono::seconds(10)); // 每隔10秒执行一次
        periodic_write_to_disk();
    }
}

template<typename K,typename V>
void SkipList<K,V>::start_background_write_thread(){
    this->m_stop = true;
    //绑定线程工作
    _write_thread = std::thread(&SkipList<K,V>::write_thread_function,this);
}



//todo ----------------------定期删除
template<typename K,typename V>
void SkipList<K,V>::start_background_delete_thread(){
    this->m_stop_delete_thread = false;
    //绑定线程函数
    _delete_thread = std::thread(&SkipList::periodic_delete_function, this);
}


template<typename K,typename V>
void SkipList<K,V>::stop_background_delete_thread(){
    this->m_stop_delete_thread = true;
    if (_delete_thread.joinable()) {
        _delete_thread.join();
    }
}

template<typename K, typename V>
void SkipList<K, V>::periodic_delete_function() {
    while (!m_stop_delete_thread) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        Node<K, V>* cur = _header->forward[0];
        while (cur != nullptr) {
            // 如果当前时间大于之前设定的超时时间点就删除
            if (std::chrono::steady_clock::now() >= cur->expiration_time) {
                // 删除节点
                K key_to_delete = cur->get_key();  // 记录当前节点的key
                cur = cur->forward[0];  // 先移动到下一个节点
                delete_element(key_to_delete);  // 再删除记录的节点
            } else {
                cur = cur->forward[0];  // 移动到下一个节点
            }

            // 检查停止标志
            if (m_stop_delete_thread) {
                std::cout<<"终止"<<std::endl;
                break;
            }
        }
    }
}
