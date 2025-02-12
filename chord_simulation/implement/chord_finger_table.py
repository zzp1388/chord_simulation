from ..chord.chord_base import BaseChordNode, quick_connect
from ..chord.chord_base import connect_node, hash_func, is_between
from ..chord.struct_class import KeyValueResult, Node, KVStatus,M
from loguru import logger
import time

class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()

        # 初始化节点的属性
        self.node_id = hash_func(f'{address}:{port}')  # 为节点生成唯一的ID
        self.kv_store = dict()  # 键值存储
        self.predecessor_kv_store = dict()  # 存储前驱节点的键值对
        self.successor_kv_store = dict()  # 存储后继节点的键值对
        self.next_finger = 0  # 用于修复finger_table

        # 创建节点对象
        self.self_node = Node(self.node_id, address, port)  # 当前节点
        self.successor = self.self_node  # 后继节点
        self.predecessor = Node(self.node_id, address, port, valid=False)  # 前驱节点
        self.stability_test_paused = False  # 是否开启稳定性测试

        self.finger_table = [[(self.node_id + 2 ** i) % (2 ** M), self.successor] for i in range(M)]

        self.logger.info(f'node {self.node_id} listening at {address}:{port}')  # 记录节点信息

    def _log_self(self):
        msg = 'now content: '
        msg += '\nlocal:'
        for k, v in self.kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        msg += '\npredecessor:'
        for k, v in self.predecessor_kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        msg += '\nsuccessor:'
        for k, v in self.successor_kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)

        pre_node_id = self.predecessor.node_id if self.predecessor.valid else "null"
        self.logger.debug(f"{pre_node_id} - {self.node_id} - {self.successor.node_id}")

    def lookup(self, key: str, depth: int = 1) -> tuple:  # Adjust return type to tuple
        # Increment the depth on each recursive call
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)

        # Check if the key is within the current node’s range
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self._lookup_local(key,depth)  # Return result and current depth
        else:
            next_node = self._closet_preceding_node(h)
            conn_next_node = connect_node(next_node)
            # 递归调用并传递增加的深度
            return conn_next_node.lookup(key,depth+1)

    def _lookup_local(self, key: str, depth: int) -> KeyValueResult:
        # 在当前节点中查找键对应的值
        result = self.kv_store.get(key, None)
        status = KVStatus.VALID if result is not None else KVStatus.NOT_FOUND
        return KeyValueResult(key, result, self.node_id, depth,status)

    def find_successor(self, key_id: int) -> Node:
        # 查找指定键的后继节点
        key_id_node = Node(key_id, "", 0)
        if is_between(key_id_node, self.self_node, self.successor):
            return self.successor
        else:
            next_node = self._closet_preceding_node(key_id)
            conn_next_node = connect_node(next_node)
            if conn_next_node:
                return conn_next_node.find_successor(key_id)
            else:
                return self.self_node

    def _closet_preceding_node(self, key_id: int) -> Node:
        tmp_key_node = Node(key_id, "", 0)
        # 检查 tmp_key_node 是否在自身节点（self_node）和后继节点（successor）之间
        if is_between(tmp_key_node, self.self_node, self.successor):
            return self.successor
        last_connected_node = None  # 记录最后一个成功连接的节点
        for i in range(M - 1, -1, -1):
            finger_node = self.finger_table[i][1]
            # 确保节点存在，并且在范围内
            if finger_node is not None and is_between(finger_node, self.self_node, tmp_key_node):
                if finger_node != last_connected_node:  # 跳过相邻的冗余节点
                    node = quick_connect(finger_node)
                    if node:  # 如果成功连接
                        return finger_node  # 返回成功连接的节点
                    last_connected_node = finger_node  # 更新最后一个连接的节点
        # 如果没有找到可连接的节点，返回后继节点
        return self.successor

    def put(self, key: str, value: str) -> KeyValueResult:
        h = hash_func(key)  # 计算哈希值
        tmp_key_node = Node(h, "", 0)
        # 判断 key 是否在当前节点（self_node）和前驱节点之间
        if is_between(tmp_key_node, self.predecessor, self.self_node) :
            # 在当前节点执行插入
            result = self.do_put(key, value, "self")

            # # 尝试将副本插入前驱节点
            # if self.predecessor and self.predecessor.valid:
            #     try:
            #         predecessor_client = connect_node(self.predecessor)
            #         predecessor_client.do_put(key, value, "successor")  # 直接调用 do_put
            #         logger.info(f"Stored ({key}, {value}) in predecessor {self.predecessor.node_id}.")
            #     except Exception as e:
            #         logger.info(f"Failed to store in predecessor {self.predecessor.node_id}: {e}")

            # # 尝试将副本插入后继节点
            # if self.successor and self.successor.valid:
            #     try:
            #         successor_client = connect_node(self.successor)
            #         successor_client.do_put(key, value, "predecessor")  # 直接调用 do_put
            #         logger.info(f"Stored ({key}, {value}) in successor {self.successor.node_id}.")
            #     except Exception as e:
            #         logger.info(f"Failed to store in successor {self.successor.node_id}: {e}")

            return result

        # 如果不在该范围内，寻找合适的下一个节点
        next_node = self._closet_preceding_node(h)
        conn_next_node = connect_node(next_node)

        # 将请求传递给下一个节点
        return conn_next_node.put(key, value)

    def do_put(self, key: str, value: str, place: str) -> KeyValueResult:
        # 存储当前节点的数据
        if place == "self":
            logger.info(f"存储 {key}:{value} 成功")
            self.kv_store[key] = value
        elif place == "predecessor":
            self.predecessor_kv_store[key] = value
        else:
            self.successor_kv_store[key] = value

        return KeyValueResult(key, value, self.node_id,0)

    def join(self, node: Node):
        # 加入指定节点的Chord网络
        conn_node = connect_node(node)
        self.successor = conn_node.find_successor(self.node_id)

    def notify(self, node: Node):
        # 通知当前节点的前驱节点
        if not self.predecessor.valid or is_between(node, self.predecessor, self.self_node):
            self.predecessor = node
            logger.info(f"Updating predecessor from {self.predecessor.node_id} to {node.node_id}.")

    def _stabilize(self):
        if not self.stability_test_paused:
            if self.successor:
                node = connect_node(self.successor)
                if node:
                    try:
                        x = node.get_predecessor()
                        # 确保 x 是有效节点
                        if x and is_between(x, self.self_node, self.successor):
                            logger.info(f"Updating successor from {self.successor.node_id} to {x.node_id}.")
                            self.successor = x
                        # 通知后继节点当前节点
                        node.notify(self.self_node)

                    except Exception as e:
                        logger.info(f"An error occurred during stabilization: {e}")
                else:
                    logger.info(f"Successor {self.successor.node_id} is not reachable.")
                    self.fix_chord()
                    return
            else:
                logger.info(f"{self.node_id} has no successor defined.")
                return

    def pause_stability_tests(self):
        self.stability_test_paused = True

    def resume_stability_tests(self):
        self.stability_test_paused = False

    def _fix_fingers(self):
        if (not self.stability_test_paused) and connect_node(self.successor):
            if self.next_finger == 0:
                self.finger_table[self.next_finger][1] = self.successor
            elif self.next_finger == 1:
                conn_next_node = connect_node(self.successor)
                if conn_next_node:
                    self.finger_table[self.next_finger][1] = conn_next_node.get_successor()
                else:
                    self.finger_table[self.next_finger][1] = self.successor
            elif self.next_finger == 2:
                conn_next_node = connect_node(self.successor)
                conn_next_node = connect_node(conn_next_node.get_successor())
                if conn_next_node:
                    self.finger_table[self.next_finger][1] = conn_next_node.get_successor()
                else:
                    self.finger_table[self.next_finger][1] = self.successor
            else:
                start_id = (self.node_id + 2 ** self.next_finger) % (2 ** M)
                self.finger_table[self.next_finger][1] = self._closet_preceding_node(start_id)
            self.next_finger = (self.next_finger + 1) % M  # 更新下一个需要更新的finger位置的索引

    def _check_predecessor(self):
        pass

    def update_data(self):
        """周期性更新数据"""
        # 获取前驱节点和后继节点的数据
        if connect_node(self.successor) and connect_node(self.predecessor) and self.predecessor.node_id != self.node_id:
            try:
                predecessor_client = connect_node(self.predecessor)
                kv_pairs1 = predecessor_client.get_all_data("successor")
                successor_client = connect_node(self.successor)
                kv_pairs2 = successor_client.get_all_data("predecessor")
                # 原数据与副本取并集
                self.kv_store.update(kv_pairs1)
                self.kv_store.update(kv_pairs2)
                self.check_and_clean_data()  # 检查本地的键值对是否属于自己
                # 更新后继与前驱中的副本
                successor_client.update_predecessor_kv_store()
                predecessor_client.update_successor_kv_store()
            except:
                logger.warning("网络质量不佳")

    def check_and_clean_data(self):
        """对当前节点的所有数据进行检查，删除不符合条件的数据"""
        keys_to_delete = []

        for key in list(self.kv_store.keys()):  # 使用 list() 防止在遍历时修改字典
            if not self.is_key_for_node(key):  # 根据需要检查数据
                keys_to_delete.append(key)

        # 删除不符合条件的数据
        for key in keys_to_delete:
            del self.kv_store[key]

    def get_all_data(self, place: str):
        if place == "self":
            return self.kv_store
        elif place == "predecessor":
            return self.predecessor_kv_store
        else:
            return self.successor_kv_store

    def is_key_for_node(self, key: str):
        """判断一个键是否应当属于某个节点，由节点ID决定键是否属于该节点"""
        h = hash_func(key)  # 计算哈希值
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return True
        return False

    def update_successor_kv_store(self):
        successor_client = connect_node(self.successor)
        kv_pairs = successor_client.get_all_data("self")
        self.successor_kv_store.clear()
        # 更新successor_kv_store
        for key, value in kv_pairs.items():
            self.successor_kv_store[key] = value

    def update_predecessor_kv_store(self):
        predecessor_client = connect_node(self.predecessor)
        kv_pairs = predecessor_client.get_all_data("self")
        self.predecessor_kv_store.clear()
        # 更新predecessor_kv_store
        for key, value in kv_pairs.items():
            self.predecessor_kv_store[key] = value

    def leave_network(self):
        successor_client = connect_node(self.successor)
        successor_client.pause_stability_tests()
        predecessor_client = connect_node(self.predecessor)
        predecessor_client.pause_stability_tests()
        self.pause_stability_tests()
        successor_client.update_predecessor(self.predecessor)
        predecessor_client.update_successor(self.successor)
        successor_client.resume_stability_tests()
        predecessor_client.resume_stability_tests()
        self.predecessor = None
        self.successor = None
        self.kv_store.clear()

    def update_predecessor(self, predecessor):
        self.predecessor = predecessor  # 更新前驱

    def update_successor(self, successor):
        self.successor = successor  # 更新后继

    def fix_chord(self):
        self.pause_stability_tests()
        new_successor = self.find_alive_successor()
        successor_client = connect_node(new_successor)
        successor_client.pause_stability_tests()
        # 在环重建之前，先将successor_client原来前驱的数据保存在本地以防丢失
        kv_pairs = successor_client.get_all_data("predecessor")
        for key, value in kv_pairs.items():
            successor_client.do_put(key, value,"self")
        self.successor = new_successor
        successor_client.update_predecessor(self.self_node)
        self.resume_stability_tests()
        successor_client.resume_stability_tests()

    # def find_alive_successor(self):
    #     for finger in self.finger_table:
    #         finger_node = finger[1]  # 假设 finger 表的第一元素是指向节点对象
    #         node = connect_node(finger_node)
    #         if node:
    #             return finger_node  # 返回第一个存活的后继节点
    #
    #     return self.self_node  # 如果没有找到存活的后继节点，返回自身

    def find_alive_successor(self):
        for finger in self.finger_table:
            finger_node = finger[1]  # 假设 finger 表的第一元素是指向节点对象

            for attempt in range(3):  # 每个 finger 有 3 次连接重试机会
                node = connect_node(finger_node)
                if node:
                    return finger_node  # 返回第一个存活的后继节点
                time.sleep(0.5)

        return self.self_node  # 如果没有找到存活的后继节点，返回自身
