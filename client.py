from chord_simulation.chord.struct_class import KeyValueResult, KVStatus
from chord_simulation.chord.chord_base import connect_address
import time


class Client:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.node = connect_address(address, port)

    def put(self, key: str, value: str, max_retries=3, delay=2):
        """
        返回 put_status: bool 和 put_node_position: int
        """
        for attempt in range(max_retries):
            try:
                node = connect_address(self.address, self.port)
                put_res: KeyValueResult = node.put(key, value)
                put_status = put_res.status == KVStatus.VALID

                if put_status:
                    print(f"存储 {key}:{value} 成功")
                    return put_status, put_res.node_id
                else:
                    print(f"{key}:{value} 的存储操作在尝试 {attempt + 1} 中失败")

            except Exception as e:
                print(f"第 {attempt + 1} 次尝试失败，错误信息: {e}")
                time.sleep(delay)  # 等待一段时间后重试

        print(f"在 {max_retries} 次尝试后未能存储 {key}:{value}")
        return None

    def get(self, key: str):
        """
         return get_status: str, get_result: k-v, get_node_position: int
        """
        get_res: KeyValueResult = connect_address(self.address, self.port).lookup(key)
        status = get_res.status
        if status == KVStatus.VALID:
            status = 'valid'
        elif status == KVStatus.NOT_FOUND:
            status = 'not_found'
        else:
            status = 'else status'
        return status, get_res.key, get_res.value, get_res.node_id

    # def get(self, key: str, max_retries=3, delay=2):
    #     """
    #     返回 get_status: str, get_result: k-v, get_node_position: int
    #     """
    #
    #     for attempt in range(max_retries):
    #         try:
    #             # 尝试连接到节点并查找键
    #             node = connect_address(self.address, self.port)
    #             get_res: KeyValueResult = node.lookup(key)
    #
    #             # 判断状态并返回结果
    #             status = get_res.status
    #             if status == KVStatus.VALID:
    #                 return 'valid', get_res.key, get_res.value, get_res.node_id
    #             elif status == KVStatus.NOT_FOUND:
    #                 return 'not_found', get_res.key, get_res.value, get_res.node_id
    #             else:
    #                 print(f"键 {key} 返回了其他状态: {status}")
    #                 return 'else status', get_res.key, get_res.value, get_res.node_id
    #
    #         except Exception as e:
    #             print(f"尝试 {attempt + 1} 失败，错误信息: {e}")
    #             time.sleep(delay)  # 等待一段时间后重试
    #
    #     print(f"在 {max_retries} 次尝试后无法获取键: {key}")
    #     return 'failed', get_res.key, get_res.value, get_res.node_id  # 返回失败状态和无效结果

