from chord_simulation.chord.struct_class import KeyValueResult, KVStatus
from chord_simulation.chord.chord_base import connect_address
import time


class Client:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.node = connect_address(address, port)

    def put(self, key: str, value: str, max_retries=3, delay=1):
        """
        返回 put_status: bool 和 put_node_position: int
        """
        for attempt in range(max_retries):
            try:
                node = connect_address(self.address, self.port)
                put_res: KeyValueResult = node.put(key, value)
                put_status = True if put_res.status == KVStatus.VALID else False
                print(f"存储{key}:{value}成功")
                return put_status, put_res.node_id

            except Exception as e:
                print(f"存储{key}:{value}第 {attempt + 1} 次尝试失败，错误信息: {e}")
                time.sleep(delay)  # 等待一段时间后重试

        print(f"在 {max_retries} 次尝试后未能存储 {key}:{value}")
        return None, None


    def get(self, key: str, max_retries=3, delay=1):
        """
         return get_status: str, get_result: k-v, get_node_position: int
        """
        for attempt in range(max_retries):
            try:
                node = connect_address(self.address, self.port)
                get_res: KeyValueResult = node.lookup(key)
                status = get_res.status
                if status == KVStatus.VALID:
                    status = 'valid'
                elif status == KVStatus.NOT_FOUND:
                    print(f"查找{key}失败")
                    status = 'not_found'
                else:
                    status = 'else status'
                return status, get_res.key, get_res.value, get_res.node_id
            except Exception as e:
                print(f"查找{key} 第 {attempt + 1} 次尝试失败，错误信息: {e}")
                time.sleep(delay)  # 等待一段时间后重试

        print(f"在 {max_retries} 次尝试后未能查找 {key}")
        return None, None,None, None


