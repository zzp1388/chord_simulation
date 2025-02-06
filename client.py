from chord_simulation.chord.struct_class import KeyValueResult, KVStatus
from chord_simulation.chord.chord_base import connect_address
import time
from loguru import logger

class Client:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.node = connect_address(address, port)

    def put(self, key: str, value: str, max_retries=3, delay=0.5):
        """
        返回 put_status: bool 和 put_node_position: int
        """
        for attempt in range(max_retries):
            try:
                node = connect_address(self.address, self.port)
                put_res: KeyValueResult = node.put(key, value)
                # 记录详细的存储结果
                # logger.info(f"尝试存储结果: key = {key},状态 = {put_res.status}, 节点 ID = {put_res.node_id}")

                put_status = True if put_res.status == KVStatus.VALID else False
                return put_status, put_res.node_id

            except Exception as e:
                logger.warning(f"存储{key}:{value}第 {attempt + 1} 次尝试失败，错误信息: {e}")
                time.sleep(delay)  # 等待一段时间后重试

        logger.error(f"在 {max_retries} 次尝试后未能存储 {key}:{value}")
        return None, None

    def get(self, key: str, max_retries=3, delay=0.5):
        """
         return get_status: str, get_result: k-v, get_node_position: int, depth: int
        """
        for attempt in range(max_retries):
            try:
                node = connect_address(self.address, self.port)
                get_res:KeyValueResult = node.lookup(key,1)
                status = get_res.status
                if status == KVStatus.VALID:
                    status = 'valid'
                elif status == KVStatus.NOT_FOUND:
                    logger.info(f"查找{key}失败")
                    status = 'not_found'
                else:
                    status = 'else status'
                return status, get_res.key, get_res.value, get_res.node_id, get_res.depth
            except Exception as e:
                logger.warning(f"查找{key} 第 {attempt + 1} 次尝试失败，错误信息: {e}")
                time.sleep(delay)  # 等待一段时间后重试

        logger.error(f"在 {max_retries} 次尝试后未能查找 {key}")
        return None,None,None,None,None



