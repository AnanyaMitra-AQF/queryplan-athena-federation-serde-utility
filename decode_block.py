import base64
import jpype
from jpype.types import JByte
from typing import List
import sys


def block_to_record_strings(block) -> List[str]:
    """
    Converts all rows in a Block to a list of human-readable strings.
    Parameters:
        block: com.amazonaws.athena.connector.lambda.data.Block Java object
    Returns:
        List of string representations of each row in the block.
    """
    result = []
    row_count = block.getRowCount()
    BlockUtils = jpype.JClass('com.amazonaws.athena.connector.lambda.data.BlockUtils')
    for i in range(row_count):
        row_str = BlockUtils.rowToString(block, i)
        result.append(row_str)
    return result


if __name__ == "__main__":
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=["jars/*"])
    base64_block = "3AAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAAEgAAAAAAAAAAAADABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAAHgAAAACAAAAAAAAAAAAAAAGAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAYAAAAAAAAAAYAAAAAAAAAIAAAAAAAAAABAAAAAAAAACgAAAAAAAAADAAAAAAAAAA4AAAAAAAAAA8AAAAAAAAAAAAAAAIAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAADAAAABgAAAAAAAAByZWRyZWQAAAMAAAAAAAAAAAAAAAUAAAAPAAAAAAAAAGFwcGxlc3RyYXdiZXJyeQA="
    # Deserialize the block from base64 to Java Block object
    BlockUtils = jpype.JClass('com.amazonaws.athena.connector.lambda.data.BlockUtils')
    Block = jpype.JClass('com.amazonaws.athena.connector.lambda.data.Block')
    # Decode base64 to bytes
    block_bytes = base64.b64decode(base64_block)
    # Convert Python bytes to Java byte[]
    java_bytes = jpype.JArray(JByte)(block_bytes)
    # Deserialize block
    block_obj = BlockUtils.fromBytes(java_bytes)
    # Convert block to list of human-readable strings
    records = block_to_record_strings(block_obj)
    # Print each record
    for rec in records:
        print(rec)
    # Shutdown JVM
    jpype.shutdownJVM()
