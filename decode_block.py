import base64
from typing import List
import jpype
from jpype.types import JByte
from pathlib import Path

def block_to_record_strings(block) -> List[str]:
    result = []
    row_count = block.getRowCount()
    BlockUtils = jpype.JClass('com.amazonaws.athena.connector.lambda.data.BlockUtils')
    for i in range(row_count):
        row_str = BlockUtils.rowToString(block, i)
        result.append(row_str)
    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python decode_block.py <base64_schema_string> <base64_records_string>")
        exit(1)

    base64_schema = sys.argv[1]
    base64_records = sys.argv[2]

    if not jpype.isJVMStarted():
        jpype.startJVM("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
                       "-Darrow.memory.allow-unsafe=true", "-XX:MaxDirectMemorySize=512m", classpath=["jars/*"])

    schema_bytes = base64.b64decode(base64_schema)
    records_bytes = base64.b64decode(base64_records)

    java_bytes_schema = jpype.JArray(JByte)(schema_bytes)

    # Step 1: Deserialize Schema
    ByteBuffer = jpype.JClass('java.nio.ByteBuffer')
    schema_buffer = ByteBuffer.wrap(java_bytes_schema)
    ArrowSchemaClass = jpype.JClass('org.apache.arrow.vector.types.pojo.Schema')
    schema_obj = ArrowSchemaClass.deserialize(schema_buffer)

    # Step 2: Create Block (Empty for now)
    BlockAllocatorImpl = jpype.JClass('com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl')
    allocator = BlockAllocatorImpl()
    block_obj = allocator.createBlock(schema_obj)

    # Step 3: Manually parse records_bytes and populate Block
    BlockUtils = jpype.JClass('com.amazonaws.athena.connector.lambda.data.BlockUtils')
    FieldResolver = jpype.JClass('com.amazonaws.athena.connector.lambda.data.FieldResolver')
    field_resolver = FieldResolver()

    # Step 4: Extract human-readable records
    records = block_to_record_strings(block_obj)
    for rec in records:
        print(rec)

    jpype.shutdownJVM()
