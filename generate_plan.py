import base64
import sys
from pathlib import Path
import jpype
from jpype.types import JString

if __name__ == "__main__":

    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=["jars/*"])
    if len(sys.argv) != 2:
        print("Usage: python generate_plan.py <sql_query_string>")
        exit(1)

    # SQL query to generate Substrait plan
    query = sys.argv[1]
    # Step 1: Read schema file into a list of CREATE TABLE statements
    schema_file = Path("./schema.sql")
    if schema_file.exists():
        with open(schema_file) as f:
            content = f.read()
        # Split by semicolon to get each CREATE TABLE as a single string
        schema_statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]
    else:
        print("schema.sql file missing.")
        exit(1)

    # Step 2: Convert Python list to Java ArrayList
    java_arraylist = jpype.java.util.ArrayList()
    for stmt in schema_statements:
        java_arraylist.add(JString(stmt))
    # Step 3: Instantiate SqlToSubstrait and call execute
    SqlToSubstrait = jpype.JClass('io.substrait.isthmus.SqlToSubstrait')
    sql_to_substrait = SqlToSubstrait()
    java_sql_string = JString(query)
    plan_proto = sql_to_substrait.execute(java_sql_string, java_arraylist)
    # Step 4: Serialize Protobuf Plan to Base64
    plan_bytes = plan_proto.toByteArray()
    base64_plan = base64.b64encode(plan_bytes).decode("utf-8")

    print("\nBase64 Encoded Substrait Plan:\n")
    print(base64_plan)
    jpype.shutdownJVM()