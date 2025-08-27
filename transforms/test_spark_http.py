from pyhive import hive
import sys

try:
    print("Connecting to Spark Thrift Server via HTTP...")
    conn = hive.Connection(
        host='spark-thrift-server',
        port=10001,
        auth='NONE',
        transport_mode='http',
        http_path='cliservice'
    )
    print("Connection successful! Creating cursor...")
    cursor = conn.cursor()
    print("Cursor created. Executing a test query...")
    cursor.execute('SELECT 1 as test_col')
    print("Query executed successfully. Result:")
    print(cursor.fetchall())
    print("\n[SUCCESS] PyHive library is working correctly!")
except Exception as e:
    print(f"\n[FAILED] An error occurred in PyHive: {e}", file=sys.stderr)
    sys.exit(1)
