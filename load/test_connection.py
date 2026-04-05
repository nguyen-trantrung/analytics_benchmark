#!/usr/bin/env python3
import sys

sys.path.insert(0, ".")
from load_data_tidb import DatabaseConfig, TiDBLoader

config = DatabaseConfig()
print(f"Testing connection with config: {config}")
loader = TiDBLoader(config)
if loader.test_connection():
    print("Connection test passed")
else:
    print("Connection test failed")
    sys.exit(1)
