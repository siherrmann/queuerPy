#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.runner import Runner

def test_function(param1: int, param2: str) -> int:
    print(f"Test function called with param1={param1}, param2='{param2}'")
    param2_int = int(param2)
    result = param1 + param2_int
    print(f"Test function returning {result}")
    return result

def main():
    print("Testing Runner directly...")
    
    # Create runner
    runner = Runner(test_function, [5, "12"])
    
    # Start execution
    print("Starting runner...")
    success = runner.run()
    print(f"Runner.run() returned: {success}")
    
    # Get results
    print("Getting results...")
    try:
        results = runner.get_results(timeout=5.0)
        print(f"Results: {results}")
    except Exception as e:
        print(f"Error getting results: {e}")

if __name__ == "__main__":
    main()