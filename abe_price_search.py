
import json
import time
import subprocess
import sys
import ast

def book_query(title, subtitle, author):


    cmd = ["xvfb-run", "-a", "python3", "./production/selenium_abe.py", title, subtitle or "", author or ""]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("❌ Selenium Script failed:")
        print(result.stderr)
        return []

    # Return the output from the child script
    try:
        data = json.loads(result.stdout.strip())
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
        print("Raw output was:", result.stdout)
        return []

    return data
    

