import os

print("Current working directory:", os.getcwd())
print("\nLooking for rules.json in current directory...")
print("Files in current directory:", os.listdir('.'))
print("\nDoes rules.json exist?", os.path.exists("rules.json"))
print("Does ./rules.json exist?", os.path.exists("./rules.json"))
print("Does full path exist?", os.path.exists(r"C:\Users\dalto\clearline-pipeline-development\demo_functionality\rules.json"))