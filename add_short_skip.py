import os
import glob
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # Skip if already has testing.Short
    if "testing.Short()" in content:
        return False

    if os.path.getsize(filepath) < 10000 and "time.Sleep" not in content and "for i :=" not in content:
        return False

    lines = content.split('\n')
    new_lines = []
    modified = False
    
    for i, line in enumerate(lines):
        new_lines.append(line)
        if line.startswith("func Test") and " *testing.T)" in line:
            # Check if next line is not already a skip
            if i + 1 < len(lines) and "if testing.Short()" not in lines[i+1]:
                indent = "\t"
                new_lines.append(indent + "if testing.Short() {")
                new_lines.append(indent + "\t\tt.Skip(\"skipping test in short mode\")")
                new_lines.append(indent + "}")
                modified = True

    if modified:
        with open(filepath, 'w') as f:
            f.write('\n'.join(new_lines))
        print(f"Modified {filepath}")
        return True
    return False

if __name__ == '__main__':
    count = 0
    for file in glob.glob("internal/store/*_test.go"):
        if process_file(file):
            count += 1
    print(f"Total files modified: {count}")
