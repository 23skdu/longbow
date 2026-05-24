import glob

for file in glob.glob("internal/store/*_test.go"):
    with open(file, 'r') as f:
        content = f.read()
    
    if "func Fuzz" not in content:
        continue
        
    lines = content.split('\n')
    new_lines = []
    modified = False
    
    for i, line in enumerate(lines):
        new_lines.append(line)
        if line.startswith("func Fuzz") and " *testing.F)" in line:
            if i + 1 < len(lines) and "if testing.Short()" not in lines[i+1]:
                new_lines.append("\tif testing.Short() {")
                new_lines.append("\t\tf.Skip(\"skipping fuzz test in short mode\")")
                new_lines.append("\t}")
                modified = True
                
    if modified:
        with open(file, 'w') as f:
            f.write('\n'.join(new_lines))
        print(f"Skipped fuzz in {file}")
