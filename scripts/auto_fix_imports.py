import subprocess
import re

for _ in range(5):
    result = subprocess.run(['go', 'test', '-c', './internal/store/index/'], capture_output=True, text=True)
    if result.returncode == 0:
        print("Success!")
        break
        
    errors = result.stdout + result.stderr
    print(errors)
    fixed_something = False
    
    for line in errors.split('\n'):
        match = re.search(r'internal/store/index/(.*?\.go):(\d+):\d+: "(.*?)" imported (?:as \w+ )?and not used', line)
        if match:
            file_path = f'internal/store/index/{match.group(1)}'
            import_path = match.group(3)
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            # comment out the exact import path
            # handle cases where the import has an alias, e.g. basecore "github.com/..."
            new_content = re.sub(r'(\n\t*.*?"' + re.escape(import_path) + r'")', r'// \1', content)
            
            if new_content != content:
                with open(file_path, 'w') as f:
                    f.write(new_content)
                fixed_something = True

    if not fixed_something:
        break
