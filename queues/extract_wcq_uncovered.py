#!/usr/bin/env python3
import re
from html.parser import HTMLParser

class CoverageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_uncovered = False
        self.current_line = None
        self.uncovered_lines = []
        self.in_code = False
        self.code_content = []
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == 'td' and attrs_dict.get('class') == 'uncovered-line':
            self.in_uncovered = True
        elif tag == 'td' and attrs_dict.get('class') == 'line-number' and self.in_uncovered:
            # Extract line number from the anchor
            for attr_name, attr_value in attrs:
                if attr_name == 'class' and attr_value == 'line-number':
                    self.current_line = True
        elif tag == 'a' and self.current_line:
            for attr_name, attr_value in attrs:
                if attr_name == 'name':
                    self.current_line = int(attr_value[1:])  # Remove 'L' prefix
        elif tag == 'td' and attrs_dict.get('class') == 'code' and self.in_uncovered:
            self.in_code = True
            
    def handle_endtag(self, tag):
        if tag == 'tr' and self.in_uncovered:
            self.in_uncovered = False
            self.current_line = None
            self.in_code = False
            if self.code_content:
                code = ''.join(self.code_content).strip()
                self.code_content = []
                
    def handle_data(self, data):
        if self.in_code and self.current_line:
            # Store the line info
            if self.current_line not in [x[0] for x in self.uncovered_lines]:
                self.uncovered_lines.append((self.current_line, data.strip()))
            self.code_content.append(data)

# Read the HTML file
with open('/home/baran/Dokumente/Uni/MA/MA/queues/coverage/html/coverage/home/baran/Dokumente/Uni/MA/MA/queues/src/mpmc/wcq_queue.rs.html', 'r') as f:
    html_content = f.read()

# Find all uncovered lines using regex
uncovered_pattern = r'<tr>.*?<td class=\'line-number\'><a name=\'L(\d+)\'.*?</a></td><td class=\'uncovered-line\'>.*?</td><td class=\'code\'><pre>(.*?)</pre></td></tr>'
uncovered_matches = re.findall(uncovered_pattern, html_content, re.DOTALL)

# Also find uncovered regions (red spans)
red_region_pattern = r'<tr>.*?<td class=\'line-number\'><a name=\'L(\d+)\'.*?</a></td>.*?<span class=\'region red\'>(.*?)</span>'
red_matches = re.findall(red_region_pattern, html_content, re.DOTALL)

print("UNCOVERED LINES IN WCQ QUEUE:")
print("=" * 80)

# Collect all uncovered lines
uncovered_lines = {}
for line_num, code in uncovered_matches:
    line_num = int(line_num)
    # Clean up the code
    code = re.sub(r'<[^>]+>', '', code).strip()
    uncovered_lines[line_num] = code

# Add lines with red regions
for line_num, code in red_matches:
    line_num = int(line_num)
    # Clean up the code
    code = re.sub(r'<[^>]+>', '', code).strip()
    if line_num not in uncovered_lines:
        uncovered_lines[line_num] = code

# Sort by line number
sorted_lines = sorted(uncovered_lines.items())

# Group by function/method
current_function = None
function_coverage = {}

# Read the source file to get function context
with open('/home/baran/Dokumente/Uni/MA/MA/queues/src/mpmc/wcq_queue.rs', 'r') as f:
    source_lines = f.readlines()

# Find function boundaries
function_starts = []
for i, line in enumerate(source_lines):
    if 'fn ' in line and not line.strip().startswith('//'):
        # Extract function name
        match = re.search(r'fn\s+(\w+)', line)
        if match:
            function_starts.append((i + 1, match.group(1)))

# Map uncovered lines to functions
for line_num, code in sorted_lines:
    # Find which function this line belongs to
    func_name = "Global/Struct definitions"
    for start_line, name in reversed(function_starts):
        if line_num >= start_line:
            func_name = name
            break
    
    if func_name not in function_coverage:
        function_coverage[func_name] = []
    function_coverage[func_name].append((line_num, code))

# Print summary by function
print("\nUNCOVERED CODE BY FUNCTION/METHOD:")
print("-" * 80)

for func_name, lines in sorted(function_coverage.items()):
    print(f"\n{func_name}:")
    for line_num, code in lines:
        print(f"  Line {line_num}: {code[:100]}...")

print("\n\nSUMMARY:")
print(f"Total uncovered lines: {len(uncovered_lines)}")
print(f"Functions with uncovered code: {len(function_coverage)}")