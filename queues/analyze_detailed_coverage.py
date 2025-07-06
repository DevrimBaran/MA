#!/usr/bin/env python3
import re
import html.parser
import sys

class CoverageAnalyzer(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.current_line = None
        self.in_code = False
        self.in_uncovered = False
        self.code_content = {}
        self.uncovered_lines = set()
        self.in_pre = False
        self.pre_content = []
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == 'a' and 'name' in attrs_dict and attrs_dict['name'].startswith('L'):
            self.current_line = int(attrs_dict['name'][1:])
            
        elif tag == 'td':
            if 'class' in attrs_dict:
                if 'uncovered-line' in attrs_dict['class']:
                    self.in_uncovered = True
                elif 'code' in attrs_dict['class']:
                    self.in_code = True
                    
        elif tag == 'pre' and self.in_code:
            self.in_pre = True
            self.pre_content = []
            
    def handle_endtag(self, tag):
        if tag == 'td':
            self.in_code = False
            self.in_uncovered = False
        elif tag == 'pre' and self.in_pre:
            self.in_pre = False
            if self.current_line and self.pre_content:
                self.code_content[self.current_line] = ''.join(self.pre_content)
                if self.in_uncovered:
                    self.uncovered_lines.add(self.current_line)
                    
    def handle_data(self, data):
        if self.in_pre:
            self.pre_content.append(data)

def analyze_coverage_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    analyzer = CoverageAnalyzer()
    analyzer.feed(content)
    
    # Find uncovered functions
    uncovered_functions = []
    current_function = None
    
    for line in sorted(analyzer.code_content.keys()):
        code = analyzer.code_content[line]
        
        # Check for function definitions
        if 'fn ' in code and '(' in code:
            fn_match = re.search(r'fn\s+(\w+)', code)
            if fn_match:
                current_function = (fn_match.group(1), line)
                
        # If this line is uncovered and we're in a function, mark the function
        if line in analyzer.uncovered_lines and current_function:
            if current_function not in uncovered_functions:
                uncovered_functions.append(current_function)
    
    # Find uncovered code blocks
    uncovered_blocks = []
    current_block = None
    
    for line in sorted(analyzer.uncovered_lines):
        if current_block is None:
            current_block = {'start': line, 'end': line, 'lines': [line]}
        elif line == current_block['end'] + 1:
            current_block['end'] = line
            current_block['lines'].append(line)
        else:
            if len(current_block['lines']) >= 3:  # Only report blocks of 3+ lines
                uncovered_blocks.append(current_block)
            current_block = {'start': line, 'end': line, 'lines': [line]}
            
    if current_block and len(current_block['lines']) >= 3:
        uncovered_blocks.append(current_block)
    
    return {
        'total_lines': len(analyzer.code_content),
        'uncovered_lines': len(analyzer.uncovered_lines),
        'coverage_percent': 100 * (1 - len(analyzer.uncovered_lines) / len(analyzer.code_content)) if analyzer.code_content else 0,
        'uncovered_functions': uncovered_functions,
        'uncovered_blocks': uncovered_blocks,
        'code_content': analyzer.code_content,
        'uncovered_line_numbers': analyzer.uncovered_lines
    }

def main():
    queues = [
        ('WCQ', 'coverage/html/coverage/home/baran/Dokumente/Uni/MA/MA/queues/src/mpmc/wcq_queue.rs.html'),
        ('YMC', 'coverage/html/coverage/home/baran/Dokumente/Uni/MA/MA/queues/src/mpmc/ymc_queue.rs.html'),
        ('Feldman-Dechev', 'coverage/html/coverage/home/baran/Dokumente/Uni/MA/MA/queues/src/mpmc/feldman_dechev_queue.rs.html'),
        ('Jiffy', 'coverage/html/coverage/home/baran/Dokumente/Uni/MA/MA/queues/src/mpsc/jiffy_queue.rs.html'),
    ]
    
    for name, filepath in queues:
        print(f"\n{'='*60}")
        print(f"Coverage Analysis: {name} Queue")
        print('='*60)
        
        result = analyze_coverage_file(filepath)
        
        print(f"Total lines: {result['total_lines']}")
        print(f"Uncovered lines: {result['uncovered_lines']}")
        print(f"Line coverage: {result['coverage_percent']:.1f}%")
        print(f"Gap to 80% coverage: {max(0, result['uncovered_lines'] - int(result['total_lines'] * 0.2)):.0f} lines need testing")
        
        print(f"\nUncovered functions ({len(result['uncovered_functions'])}):")
        for func_name, line in result['uncovered_functions'][:10]:
            print(f"  - {func_name} (line {line})")
            
        print(f"\nLarge uncovered blocks ({len(result['uncovered_blocks'])}):")
        for block in result['uncovered_blocks'][:5]:
            print(f"  - Lines {block['start']}-{block['end']} ({len(block['lines'])} lines)")
            # Show some context
            for i in range(max(0, block['start']-1), min(block['end']+2, max(result['code_content'].keys())+1)):
                if i in result['code_content']:
                    prefix = "  ! " if i in result['uncovered_line_numbers'] else "    "
                    print(f"{prefix}{i}: {result['code_content'][i][:60]}...")

if __name__ == '__main__':
    main()