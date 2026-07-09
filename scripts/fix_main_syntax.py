#!/usr/bin/env python3
"""Fix main.py syntax error and complete dead code cleanup."""

with open('main.py', 'r', encoding='utf-8') as f:
    content = f.read()

original_len = len(content)
changes = 0

# The dead code removal left _process_login_text without its final closing lines.
# After the 'return' at the end of the password handler, we need to add the
# closing lines for _process_login_text before main()'s try block.

# Find the pattern: the 'return' that closes the password handler,
# followed immediately by '    try:' (main's try block)
# We need to insert the missing closing lines between them.

target = (
    '            return\n'
    '    try:\n'
        '        req = Request('
)
# The issue is that after removing dead code, the function _process_login_text
# doesn't have its final _clear_login_flow + return.
# Let's find the actual pattern in the file.

# Search for the break point
idx1 = content.find('            return\n    try:\n        req = Request(')
if idx1 >= 0:
    # Insert the missing closing lines
    insert = (
        '            return\n'
        '        _clear_login_flow(user_id, context)\n'
        '        return\n'
        '\n'
        '    try:\n'
        '        req = Request('
    )
    old = '            return\n    try:\n        req = Request('
    content = content.replace(old, insert, 1)
    changes += 1
    print('1. Fixed _process_login_text closing lines')
else:
    print('1. SKIP: target pattern not found')
    # Debug
    idx = content.find('            return\n    try:\n')
    if idx >= 0:
        print(f'   Found partial match at index {idx}')
        print(f'   Context: {repr(content[idx:idx+200])}')
    else:
        # Try looking for the bare pattern
        idx = content.find('            return\n')
        if idx >= 0:
            after = content[idx+len('            return\n'):idx+len('            return\n')+50]
            print(f'   After return: {repr(after)}')

with open('main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'\nApplied {changes} changes ({original_len} -> {len(content)} chars)')
