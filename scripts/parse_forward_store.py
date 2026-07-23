import ast
import traceback

with open('utils/forward_store.py', encoding='utf-8') as f:
    s = f.read()
try:
    ast.parse(s)
    print('ast ok')
except Exception:
    traceback.print_exc()
