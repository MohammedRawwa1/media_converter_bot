import ast,traceback
s=open('utils/forward_store.py','r',encoding='utf-8').read()
try:
    ast.parse(s)
    print('ast ok')
except Exception:
    traceback.print_exc()
