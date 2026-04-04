import py_compile,traceback
try:
    py_compile.compile('utils/forward_store.py', doraise=True)
    print('compiled ok')
except Exception:
    traceback.print_exc()
