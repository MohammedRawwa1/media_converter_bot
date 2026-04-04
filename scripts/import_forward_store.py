import importlib.util, traceback
try:
    spec = importlib.util.spec_from_file_location('fs','utils/forward_store.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    print('import ok')
except Exception:
    traceback.print_exc()
