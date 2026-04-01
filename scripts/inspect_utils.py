import sys
import os
import traceback

# Ensure project root is on sys.path so local `utils` package is imported
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

print('CWD:', sys.path[0])
print('sys.path:')
for p in sys.path:
    print(' -', p)

try:
    import utils
    print('Imported utils from', getattr(utils, '__file__', repr(utils)))
    try:
        import utils.storage
        print('utils.storage found at', utils.storage.__file__)
    except Exception as e:
        print('utils.storage import failed:', e)
        traceback.print_exc()
except Exception as e:
    print('Import utils failed:', e)
    traceback.print_exc()
