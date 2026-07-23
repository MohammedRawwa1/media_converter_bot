import importlib
import os
import sys
import traceback

# ensure project root is on sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

modules = [
    'main',
    'handlers',
    'media_converter',
    'models',
    'tasks.cleanup_tasks',
    'tasks.conversion_tasks',
    'tasks.media_schema',
    'utils.ffmpeg_runner',
    'utils.job_queue',
    'utils.job_store',
    'utils.file_utils',
    'utils.progress_tracker',
    'workers.ffmpeg_worker',
    'config',
]

errors = []
for m in modules:
    try:
        importlib.import_module(m)
        print(f'OK: {m}')
    except Exception:
        tb = traceback.format_exc()
        errors.append((m, tb))
        print(f'ERR: {m}')

if errors:
    print('\nSummary of import errors:')
    for name, tb in errors:
        print('---')
        print(name)
        print(tb)
    raise SystemExit(2)
else:
    print('\nAll imports OK')
