"""Dev helper: list persisted sessions and their merge_list contents.

Run from project root in venv:
.venv\Scripts\python.exe scripts\check_sessions.py
"""
import os
import json

BASE = os.path.join(os.path.dirname(__file__), "..")
SESS_DIR = os.path.join(BASE, "storage", "temp_sessions")

def main():
    print("Looking for session files in:", SESS_DIR)
    if not os.path.exists(SESS_DIR):
        print("No sessions directory found.")
        return
    files = [f for f in os.listdir(SESS_DIR) if f.startswith("session_") and f.endswith('.json')]
    if not files:
        print("No session files found.")
        return
    for fn in files:
        path = os.path.join(SESS_DIR, fn)
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            print(f"\n== {fn} ==")
            print("current_file:", data.get('current_file'))
            ml = data.get('merge_list') or []
            print(f"merge_list ({len(ml)}):")
            for i, it in enumerate(ml, 1):
                if isinstance(it, dict):
                    print(f" {i}. {it.get('type')} - {it.get('path')}")
                else:
                    print(f" {i}. {it}")
        except Exception as e:
            print("Failed to read", fn, e)

if __name__ == '__main__':
    main()
