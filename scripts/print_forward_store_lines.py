import sys
p='utils/forward_store.py'
with open(p,'r',encoding='utf-8') as f:
    for i,line in enumerate(f,1):
        print(f"{i:04d}: {line.rstrip()}")
