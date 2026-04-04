p='utils/forward_store.py'
with open(p,'rb') as f:
    for i,line in enumerate(f,1):
        if 78 <= i <= 106:
            print(i, repr(line))
        if i>106:
            break
