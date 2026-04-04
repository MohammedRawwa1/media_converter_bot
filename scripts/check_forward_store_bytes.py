p='utils/forward_store.py'
with open(p,'rb') as f:
    data=f.read()
for i,b in enumerate(data):
    if b>127:
        print(i,b,hex(b))
        break
else:
    print('no high bytes')
