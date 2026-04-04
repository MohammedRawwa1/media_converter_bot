p='utils/forward_store.py'
with open(p,'rb') as f:
    data=f.read()
idx=None
for i,b in enumerate(data):
    if b>127:
        idx=i
        break
if idx is None:
    print('no non-ascii')
else:
    start=max(0,idx-40)
    end=min(len(data),idx+40)
    print('offset',idx)
    print(data[start:end])
    print(data[idx:idx+4])
