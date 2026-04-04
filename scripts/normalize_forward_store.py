p='utils/forward_store.py'
with open(p,'r',encoding='utf-8') as f:
    s=f.read()
# normalize newlines and remove any stray carriage returns
s=s.replace('\r\n','\n')
# write back
with open(p,'w',encoding='utf-8',newline='\n') as f:
    f.write(s)
print('rewrote file')
