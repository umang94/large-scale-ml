import matplotlib.pyplot as plt
import numpy as np


f=open('plot_out','r')
point=[map(float,f.readline().strip().split(" "))]
print point
nb=[]
for i in xrange(5):
    nb.append(map(float,f.readline().strip().split(" ")))

print nb

s=[]
train=map(lambda x:x.split(' '),map(lambda x: x.strip(), open('dt.txt').readlines()))
for t in train:
    if t[0]=='s':
        s.append(map(float,t[1:]))

plt.plot(np.asarray(s)[:,0],np.asarray(s)[:,1],'go',label='S Class')
plt.plot(np.asarray(nb)[:,0],np.asarray(nb)[:,1],'bo',label='5 nearest neighbours')
plt.plot(np.asarray(point)[:,0],np.asarray(point)[:,1],'ro',label='Point r')
plt.title("kNN")
plt.xlabel("x1")
plt.ylabel("x2")
plt.legend(loc="upper left")

plt.savefig("../knn_out.png")
plt.show()




