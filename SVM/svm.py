#The Wrapper fucntion for hadling all the MAp Reduce Jobs
import math
import sys
from mr_svm import MRSVM
import matplotlib.pyplot as plt
import numpy as np

# Initialise a Map-Reduce task
mr_job = MRSVM(args=[sys.argv[1]])

# Iterations handler
while(True):
    with mr_job.make_runner() as runner:
        runner.run()

        # Get the current weight
        f=open('weight','r')
        v=map(float,f.read().split(' '))
        f.close()

        # Set previous weight
        prev=v[:]

        # Update weight using the output of the reducer
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            v[key]+=value

        # Set the updated weight
        f=open('weight','w')
        val = map(str, v)
        f.write(" ".join(val))
        f.close()

        # Convergence criteria
        diff=sum(abs(a - b) for a, b in zip(prev,v))
        if(diff < 0.1):
            break



## Test Accuracy of output
# Get Test data
test=map(lambda x: map(float,x),map(lambda x:x.split(' '),map(lambda x: x.strip(), open(sys.argv[2]).readlines())))

# Initialisation of number of examples in each class
c1=0 # Number of correctly classified examples in class1
c2=0 # Number of correctly classified examples in class2

# Test classification
isC1=False
isC2=False


# Test each test data point
for t in test:

    # Target output 
    if(t.pop()==1):
        isC1=True
    else:
        isC2=True

    # Set x0 to 1 to comply with variable threshold
    t.append(1)

    # Discriminant function fn calculated 
    fn = sum((a*b) for a,b in zip(v,t))

    # Test for correct classification
    if fn>0 and isC1:
        c1+=1
    elif fn <=0 and isC2:
        c2+=1

    # Set test flags to 0 for next iteration
    isC1=False
    isC2=False


# Final Accuracy
print "Accuracy on test data = ",float(c1+c2)/len(test)



## Plotting output line and training data

# Get training data for plotting purpose
train=map(lambda x: map(float,x),map(lambda x:x.split(' '),map(lambda x: x.strip(), open(sys.argv[1]).readlines())))

# Classify them into their respective classes
c1t=[]
c2t=[]
for t in train:
    if(t.pop()==1):
        c1t.append(t)
    else:
        c2t.append(t)

# Get final weight
f=open('weight','r')
fin_weight=map(float,f.read().split(' '))
print "Weight vector =", fin_weight
f.close()

# Find slope and intercept
slope=(-fin_weight[0])/fin_weight[1]
intercept=(-fin_weight[2])/fin_weight[1]

# Plot line 
minx=math.floor(min(np.asarray(c1t+c2t)[:,0]))
maxx=math.ceil(max(np.asarray(c1t+c2t)[:,0]))
xt=range(int(minx),(int(maxx)+1))
ablineValues = []
for i in xt:
  ablineValues.append(slope*i+intercept)

plt.plot(np.asarray(c1t)[:,0],np.asarray(c1t)[:,1],'ro',label='Class 1')
plt.plot(np.asarray(c2t)[:,0],np.asarray(c2t)[:,1],'go',label='Class 2')
plt.plot(xt,ablineValues,'b',label="Model line output")
plt.title("SVM using Map-Reduce")
plt.xlabel("x1")
plt.ylabel("x2")
plt.legend(loc="upper left")

plt.savefig("../svm_out.png")
plt.show()







