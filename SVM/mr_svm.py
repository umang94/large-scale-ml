#Map Reduce Code for training a SVM ( Support Vector Machine)

from mrjob.job import MRJob
import re
import sys

class MRSVM(MRJob):
    def mapper_init(self):
        self.weight = map(float,open('/home/umang/Desktop/Part2/weight','r').read().split(' '))
 
    def mapper(self, _, line):
        inp=map(float,line.split(' '))
        x=inp[0:len(inp)-1]
        x.append(1)
        y=inp[-1]
        if (y==0):
          y=-1
        tmp=sum(a*b for a,b in zip(self.weight,x))
        cost=10
        lr=0.001
        if(tmp*y < 1):
            for i in xrange(len(x)):
              yield(i,((-1)*lr)*self.weight[i]+(cost)*lr*y*x[i])


    def reducer(self, key, updates):
        total_upd=0
        for upd in updates:
            total_upd+=upd

        yield (key,total_upd)


if __name__ == '__main__':
    MRPerceptron.run()
