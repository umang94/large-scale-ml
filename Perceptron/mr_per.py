# Code for training a perceptron using map reduce.

from mrjob.job import MRJob
import sys
#class for initialiing the Map Reduce process.
class MRPerceptron(MRJob):
    #initialising the mapper . This function will be run everytime a mapper is run
    def mapper_init(self):
        self.weight = map(float,open('/home/umang/Desktop/Part1/weight','r').read().split(' '))
 
#The Mapper 
    def mapper(self, _, line):
        inp=map(float,line.split(' '))
        x=inp[0:len(inp)-1]
        x.append(1)
        y=inp[-1]
        if (y==0):
          y=-1
        tmp=sum(a*b for a,b in zip(self.weight,x))
        if(tmp*y <= 0):
            for i in xrange(len(x)):
              yield(i,0.1*y*x[i])

# The Reducer
    def reducer(self, key, updates):
        total_upd=0
        for upd in updates:
            total_upd+=upd

        yield (key,total_upd)


if __name__ == '__main__':
    MRPerceptron.run()
