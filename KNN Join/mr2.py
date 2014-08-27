from mrjob.job import MRJob
import re
import sys
import pprint
import math

# Number of nearest neighbours
k = 6

class MR2(MRJob):

    def mapper(self, _, line):
      
      # Get individual points
      inp=line.split(',')[:-1]

      # Initialise r and s to hold points
      r=[]
      s=[]
      
      # Classify points
      for elem in inp:
        elem = elem.split(' ')
        if elem[0]=='r':
          r.append(elem[2:])
        else:
          s.append(elem[2:])

      # Find kNN in s for each point in r
      for i in range(len(r)):
        d=[]
        for dest in s:
          d.append([math.sqrt(sum(abs(float(a)-float(b))**2 for a,b in zip(r[i],dest))), dest])
        d.sort(key=lambda x:float(x[0]))

        # emit the k nearest neighbours
        for j in d[:k]:
          yield(r[i],j)


    def reducer(self, key, values):
        # Print the nearest neighbours
        print "\nPoint:",
        values=map(lambda y: [y[0], map(float, y[1])], values)
        pprint.pprint(map(float,list(key)))
        print "Nearest ",k,"neighbors"
        pprint.pprint(list(values))
        yield(key,values)



if __name__ == '__main__':
    MRPerceptron.run()
