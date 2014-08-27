from mrjob.job import MRJob
import re
import sys


class MR1(MRJob):

    def mapper(self, _, line):
        # read helper.py for interleave function
        import imp
        hp=imp.load_source('helper','/home/umang/Desktop/Part3/helper.py')

        # read input linewise
        inp=line.split(' ')
        typ=inp[0]
        x1=inp[1]
        x2=inp[2]

        # Calculate z value
        z=hp.interleave2(float(x1),float(x2))

        # Emit key and z value
        yield(typ,[z,float(x1),float(x2)])


    def reducer(self, key, values):
        values=list(values)

        # sort list according to z values
        values.sort(key=lambda x:int(x[0]))

        yield(key,values)



if __name__ == '__main__':
    MRPerceptron.run()
