from mr1 import MR1
from mr2 import MR2
from mr2 import k
import math
import pprint
import sys

# make_chunk - function to divide a vector into equal chunks
def make_chunk(l, n):
  for i in xrange(0, len(l), n):
    yield l[i:i+n]


# Find z order values using Map-Reduce
mr_job = MR1(args=[sys.argv[1]])
with mr_job.make_runner() as runner:
  runner.run()
  for line in runner.stream_output():
    key, value = mr_job.parse_output_line(line)
    if(key=="r"):
      r=value
    else:
      s=value

# Divide S into equal sized chunks
s_chunks=list(make_chunk(s,k))

# Divide R into chunks corresponding to that of S
count=0 # index of r in R
rs_chunks=[[]] # rs_chunks contains r,s corresponding to s_chunks

# Code to divide r into chunks
map(lambda x:x.insert(0,'r'),r)
for i in range(1,len(s_chunks)+1):
  while((count<len(r)and(r[count][1] <= s_chunks[i][0][0]))):
    rs_chunks[i-1].append(r[count])
    count+=1
  rs_chunks.append([])

# Border case for dividing r into the chunks - 1
# If z value or r is greater than the largest z-value of s
while(count!=len(r)):
  rs_chunks[len(s_chunks)].append(r[count])
  count+=1

# Code to divide s into chunks
for i in range(len(s_chunks)):
  for j in range(len(s_chunks[i])):
    s_chunks[i][j].insert(0,'s')

for i in xrange(len(s_chunks)-1):
  rs_chunks[i].extend(s_chunks[i])
  if(i-1>=0):
    rs_chunks[i].extend(s_chunks[i-1])
  rs_chunks[i].extend(s_chunks[i+1])


# Border case for dividing s into the chunks - 1
rs_chunks[len(s_chunks)-1].extend(s_chunks[len(s_chunks)-1])
rs_chunks[len(s_chunks)-1].extend(s_chunks[len(s_chunks)-2])

# Border case for dividing s into the chunks - 2
rs_chunks[len(s_chunks)].extend(s_chunks[len(s_chunks)-1])

# Set input as chunk for the next map-reduce
f=open('sorteddt.txt','w')
for rs_chunk in rs_chunks:
  for rs_point in rs_chunk:
    rs_point=" ".join(map(str,rs_point))
    f.write(rs_point)
    f.write(',')
  f.write('\n')
f.close()

# Run second map-reduce job to find the nearest k neighbours
mr_job2 = MR2(args=['sorteddt.txt'])
with mr_job2.make_runner() as runner:
  runner.run()
















