import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

mul = 1
for line in open(inputfile) :
    mul *= int(line)

fn = open(outputfile, 'w')
fn.write("%d" % mul)
fn.close()
