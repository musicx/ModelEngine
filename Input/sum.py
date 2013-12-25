import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

sum = 0
for line in open(inputfile) :
    try :
        sum += int(line)
    except :
        pass

fn = open(outputfile, 'w')
fn.write("%d" % sum)
fn.close()
