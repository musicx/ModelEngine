import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

with open(outputfile, 'w') as fn:
    for line in open(inputfile) :
        try :
            fn.write(str(int(line) * 2))
            fn.write("\n")
        except :
            pass

