import sys

lines = []
for item in sys.argv[1:-1] :
    for line in open(item) :
        if line.strip() != '':
            lines.append(line)

fn = open(sys.argv[-1], 'w')
for line in lines :
    fn.write(line.strip())
    fn.write("\n")
for i in xrange(5) :
    fn.write("{0}\n".format(i))

