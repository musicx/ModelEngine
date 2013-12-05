import sys

lines = []
for item in sys.argv[1:-1] :
    for line in open(item) :
        lines.append(line)

fn = open(sys.argv[-1], 'w')
for line in lines :
    fn.write(line)
    fn.write("\n")
