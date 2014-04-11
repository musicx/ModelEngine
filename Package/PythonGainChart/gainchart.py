import json
import logging
from optparse import OptionParser
import os

__author__ = 'yijiliu'


def parse_source(src_string) :
    if os.path.exists(src_string) :
        line = open(src_string).read()
    else :
        line = src_string
    try :
        sources = json.loads(line)
    except ValueError:
        return None
    #for source in sources :
    #    if not os.path.exists(source["path"]) :
    #        return None
    return sources

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-s", "--src", dest="src", help="raw string of scored files encoded with json or json file. example:\n" +
                                                      '{[{"path":"score.csv","score":["new_score"],"id":["trans_id"],"base":["bad","weight","loss","region"]}]}', action="store", type="string")
    parser.add_option("-b", "--bad", dest="bad", help="names of bad variables", action="store", type="string")
    parser.add_option("-w", "--wgt", dest="wgt", help="names of weight variables, format: {unit|1;dollar}", action="store", type="string")
    parser.add_option("-c", "--cat", dest="cat", help="optional, class combination in format [[class_var1,class_var2],[class_var3]]", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file, if not given, stdout is used", action="store", type="string")
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, accept xASCII format, default=,", action="store", type="string", default=",")
    parser.add_option("-o", "--out", dest="out", help="output performance file", action="store", type="string")
    (options, args) = parser.parse_args()

    if not options.src:
        print "You must specify the input files!"
        exit()

    sources = parse_source(options.src)
    if sources is None :
        print "Error occurs during parsing the source json"
        exit()
    print sources

    if not options.bad:
        print "You must specify the bad variables!"
        exit()

    if not options.wgt:
        print "You must specify the weight variables!"
        exit()
    if options.wgt.find(';') < 0 :
        print "Error occurs during parsing the weight"
        exit()
    else :
        unit_weight = options.wgt[:options.wgt.find(';')]
        dollar_weight = options.wgt[options.wgt.find(';') + 1:]

    if options.dlm.startswith('x'):
        options.dlm = chr(int(options.dlm[1:]))

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    logging.info("program started...")
