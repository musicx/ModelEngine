from optparse import OptionParser

__author__ = 'yijiliu'

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input", action="store", type="string",
                      help="input datasets separated with ','")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output file names separated with ',', if not given, '_mani' will be appended by default")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=,")
    parser.add_option("-t", "--type", dest="type", action="store", type="string",
                      help="optional variable type files / json strings in format var_name[,k[eep]|d[rop]] on each line. " +
                           "Multiple files should be separated with ';' in the same order of the input data files")
    parser.add_option("-k", "--keep", dest="keep", action="store", type="string",
                      help="optional variable list for simple keeping, whilchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-x", "--excl", dest="drop", action="store", type="string",
                      help="optional variable list for simple exclusion, wildchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-f", "--filter", dest="filter", action="store", type="string",
                      help="optional filtering criteria")
    (options, args) = parser.parse_args()

