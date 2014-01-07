from optparse import OptionParser
import os


__author__ = 'yijiliu'

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="key", action="store", type="string",
                      help="primary key variables separated with ','")
    parser.add_option("-b", "--bad", dest="bad", action="store", type="string",
                      help="target bad tagging variable")
    parser.add_option("-t", "--train", dest="train", action="store", type="string",
                      help="training dataset file")
    parser.add_option("-e", "--test", dest="test", action="store", type="string",
                      help="test dataset file")
    parser.add_option("-s", "--sep", dest="sep", action="store", type="string",
                      help="training / validation separate variable in training dataset file")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=,")
    parser.add_option("-v", "--vars", dest="vars", action="store", type="string",
                      help="variable list separated with ',' and wild characters is supported such as '*_zscl'")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output file name")
    (options, args) = parser.parse_args()

