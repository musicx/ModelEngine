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
                      help="test dataset files separated with ';'")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=','")
    parser.add_option("-v", "--vars", dest="vars", action="store", type="string",
                      help="variable list separated with ',' and wild characters is supported such as '*_zscl'")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output file name")
    parser.add_option("-n", "--node", dest="node", action="store", type="string",
                      help="nodes in the hidden layer, separated with ','")
    (options, args) = parser.parse_args()

    if options.key.find(',') >= 0 :
        keys = [x.lower() for x in options.key.split(',')]
    else :
        keys = [options.key.lower()]

    config_string = {'bad' : options.bad,
                     'train' : options.train}
    if options.test.find(';') >= 0 :
        tests = options.test.split(';')
    else :
        tests = [options.test]

    if options.dlm.startswith('x') :
        config_string['delimiter'] = '\\' + oct(int(options.dlm[1:], 16))
    else:
        config_string['delimiter'] = options.dlm

    config_string['role_ids'] = '\telse if'
    config_string['keep_vars'] = '"*_zscl", '