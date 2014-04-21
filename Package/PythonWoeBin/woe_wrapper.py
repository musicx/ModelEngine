from optparse import OptionParser
from subprocess import call

__author__ = 'yijiliu'




if __name__ == '__main__' :
    parser = OptionParser()
    parser.add_option("-s", "--src", dest="src", help="develop data file", action="store", type="string")
    parser.add_option("-v", "--val", dest="val", help="validate data file, if many, use ','", action="store", type="string")
    parser.add_option("-b", "--bad", dest="bad", help="index or names of bad variables", action="store", type="string")
    parser.add_option("-w", "--wgt", dest="wgt", help="index or names of weight, optional", action="store", type="string")
    parser.add_option("-e", "--head", dest="head", help="optional head line file, must use ',' as delimiter", action="store", type="string")
    parser.add_option("-t", "--type", dest="type", help="optional variable type file in format var_name[,keep|drop][,num|char] on each line", action="store", type="string")
    parser.add_option("-x", "--excl", dest="exclude", help="optional variable list for simple exclusion of binning, wildchar supported, separate with ','", action="store", type="string")
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, accept xASCII format, default=,", action="store", type="string")
    parser.add_option("-f", "--suf", dest="suffix", help="optional suffix for final output files", action="store", type="string")
    parser.add_option("-c", "--sel", dest="sel", help="# of variables selected based on iv", action="store", type="int", default=1000)
    (options, args) = parser.parse_args()

    if not options.src :
        print "You must specify the input .csv file!"
        exit()

    if not options.bad :
        print "You must specify the bad variables!"
        exit()

    config_string = {'dev' : options.src, 'bad' : options.bad,
                     'oot' : '-v ' + options.val if options.val else '',
                     'wgt' : '-w ' + options.wgt if options.wgt else '',
                     'head' : '-e ' + options.head if options.head else '',
                     'type' : '-t ' + options.type if options.type else '',
                     'dlm' : '-d ' + options.dlm if options.dlm else '',
                     'ex' : '-x ' + options.exclude if options.exclude else '',
                     'suf' : options.suffix + "_" if options.suffix is not None and options.suffix.strip() != '' else ''
    }

    fr = open('run_py_woe.sh', 'w')
    fr.write('python fine.py -s {opt[dev]} {opt[oot]} -b {opt[bad]} {opt[wgt]} {opt[head]} {opt[dlm]} {opt[type]} {opt[ex]} -l {opt[suf]}woe_fine_bin.log -o {opt[suf]}woe_result.bin\n\n'.format(opt=config_string))
    fr.write('python coarse.py -b {opt[suf]}woe_result.bin -l {opt[suf]}woe_coarse_bin.log\n\n'.format(opt=config_string))

    fr.write('python trans.py -r {opt[dev]} -z {opt[suf]}woe_result_woe_zscl.txt -v {opt[suf]}woe_result_lst.txt {opt[dlm]} -l {opt[suf]}woe_trans_data_train.log -o {opt[suf]}train_woe_zscl.csv\n'.format(opt=config_string))
    if options.val :
        if options.val.find(',') > 0 :
            tests = options.val.split(',')
        else :
            tests = [options.val]
        for ind in xrange(len(tests)) :
            config_string['test'] = tests[ind]
            config_string['data'] = 'oot{}'.format(ind+1)
            fr.write('python trans.py -r {opt[test]} -z {opt[suf]}woe_result_woe_zscl.txt -v {opt[suf]}woe_result_lst.txt {opt[dlm]} -l {opt[suf]}woe_trans_data_{opt[data]}.log -o {opt[suf]}{opt[data]}_woe_zscl.csv\n'.format(opt=config_string))


    fr.flush()
    fr.close()

    call(["chmod", "777", "run_py_woe.sh"])

    call(["bash", "run_py_woe.sh"])