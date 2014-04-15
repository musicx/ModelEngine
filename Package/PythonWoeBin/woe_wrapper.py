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
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, accept xASCII format, default=,", action="store", type="string")
    parser.add_option("-c", "--sel", dest="sel", help="# of variables selected based on iv", action="store", type="int", default=1000)

    extra_parameters = '''
    parser.add_option("-o", "--out", dest="out", help="output fine bin file", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file, if not given, stdout is used", action="store", type="string")
    parser.add_option("-n", "--num", dest="num", help="number of variables per batch, default is all at once", action="store", type="int", default=0)
    parser.add_option("-m", "--mess", dest="mess", help="minimum mess block, default=2% of total population", action="store", type="float", default=0.02)
    parser.add_option("-p", "--pct", dest="pct", help="minimum target block, default=2% of all bad or all good", action="store", type="float", default=0.02)

    parser.add_option("-b", "--bin", dest="bin", help="binning input file", action="store", type="string")
    parser.add_option("-w", "--woe", dest="woe", help="WoE output file", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file", action="store", type="string")
    parser.add_option("-f", "--suffix", dest="suf", help="suffix except _woe", action="store", type="string")
    parser.add_option("-i", "--iv", dest="iv", help="iv excluding threshold", action="store", type="float", default=0.001)
    parser.add_option("-r", "--mrate", dest="mr", help="missing rate excluding threshold", action="store", type="float", default=0.95)
    parser.add_option("-d", "--drpb", dest="drpb", help="drop bin threshold", action="store", type="float", default=100)
    parser.add_option("-g", "--aggb", dest="aggb", help="aggregation bin threshold", action="store", type="float", default=500)
    parser.add_option("-o", "--outb", dest="outb", help="output min bin threshold", action="store", type="float", default=0.02)

    parser.add_option("-r", "--raw", dest="raw", help="raw csv file", action="store", type="string")
    parser.add_option("-w", "--woe", dest="woe", help="WoE input file", action="store", type="string")
    parser.add_option("-b", "--bin", dest="bin", help="z-scaled input file", action="store", type="string")
    parser.add_option("-z", "--zscl", dest="zscl", help="z-scaled WoE input file", action="store", type="string")
    parser.add_option("-v", "--var", dest="var", help="variable list file", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file", action="store", type="string")
    parser.add_option("-o", "--out", dest="out", help="output file", action="store", type="string")
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, default=,", action="store", type="string", default=",")
    parser.add_option("-p", "--drop", dest="drop", help="drop variable list", action="store", type="string")
    '''

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
                     'dlm' : '-d ' + options.dlm if options.dlm else ''
    }

    fr = open('run_py_woe.sh', 'w')
    fr.write('python fine.py -s {opt[dev]} {opt[oot]} -b {opt[bad]} {opt[wgt]} {opt[head]} {opt[dlm]} {opt[type]} -l woe_fine_bin.log -o temp_result.bin\n\n'.format(opt=config_string))
    fr.write('python coarse.py -b temp_result.bin -l woe_coarse_bin.log\n\n'.format(opt=config_string))

    fr.write('python trans.py -r {opt[dev]} -z temp_result_woe_zscl.txt -v temp_result_lst.txt {opt[dlm]} -l woe_trans_data_train.log -o train_woe_zscl.csv\n'.format(opt=config_string))
    if options.val :
        if options.val.find(',') > 0 :
            tests = options.val.split(',')
        else :
            tests = [options.val]
        for ind in xrange(len(tests)) :
            config_string['test'] = tests[ind]
            config_string['data'] = 'oot{}'.format(ind+1)
            fr.write('python trans.py -r {opt[test]} -z temp_result_woe_zscl.txt -v temp_result_lst.txt {opt[dlm]} -l woe_trans_data_{opt[data]}.log -o {opt[data]}_woe_zscl.csv\n'.format(opt=config_string))


    fr.flush()
    fr.close()

    call(["chmod", "777", "run_py_woe.sh"])

    call(["bash", "run_py_woe.sh"])