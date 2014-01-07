import logging
from optparse import OptionParser

__author__ = 'yijiliu'

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-s", "--src", dest="src", help="develop data file", action="store", type="string")
    parser.add_option("-v", "--val", dest="val", help="validate data file, if many, use ','", action="store",
                      type="string")
    parser.add_option("-b", "--bad", dest="bad", help="index or names of bad variables", action="store", type="string")
    parser.add_option("-o", "--out", dest="out", help="output fine bin file", action="store", type="string")
    parser.add_option("-w", "--wgt", dest="wgt", help="index or names of weight, optional", action="store",
                      type="string")
    parser.add_option("-e", "--head", dest="head", help="optional head line file, must use ',' as delimiter",
                      action="store", type="string")
    parser.add_option("-t", "--type", dest="type",
                      help="optional variable type file in format var_name[,keep|drop][,num|char] on each line",
                      action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file, if not given, stdout is used", action="store",
                      type="string")
    parser.add_option("-n", "--num", dest="num", help="number of variables per batch, default is all at once",
                      action="store", type="int", default=0)
    parser.add_option("-m", "--mess", dest="mess", help="minimum mess block, default=2% of total population",
                      action="store", type="float", default=0.02)
    parser.add_option("-p", "--pct", dest="pct", help="minimum target block, default=2% of all bad or all good",
                      action="store", type="float", default=0.02)
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, accept xASCII format, default=,", action="store",
                      type="string", default=",")
    (options, args) = parser.parse_args()

    if not options.src:
        print "You must specify the input .csv file!"
        exit()

    if not options.bad:
        print "You must specify the bad variables!"
        exit()

    if options.dlm.startswith('x'):
        options.dlm = chr(int(options.dlm[1:]))

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    logging.info("program started...")
    if not checkHeader(options):
        print "header mismatch found in the dataset!"
        exit()

    bads, wgt, vnames = parseKeys(options.src, options)
    if not bads:
        print "bad variables parsing error!"
        exit()
    logging.info("key variables parsed...")

    if options.type:
        special_type, keeps, drops = parseType(options)
    else:
        special_type = {}
        keeps = set()
        drops = set()
    logging.info("special variable type parsed...")

    logging.info("start generating distinct values...")
    name_batch = {}
    variables = {}
    bad_names = {}
    for bad in bads:
        bad_names[bad] = vnames[bad]
    for vind in xrange(len(vnames)):
        if len(keeps) > 0:
            if vnames[vind].lower() in keeps:
                name_batch[vind] = vnames[vind]
        else:
            if vnames[vind].lower() not in drops and vind not in bads and vind != wgt:
                name_batch[vind] = vnames[vind]
        if 0 < options.num <= len(name_batch):
            # TODO: use multiprocessing to accelerate this process
            raw = distinctSortVariables(options, bad_names, wgt, name_batch, special_type)
            for variable in raw:
                variables[variable] = raw[variable]
            name_batch = {}
    raw = distinctSortVariables(options, bad_names, wgt, name_batch, special_type)
    for variable in raw:
        variables[variable] = raw[variable]

    if options.val:
        logging.info("start applying fine bin boundaries to other datasets...")
        vals = [x.strip() for x in options.val.split(",")]
        for val in vals:
            for variable in variables:
                variables[variable].newdataset()
                # TODO : combine this with the last on the batch level
            applyBoundaries(val, options, variables)

    logging.info("start outputing the binning results...")
    bad_names = list(bad_names.values())
    with open(options.out, "w") as fn:
        for variable in variables:
            fn.write(variables[variable].toString(bad_names))
            fn.flush()

    logging.info("fine binning job done!")
