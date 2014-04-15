from collections import OrderedDict
import logging
from optparse import OptionParser
import os
import random
import subprocess
import time
import sys

__author__ = 'yijiliu'

def ignore_exception(IgnoreException=Exception, DefaultVal=None):
    """ Decorator for ignoring exception from a function
    e.g.   @ignore_exception(DivideByZero)
    e.g.2. ignore_exception(DivideByZero)(Divide)(2/0)
    """
    def dec(function):
        def _dec(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except IgnoreException:
                return DefaultVal
        return _dec
    return dec


def parseBin(options) :
    if not options.bin or not os.path.exists(options.bin):
        return {}
    zscls = {}
    zfloat = ignore_exception(ValueError, 0)(float)
    ofloat = ignore_exception(ValueError, 1.0)(float)
    with open(options.bin, 'r') as fn :
        while True:
            line = fn.readline()
            if not line :
                break
            if not line.startswith('====') :
                continue
            parts = line.split(',')
            if parts[2].strip() != 'num' :
                continue
            zscls[parts[1]] = (zfloat(parts[3]), ofloat(parts[4]))
    return zscls


def parseVarList(options) :
    if not options.var or not os.path.exists(options.var) :
        return {}
    vardict = {}
    with open(options.var, 'r') as fn :
        while True :
            line = fn.readline()
            if not line :
                break
            parts = [x.strip() for x in line.split(",")]
            is_num = True if parts[2] == '1' else False
            vardict[parts[0]] = (parts[1], is_num)
    return vardict


def parseHeader(options) :
    with open(options.raw, 'r') as fn :
        line = fn.readline()
    parts = line.split(options.dlm)
    #variables = OrderedDict()
    variables = []
    for part in parts :
        #variables[part.strip()] = None
        variables.append(part.strip())
    return variables


# this is template file for data transformantion
# following variables should be assigned before this part of code
#   outputData: path to the output data
#   firstLine: the header of the output file
#   rawData: path to the source dataset
#   rawDataDlm: delimiter of the raw dataset
template = """
_eps = 1e-36
def ignore_exception(IgnoreException=Exception, DefaultVal=None):
    def dec(function):
        def _dec(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except IgnoreException:
                return DefaultVal
        return _dec
    return dec

num_parse = ignore_exception(ValueError, None)(float)

def char_parse(input) :
    return input

fout = open(outputData, 'w')

fout.write(firstLine)
fout.write("\\n")

with open(rawData, 'r') as fn :
    line = fn.readline()
    while True :
        line = fn.readline()
        if not line :
            break
        parts = [x.strip() for x in line.split(rawDataDlm)]
        # here goes the variable reader
        # 2 tabs before the code lines
        # following certain pattern:
        # variable_name = num_parse(parts[i])
        # or
        # variable_name = char_parse(parts[i])
{0}

        # here goes the transform parts
        # 2 tabs before the code lines
        # it could be z-scaling code
        # variable_zscl = None if variable is None else (variable - mean) / std
        # or WOE / ZWOE mapping
        # if variable_name = XXX :
        #     variable_woe = YYY
        # or scorecards
        # score = a * X1 + b * X2 + ...
{1}

        # here goes the output parts
        # 2 tabs before the code lines
        # it should follow the same order as the firstLine
        # and looks like
        # fout.write(variable_name_with_format)
{2}
        fout.write("\\n")
        fout.flush()
"""


def parseDrop(options) :
    if not options.drop :
        return set()
    drops = set()
    with open(options.drop) as fn:
        while True:
            line = fn.readline()
            if not line :
                break
            drops.add(line.strip().lower())
    return drops


if __name__ == "__main__" :
    parser = OptionParser()
    parser.add_option("-r", "--raw", dest="raw", help="raw csv file", action="store", type="string")
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, default=,", action="store", type="string", default=",")
    parser.add_option("-b", "--bin", dest="bin", help="z-scaled input file, choose one or more among {bin, woe, zscl}", action="store", type="string")
    parser.add_option("-w", "--woe", dest="woe", help="WoE input file, choose one or more among {bin, woe, zscl}", action="store", type="string")
    parser.add_option("-z", "--zscl", dest="zscl", help="z-scaled WoE input file, choose one or more among {bin, woe, zscl}", action="store", type="string")
    parser.add_option("-v", "--var", dest="var", help="variable list file, required for woe transform", action="store", type="string")
    parser.add_option("-p", "--drop", dest="drop", help="drop variable list, optional", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file", action="store", type="string")
    parser.add_option("-o", "--out", dest="out", help="output file", action="store", type="string")
    (options, args) = parser.parse_args()

    if not options.raw or not os.path.exists(options.raw):
        print "You must specify the source dataset file!"
        exit()
    if (not options.var or not os.path.exists(options.var)) and (not options.bin or not os.path.exists(options.bin)) :
        print "You must provide either the fine bin result file (.bin) or the coarse bin result variable list file (_lst.txt)!"
        exit()
    if not options.out :
        options.out = options.raw + ".out"
    if options.log :
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s', filename=options.log, filemode='w')
    else :
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    if options.dlm.startswith('x') :
        options.dlm = chr(int(options.dlm[1:]))

    # get the sequence of raw variables
    header = parseHeader(options)
    # get the raw mean and std
    rawZscl = parseBin(options)
    # get the WOE variable list
    woeVars = parseVarList(options)
    # get the drop variable list
    dropVars = parseDrop(options)

    variableNames = []
    writeString = "\n"
    first = True
    for var in header :
        if var.lower() in dropVars :
            continue
        if first :
            writeString += "        fout.write('{0}'.format({1}))\n".format('{0}', var)
            first = False
        else :
            writeString += "        fout.write('{0}'.format(rawDataDlm, {1}))\n".format('{0}{1}', var)
        variableNames.append(var)
    if options.bin :
        for var in header:
            if var.lower() in dropVars :
                continue
            if var in rawZscl :
                variableNames.append(var + "_zscl")
                writeString += "        fout.write('{0}'.format(rawDataDlm, {1}))\n".format("{0}{1}", var + "_zscl")
    if options.woe :
        for var in header:
            if var.lower() in dropVars :
                continue
            if var in woeVars :
                variableNames.append(woeVars[var][0])
                writeString += "        fout.write('{0}'.format(rawDataDlm, {1}))\n".format("{0}{1}", woeVars[var][0])
    if options.zscl :
        for var in header:
            if var.lower() in dropVars :
                continue
            if var in woeVars :
                variableNames.append(woeVars[var][0] + "_zscl")
                writeString += "        fout.write('{0}'.format(rawDataDlm, {1}))\n".format("{0}{1}", woeVars[var][0] + "_zscl")
    firstLine = options.dlm.join(variableNames)

    outputContent = "outputData = {0}\n".format(repr(options.out))
    outputContent += "firstLine = {0}\n".format(repr(firstLine))
    outputContent += "rawData = {0}\n".format(repr(options.raw))
    outputContent += "rawDataDlm = {0}\n".format(repr(options.dlm))

    readString = "\n"
    for ind in xrange(len(header)) :
        var = header[ind]
        if var in woeVars and woeVars[var][1] :
            readString += "        {0} = num_parse(parts[{1}])\n".format(var, ind)
        else :
            readString += "        {0} = char_parse(parts[{1}])\n".format(var, ind)

    zsclString = "\n"
    if options.bin :
        for var in rawZscl :
            zsclString += "        {0}_zscl = None if {0} is None else ({0} - {1}) / ({2} + _eps)\n".format(var, rawZscl[var][0], rawZscl[var][1])

    woeString = "\n"
    if options.woe :
        for line in open(options.woe) :
            woeString += "        " + line

    zwoeString = "\n"
    if options.zscl :
        for line in open(options.zscl) :
            zwoeString += "        " + line

    transformString = "\n".join([zsclString, woeString, zwoeString])

    tempFileName = "transform_{0:.0f}_{1:d}.py".format(time.time(), random.randint(0,100))

    with open(tempFileName, 'w') as fn :
        fn.write(outputContent)
        fn.write("\n")
        fn.write(template.format(readString, transformString, writeString))

    if sys.platform.startswith("win") :
        runningOutput = subprocess.check_output(["cmd /C python " + tempFileName + "& exit 0"], stderr=subprocess.STDOUT, shell=True)
    else :
        runningOutput = subprocess.check_output(["python " + tempFileName + "; exit 0"], stderr=subprocess.STDOUT, shell=True)

    #print "Process Output:"

    print runningOutput
    os.remove(tempFileName)
