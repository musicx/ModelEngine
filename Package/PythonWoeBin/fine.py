from collections import defaultdict
import fnmatch
import json
import logging
from optparse import OptionParser
import math
import os

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


class Stats :
    def __init__(self):
        self.bad = 0.0
        self.good = 0.0
        self.ind = 0.0
        self.wbad = 0.0
        self.wgood = 0.0
        self.wind = 0.0

    def total(self):
        return self.bad + self.good + self.ind

    def wtotal(self):
        return self.wbad + self.wgood + self.wind

    def copy(self):
        dup = Stats()
        dup.bad = self.bad
        dup.good = self.good
        dup.ind = self.ind
        dup.wbad = self.wbad
        dup.wgood = self.wgood
        dup.wind = self.wind
        return dup

    def add(self, other):
        self.bad += other.bad
        self.good += other.good
        self.ind += other.ind
        self.wbad += other.wbad
        self.wgood += other.wgood
        self.wind += other.wind

    def minus(self, other):
        self.bad -= other.bad
        self.good -= other.good
        self.ind -= other.ind
        self.wbad -= other.wbad
        self.wgood -= other.wgood
        self.wind -= other.wind

    def toString(self, dlm=","):
        return dlm.join([str(x) for x in [self.total(), self.good, self.bad, self.wtotal(), self.wgood, self.wbad]])


class UniBin :
    def __init__(self, value=None, bads=None):
        self.value = value
        self.stats = {}
        if bads is not None:
            for bad in bads :
                self.stats[bad] = Stats()

    def mergeStats(self, other):
        for bn in self.stats :
            self.stats[bn].add(other.stats[bn])

    def mergeValue(self, other):
        self.value = max(self.value, other.value)


class Distribution :
    def __init__(self):
        self.mean = 0.0
        self.median = 0.0
        self.std = 0.0
        self.mode = ""
        self.pct_1 = 0.0
        self.pct_5 = 0.0
        self.pct_10 = 0.0
        self.pct_90 = 0.0
        self.pct_95 = 0.0
        self.pct_99 = 0.0

    def toString(self, type, dlm=","):
        if type == "num" :
            return dlm.join(["{0}".format(x) for x in [self.mean, self.std,
                                                       self.pct_1, self.pct_5, self.pct_10,
                                                       self.median,
                                                       self.pct_90, self.pct_95, self.pct_99]])
        elif type == "char" :
            return self.mode


class BadRate :
    def __init__(self):
        self.total = 0.0
        self.missing = 0.0
        self.nonmiss = 0.0
        self.bad = 0.0
        self.mbad = 0.0
        self.nmbad = 0.0

    def toString(self, dlm=","):
        return dlm.join(["{0:.4e}".format(x) for x in [self.total, self.missing, self.nonmiss]])


class Variable :
    def __init__(self, name):
        self.name = name
        self.distincts = {}             # distinct bins from source data if format ("value":UniBin)
        self.bins = defaultdict(list)   # sorted and merged bins on different bads, type corrected
        self.val_datasets = []          # bins for validation datasets
        self.type = "char"
        self.special = False
        self.distribution = Distribution()
        self.badrates = {}

    def bin(self, mess, pct, sums) :
        if self.type == "char" :
            bins = list(self.distincts.values())
            bn = ""
            for bad_name in sums :
                self.bins[bad_name] = bins
                bn = bad_name  # it is only used for fetching the weighted total, so it doesn't matter which specific bad it is
            modenum = []
            for cbin in bins :
                modenum.append((cbin.value, cbin.stats[bn].wtotal()))
            modenum.sort(key=lambda k: k[1], reverse=True)
            ind = 0
            while modenum[ind][0] == "" :
                ind += 1
            self.distribution.mode = modenum[ind][0]
            for bad_name in sums :
                self.badrates[bad_name] = BadRate()
                for cbin in self.bins[bad_name] :
                    if cbin.value == "" :
                        self.badrates[bad_name].mbad += cbin.stats[bad_name].wbad
                        self.badrates[bad_name].missing += cbin.stats[bad_name].wtotal()
                    else :
                        self.badrates[bad_name].nmbad += cbin.stats[bad_name].wbad
                        self.badrates[bad_name].nonmiss += cbin.stats[bad_name].wtotal()
                    self.badrates[bad_name].bad += cbin.stats[bad_name].wbad
                    self.badrates[bad_name].total += cbin.stats[bad_name].wtotal()
                self.badrates[bad_name].total = self.badrates[bad_name].bad / (self.badrates[bad_name].total + 1e-36)
                self.badrates[bad_name].missing = self.badrates[bad_name].mbad / (self.badrates[bad_name].missing + 1e-36)
                self.badrates[bad_name].nonmiss = self.badrates[bad_name].nmbad / (self.badrates[bad_name].nonmiss + 1e-36)
        else :
            nfloat = ignore_exception(ValueError, None)(float)
            lefts = {}
            for bad_name in sums :
                lefts[bad_name] = sums[bad_name].copy()
                bn = bad_name
            for dist in self.distincts :
                self.distincts[dist].value = nfloat(dist)
            sorted_bins = sorted(self.distincts.values(), key=lambda b: b.value)
            mcut = {}
            gcut = {}
            bcut = {}
            cums = []
            cums_cut = [0.01, 0.05, 0.1, 0.5, 0.9, 0.95, 0.99]
            cum_ind = 0
            cum_total = 0.0
            nonmiss_cnt = 0.0
            nonmiss_total = 0.0
            nonmiss_sqr = 0.0
            for bad_name in sums :
                if mess <= 0 and pct <= 0 :
                    mess = 0.02
                    pct = 0
                mcut[bad_name] = mess * sums[bad_name].wtotal() if 0 <= mess < 1 else mess
                gcut[bad_name] = pct * sums[bad_name].wgood if 0 <= pct < 1 else pct
                bcut[bad_name] = pct * sums[bad_name].wbad if 0 <= pct < 1 else pct
                if gcut[bad_name] > sums[bad_name].wgood :
                    gcut[bad_name] = 0.02 * sums[bad_name].wgood
                if bcut[bad_name] > sums[bad_name].wbad :
                    bcut[bad_name] = 0.02 * sums[bad_name].wbad
                if mcut[bad_name] > sums[bad_name].wtotal() :
                    mcut[bad_name] = 0.02 * sums[bad_name].wtotal()
            zbin = {}
            for bad_name in sums :
                zbin[bad_name] = UniBin(None, [bad_name])
                self.badrates[bad_name] = BadRate()
            for cbin in sorted_bins :
                for bad_name in sums :
                    if cbin.value is not None and zbin[bad_name].value is None :
                        self.bins[bad_name].append(zbin[bad_name])
                        zbin[bad_name] = UniBin(cbin.value, [bad_name])
                        if nonmiss_cnt == 0 :
                            nonmiss_cnt += lefts[bn].wtotal()
                    elif (  zbin[bad_name].value is not None
                            and (zbin[bad_name].stats[bad_name].wbad >= bcut[bad_name]
                            or zbin[bad_name].stats[bad_name].wgood >= gcut[bad_name])
                            and zbin[bad_name].stats[bad_name].wtotal() >= mcut[bad_name]
                            and zbin[bad_name].stats[bad_name].wtotal() > 0
                            and lefts[bad_name].wtotal() >= mcut[bad_name]
                            and lefts[bad_name].wbad >= bcut[bad_name]
                            and lefts[bad_name].wgood >= gcut[bad_name]) :
                        self.bins[bad_name].append(zbin[bad_name])
                        zbin[bad_name] = UniBin(cbin.value, [bad_name])
                    zbin[bad_name].mergeStats(cbin)
                    if cbin.value is not None :
                        zbin[bad_name].mergeValue(cbin)
                        self.badrates[bad_name].nmbad += cbin.stats[bad_name].wbad
                        self.badrates[bad_name].nonmiss += cbin.stats[bad_name].wtotal()
                    else :
                        self.badrates[bad_name].mbad += cbin.stats[bad_name].wbad
                        self.badrates[bad_name].missing += cbin.stats[bad_name].wtotal()
                    self.badrates[bad_name].bad += cbin.stats[bad_name].wbad
                    self.badrates[bad_name].total += cbin.stats[bad_name].wtotal()
                    lefts[bad_name].minus(cbin.stats.get(bad_name, Stats()))
                if cbin.value is not None :
                    cum_total += cbin.stats[bn].wtotal()
                    nonmiss_total += cbin.stats[bn].wtotal() * cbin.value
                    nonmiss_sqr += cbin.stats[bn].wtotal() * cbin.value * cbin.value
                    while cum_ind < len(cums_cut) and cum_total >= cums_cut[cum_ind] * nonmiss_cnt :
                        cums.append(cbin.value)
                        cum_ind += 1
            for bad_name in sums :
                self.bins[bad_name].append(zbin[bad_name])
                self.badrates[bad_name].total = self.badrates[bad_name].bad / (self.badrates[bad_name].total + 1e-36)
                self.badrates[bad_name].missing = self.badrates[bad_name].mbad / (self.badrates[bad_name].missing + 1e-36)
                self.badrates[bad_name].nonmiss = self.badrates[bad_name].nmbad / (self.badrates[bad_name].nonmiss + 1e-36)
            if cum_ind > 0 :
                self.distribution.pct_1 = cums[0]
                self.distribution.pct_5 = cums[1]
                self.distribution.pct_10 = cums[2]
                self.distribution.median = cums[3]
                self.distribution.pct_90 = cums[4]
                self.distribution.pct_95 = cums[5]
                self.distribution.pct_99 = cums[6]
                if math.isinf(nonmiss_sqr) or math.isinf(nonmiss_total) or math.isinf(nonmiss_total ** 2) :
                    logging.info("found infinity when z-scaling variable : {}, using 5th and 95th percentile to cap".format(self.name))
                    if math.isinf(self.cap_bin(mess, pct, sums, (self.distribution.pct_5, self.distribution.pct_95))) :
                        self.cap_bin(mess, pct, sums, (-1e50, 1e50))
                else :
                    self.distribution.mean = nonmiss_total / (nonmiss_cnt + 1e-36)
                    self.distribution.std = math.sqrt(abs((nonmiss_sqr - 2 * nonmiss_total * self.distribution.mean + nonmiss_cnt * self.distribution.mean ** 2) / (nonmiss_cnt - 1 + 1e-36)))
        self.distincts = None

    def cap_bin(self, mess, pct, sums, cap):
        nfloat = ignore_exception(ValueError, None)(float)
        lefts = {}
        for bad_name in sums :
            lefts[bad_name] = sums[bad_name].copy()
            bn = bad_name
        old_distincts = self.distincts
        self.distincts = {}
        for dist in old_distincts :
            value = nfloat(dist)
            if value is None :
                self.distincts[dist] = old_distincts[dist]
                self.distincts[dist].value = value
                value_str = dist
            else :
                if value > cap[1] :
                    cvalue = cap[1]
                elif value < cap[0] :
                    cvalue = cap[0]
                else :
                    cvalue = value
                if cvalue == value :
                    value_str = dist
                else :
                    evalue = "{0:.6e}".format(cvalue)
                    numpart = float(evalue[:evalue.find('e')])
                    numpart = math.ceil(numpart * 1000) * 0.001
                    value_str = "{0:.3f}".format(numpart) + evalue[evalue.find('e'):]
                if value_str not in self.distincts :
                    self.distincts[value_str] = UniBin(value, sums.keys())
            #self.distincts[value_str].value = value
            self.distincts[value_str].mergeStats(old_distincts[dist])
        self.bins = defaultdict(list)
        sorted_bins = sorted(self.distincts.values(), key=lambda b: b.value)
        mcut = {}
        gcut = {}
        bcut = {}
        cum_total = 0.0
        nonmiss_cnt = 0.0
        nonmiss_total = 0.0
        nonmiss_sqr = 0.0
        for bad_name in sums :
            if mess <= 0 and pct <= 0 :
                mess = 0.02
                pct = 0
            mcut[bad_name] = mess * sums[bad_name].wtotal() if 0 <= mess < 1 else mess
            gcut[bad_name] = pct * sums[bad_name].wgood if 0 <= pct < 1 else pct
            bcut[bad_name] = pct * sums[bad_name].wbad if 0 <= pct < 1 else pct
            if gcut[bad_name] > sums[bad_name].wgood :
                gcut[bad_name] = 0.02 * sums[bad_name].wgood
            if bcut[bad_name] > sums[bad_name].wbad :
                bcut[bad_name] = 0.02 * sums[bad_name].wbad
            if mcut[bad_name] > sums[bad_name].wtotal() :
                mcut[bad_name] = 0.02 * sums[bad_name].wtotal()
        zbin = {}
        for bad_name in sums :
            zbin[bad_name] = UniBin(None, [bad_name])
            self.badrates[bad_name] = BadRate()
        for cbin in sorted_bins :
            for bad_name in sums :
                if cbin.value is not None and zbin[bad_name].value is None :
                    self.bins[bad_name].append(zbin[bad_name])
                    zbin[bad_name] = UniBin(cbin.value, [bad_name])
                    if nonmiss_cnt == 0 :
                        nonmiss_cnt += lefts[bn].wtotal()
                elif (  zbin[bad_name].value is not None
                        and (zbin[bad_name].stats[bad_name].wbad >= bcut[bad_name]
                        or zbin[bad_name].stats[bad_name].wgood >= gcut[bad_name])
                        and zbin[bad_name].stats[bad_name].wtotal() >= mcut[bad_name]
                        and zbin[bad_name].stats[bad_name].wtotal() > 0
                        and lefts[bad_name].wtotal() >= mcut[bad_name]
                        and lefts[bad_name].wbad >= bcut[bad_name]
                        and lefts[bad_name].wgood >= gcut[bad_name]) :
                    self.bins[bad_name].append(zbin[bad_name])
                    zbin[bad_name] = UniBin(cbin.value, [bad_name])
                zbin[bad_name].mergeStats(cbin)
                if cbin.value is not None :
                    zbin[bad_name].mergeValue(cbin)
                    self.badrates[bad_name].nmbad += cbin.stats[bad_name].wbad
                    self.badrates[bad_name].nonmiss += cbin.stats[bad_name].wtotal()
                else :
                    self.badrates[bad_name].mbad += cbin.stats[bad_name].wbad
                    self.badrates[bad_name].missing += cbin.stats[bad_name].wtotal()
                self.badrates[bad_name].bad += cbin.stats[bad_name].wbad
                self.badrates[bad_name].total += cbin.stats[bad_name].wtotal()
                lefts[bad_name].minus(cbin.stats.get(bad_name, Stats()))
            if cbin.value is not None :
                cum_total += cbin.stats[bn].wtotal()
                nonmiss_total += cbin.stats[bn].wtotal() * cbin.value
                nonmiss_sqr += cbin.stats[bn].wtotal() * cbin.value * cbin.value
        for bad_name in sums :
            self.bins[bad_name].append(zbin[bad_name])
            self.badrates[bad_name].total = self.badrates[bad_name].bad / (self.badrates[bad_name].total + 1e-36)
            self.badrates[bad_name].missing = self.badrates[bad_name].mbad / (self.badrates[bad_name].missing + 1e-36)
            self.badrates[bad_name].nonmiss = self.badrates[bad_name].nmbad / (self.badrates[bad_name].nonmiss + 1e-36)
        if math.isinf(nonmiss_sqr) or math.isinf(nonmiss_total) or math.isinf(nonmiss_total ** 2) :
            return float('inf')
        else :
            self.distribution.mean = nonmiss_total / (nonmiss_cnt + 1e-36)
            self.distribution.std = math.sqrt(abs((nonmiss_sqr - 2 * nonmiss_total * self.distribution.mean + nonmiss_cnt * self.distribution.mean ** 2) / (nonmiss_cnt - 1 + 1e-36)))
            return 0

    def newdataset(self):
        empty_bins = {}
        for bad_name in self.bins :
            empty_bins[bad_name] = []
            for vbin in self.bins[bad_name] :
                empty_bins[bad_name].append(UniBin(vbin.value, [bad_name]))
        self.val_datasets.append(empty_bins)

    def toString(self, names=None, dlm=","):
        if names is None :
            names = list(self.bins.keys())
        line = dlm.join(("====", self.name, self.type, self.distribution.toString(self.type, dlm))) + "\n"
        for name in names :
            line += dlm.join(("----", name, self.badrates[name].toString(dlm))) + "\n"
            line += dlm.join(("data", "total", "good", "bad", "weighted total", "weighted good", "weighted bad", "high/value")) + "\n"
            data_ind = 0
            for ind in xrange(len(self.bins[name])) :
                line += dlm.join((str(data_ind), self.bins[name][ind].stats[name].toString(dlm),
                                  str(self.bins[name][ind].value))) + "\n"
            for dind in xrange(len(self.val_datasets)) :
                data_ind = dind + 1
                for ind in xrange(len(self.val_datasets[dind][name])) :
                    line += dlm.join((str(data_ind), self.val_datasets[dind][name][ind].stats[name].toString(dlm),
                                      str(self.val_datasets[dind][name][ind].value))) + "\n"
        return line


def parseType(options) :
    # this special type should contain "num, char, drop, keep" 4 types
    # the logic of keep / drop is : if keep, the drop all others, otherwise, drop listed.
    special = {}
    ikeeps = set()
    idrops = set()
    if os.path.exists(options.type) :
        lines = open(options.type).readlines()
        for line in lines:
            parts = [x.lower().strip() for x in line.split(',')]
            if "num" in parts or 'n' in parts:
                special[parts[0]] = "num"
            elif "char" in parts or 'c' in parts:
                special[parts[0]] = "char"
            if "drop" in parts or 'd' in parts:
                idrops.add(parts[0])
            if "keep" in parts or 'k' in parts:
                ikeeps.add(parts[0])
    else :
        try :
            variables = json.loads(options.type.lower())
            for name in variables :
                if 'num' in variables[name] or 'n' in variables[name] :
                    special[name] = 'num'
                elif 'char' in variables[name] or 'c' in variables[name] :
                    special[name] = 'char'
                if 'drop' in variables[name] or 'd' in variables[name] :
                    idrops.add(name)
                if 'keep' in variables[name] or 'k' in variables[name] :
                    ikeeps.add(name)
        except ValueError :
            pass
    if len(ikeeps) > 0 :
        idrops = set()
    return special, ikeeps, idrops


def parseKeys(csv, options, hind=0) :
    # return bads, wgt, vnames
    # list of bad taggings
    # var of weight
    # names of all variables
    with open(csv, "r") as fn:
        head = fn.readline().replace(':', '_')
        vnames = [x.strip() for x in head.split(options.dlm)]
    if options.head :
        head_files = options.head.split(',')
        if len(head_files) == 1:
            with open(head_files[0], "r") as fn :
                head = fn.readline().replace(':', '_')
                vnames = [x.strip() for x in head.split(",")]
        else :
            with open(head_files[hind], "r") as fn :
                head = fn.readline().replace(':', '_')
                vnames = [x.strip() for x in head.split(",")]
    heads = [x.lower() for x in vnames]
    sbads = set([x.lower().strip() for x in options.bad.split(",")])
    if options.wgt :
        swgt = [x.lower().strip() for x in options.wgt.split(",")][0]
    else :
        swgt = ""
    bads = set()
    wgt = -1
    length = len(heads)
    for ind in xrange(length) :
        if heads[ind] in sbads :
            bads.add(ind)
        elif heads[ind] == swgt :
            wgt = ind
    sint = ignore_exception(ValueError, -1)(int)
    if len(bads) != len(sbads) :
        for bad in sbads :
            ibad = sint(bad)
            if 0 <= ibad < length :
                bads.add(ibad)
    if len(bads) != len(sbads) :
        logging.error("parsing bad variables error!")
        return None, None, None
    if wgt < 0 and swgt != "" :
        iwgt = sint(swgt)
        if 0 <= iwgt < length :
            wgt = iwgt
        else :
            logging.error("parsing weight variable error!")
            return None, None, None
    if wgt in bads :
        logging.error("key variables duplication!")
        return None, None, None
    return bads, wgt, vnames


def shortVarStr(svalue, nfloat) :
    fvalue = nfloat(svalue)
    if fvalue is None or fvalue == float('inf') :
        return svalue
    try :
        evalue = "{0:.6e}".format(fvalue)
        numpart = float(evalue[:evalue.find('e')])
        numpart = math.ceil(numpart * 1000) * 0.001
    except ValueError as e :
        return 'None'
    return "{0:.3f}".format(numpart) + evalue[evalue.find('e'):]


def distinctSortVariables(options, bads, wgt, vars, special_type, header_count) :
    if len(vars) == 0 :
        return []
    bad_float = ignore_exception(ValueError, 2)(float)
    type_float = ignore_exception(ValueError, None)(float)
    wgt_float = ignore_exception(ValueError, 1)(float)
    known_error_value = {'', 'null', 'none', 'missing', '.', '_missing_'}
    input_vars = {}
    binned_vars = {}
    discard_vars = set()
    bad_names = [bads[ind] for ind in bads]
    total_sum = UniBin(bads=bad_names)
    for var_ind in vars :
        input_vars[var_ind] = Variable(vars[var_ind])
        if vars[var_ind].lower() in special_type :
            input_vars[var_ind].special = True
            input_vars[var_ind].type = special_type[vars[var_ind].lower()]

    logging.info("start processing variables : {0}".format(", ".join(vars.values())))
    with open(options.src, "r") as fn :
        line = fn.readline()
        lnum = 0
        while True :
            line = fn.readline()
            if (not line and lnum < 10000) or lnum == 10000 :
                #logging.info("start correcting variable type...")
                for ind in input_vars :
                    dist_num = len(input_vars[ind].distincts)
                    if input_vars[ind].special :
                        logging.debug("variable {0} is assigned as {1}".format(input_vars[ind].name, input_vars[ind].type))
                    else :
                        parse_error_num = 0
                        known_error_num = 0
                        for dist in input_vars[ind].distincts :
                            if type_float(dist) is None :
                                parse_error_num += 1
                            if dist.lower() in known_error_value :
                                known_error_num += 1
                        if parse_error_num == known_error_num :
                            input_vars[ind].type = "num"
                            logging.debug("variable {0} is detected as numeric variable".format(input_vars[ind].name))
                        else :
                            logging.debug("variable {0} is detected as character variable".format(input_vars[ind].name))
                    if input_vars[ind].type == "char" and dist_num > 500 :
                        discard_vars.add(ind)
                        logging.info("variable {0} is DELETED due to too many distinct value".format(input_vars[ind].name))
                    elif dist_num > 0.95 * lnum :
                        logging.info("variable {0} is possibly an ID due to too many distinct value".format(input_vars[ind].name))
            if not line :
                break
            if line.strip() == '' :
                continue
            parts = [xi.strip() for xi in line.split(options.dlm)]
            if len(parts) != header_count :
                continue
            if len(parts) < max(vars.keys()) :
                logging.error("column mismatch when parsing develop dataset!")
                return {}
            weight = 1 if wgt < 0 else wgt_float(parts[wgt])
            bad_values = {}
            for bad_ind in bads :
                bad_value = bad_float(parts[bad_ind])
                bad_values[bads[bad_ind]] = int(math.floor(bad_value + 0.5)) if 0 <= bad_value <= 1 else 2
                if bad_values[bads[bad_ind]] == 0 :
                    total_sum.stats[bads[bad_ind]].good += 1
                    total_sum.stats[bads[bad_ind]].wgood += weight
                elif bad_values[bads[bad_ind]] == 1 :
                    total_sum.stats[bads[bad_ind]].bad += 1
                    total_sum.stats[bads[bad_ind]].wbad += weight
                else :
                    total_sum.stats[bads[bad_ind]].ind += 1
                    total_sum.stats[bads[bad_ind]].wind += weight
            for ind in input_vars :
                if ind in discard_vars :
                    continue
                varstr = shortVarStr(parts[ind], type_float)
                if varstr not in input_vars[ind].distincts :
                    input_vars[ind].distincts[varstr] = UniBin(varstr, bad_names)
                for bad_name in bad_values :
                    if bad_values[bad_name] == 0 :
                        input_vars[ind].distincts[varstr].stats[bad_name].good += 1
                        input_vars[ind].distincts[varstr].stats[bad_name].wgood += weight
                    elif bad_values[bad_name] == 1 :
                        input_vars[ind].distincts[varstr].stats[bad_name].bad += 1
                        input_vars[ind].distincts[varstr].stats[bad_name].wbad += weight
                    else :
                        input_vars[ind].distincts[varstr].stats[bad_name].ind += 1
                        input_vars[ind].distincts[varstr].stats[bad_name].wind += weight
            lnum += 1
    for ind in input_vars :
        if ind in discard_vars :
            continue
        logging.debug("start fine bin for variable: {}".format(input_vars[ind].name))
        input_vars[ind].bin(options.mess, options.pct, total_sum.stats)
        binned_vars[input_vars[ind].name.lower()] = input_vars[ind]
    return binned_vars


def findBin(value, bins, binType) :
    if binType == "char" :
        for bin_ind in xrange(len(bins)) :
            if bins[bin_ind].value == value :
                return bin_ind
        return None
    elif binType == "num" :
        if value is None or len(bins) == 1:
            return 0
        lb = 1
        ub = len(bins)
        while lb + 1 < ub :
            mid = int(math.floor((lb + ub) / 2))
            if bins[mid - 1].value < value <= bins[mid].value :
                return mid
            elif bins[mid].value < value :
                lb = mid
            elif value <= bins[mid - 1].value :
                ub = mid
        return lb


def applyBoundaries(val, options, variables, header_count, hind) :
    val_bads, val_wgt, val_names = parseKeys(val, options, hind)
    if val_bads is None :
        return
    val_bad_names = {}
    new_char_bins = {}
    for val_bad in val_bads :
        val_bad_names[val_bad] = val_names[val_bad]
        new_char_bins[val_names[val_bad]] = set()
    bad_float = ignore_exception(ValueError, 2)(float)
    nfloat = ignore_exception(ValueError, None)(float)
    wfloat = ignore_exception(ValueError, 1)(float)
    with open(val, "r") as fn :
        line = fn.readline()
        while True :
            line = fn.readline()
            if not line :
                break
            if line.strip() == '' :
                continue
            parts = [xi.strip() for xi in line.split(options.dlm)]
            #if len(parts) != header_count :
            #    continue
            if len(parts) < len(val_names) :
                #logging.error("column mismatch when parsing validation dataset!")
                #break
                continue
            weight = 1 if val_wgt < 0 else wfloat(parts[val_wgt])
            bad_values = {}
            for bad_ind in val_bads :
                bad_value = bad_float(parts[bad_ind])
                bad_values[val_bad_names[bad_ind]] = int(math.floor(bad_value + 0.5)) if 0 <= bad_value <= 1 else 2
            for ind in xrange(len(val_names)) :
                val_name = val_names[ind].lower()
                discard = False
                if val_name not in variables :
                    continue
                if variables[val_name].type == "char" :
                    fvalue = parts[ind]
                else :
                    fvalue = nfloat(parts[ind])
                for bad_name in bad_values :
                    bin_num = findBin(fvalue, variables[val_name].val_datasets[-1][bad_name], variables[val_name].type)
                    if variables[val_name].type == "char" and bin_num is None :
                        bin_num = -1
                        if fvalue not in new_char_bins[bad_name] :
                            new_char_bins[bad_name].add(fvalue)
                            variables[val_name].bins[bad_name].append(UniBin(fvalue, [bad_name]))
                            variables[val_name].val_datasets[-1][bad_name].append(UniBin(fvalue, [bad_name]))
                        if len(variables[val_name].bins[bad_name]) > 500 :
                            discard = True
                            logging.info("variable {0} is DELETED due to too many distinct value".format(variables[val_name].name))
                            break
                    if bad_values[bad_name] == 0 :
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].good += 1
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].wgood += weight
                    elif bad_values[bad_name] == 1 :
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].bad += 1
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].wbad += weight
                    else :
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].ind += 1
                        variables[val_name].val_datasets[-1][bad_name][bin_num].stats[bad_name].wind += weight
                    if variables[val_name].type == "num" and fvalue > variables[val_name].val_datasets[-1][bad_name][-1].value :
                        variables[val_name].val_datasets[-1][bad_name][-1].value = fvalue
                if discard :
                    variables.pop(val_name)


def checkHeader(options) :
    with open(options.src) as fn:
        head = fn.readline()
        headFields = head.split(options.dlm)
        value_count = set()
        for l in range(10) :
            value = fn.readline()
            if not value :
                break
            valueFields = value.split(options.dlm)
            value_count.add(len(valueFields))
    if options.head :
        with open(options.head) as fn :
            head = fn.readline()
            headFields = head.split(',')
    if len(headFields) not in value_count:
        logging.error('mismatch has been found on header line and following lines')
        return 0, False
    return len(headFields), True


def findPatterns(base, patterns) :
    selected = set()
    for pattern in patterns :
        selected = selected.union(fnmatch.filter(base, pattern))
    return selected


if __name__ == '__main__' :
    parser = OptionParser()
    parser.add_option("-s", "--src", dest="src", help="develop data file", action="store", type="string")
    parser.add_option("-v", "--val", dest="val", help="validate data file, if many, use ','", action="store", type="string")
    parser.add_option("-d", "--dlm", dest="dlm", help="delimiter char, accept xASCII format, default=,", action="store", type="string", default=",")
    parser.add_option("-b", "--bad", dest="bad", help="index or names of bad variables", action="store", type="string")
    parser.add_option("-w", "--wgt", dest="wgt", help="index or names of weight, optional", action="store", type="string")
    parser.add_option("-e", "--head", dest="head", help="optional head line file, must use ',' as delimiter", action="store", type="string")
    parser.add_option("-t", "--type", dest="type", help="optional variable type file / json string in format var_name[,k[eep]|d[rop]][,n[um]|c[har]] on each line", action="store", type="string")
    parser.add_option("-x", "--excl", dest="exclude", help="optional variable list for simple exclusion of binning, wildchar supported, separate with ','", action="store", type="string")
    parser.add_option("-n", "--num", dest="num", help="number of variables per batch, default is 100 vars at once", action="store", type="int", default=100)
    parser.add_option("-m", "--mess", dest="mess", help="minimum mess block, default=2% of total population", action="store", type="float", default=0.02)
    parser.add_option("-p", "--pct", dest="pct", help="minimum target block, default=2% of all bad or all good", action="store", type="float", default=0.02)
    parser.add_option("-o", "--out", dest="out", help="output fine bin file", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file, if not given, stdout is used", action="store", type="string")
    (options, args) = parser.parse_args()

    if not options.src :
        print "You must specify the input .csv file!"
        exit()

    if not options.bad :
        print "You must specify the bad variables!"
        exit()

    if options.dlm.startswith('x') :
        options.dlm = chr(int(options.dlm[1:]))

    if options.log :
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else :
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    logging.info("program started...")
    header_count, same_count = checkHeader(options)
    if not same_count :
        print "header mismatch found in the dataset!"
        exit()
    logging.info("{} variables from header, records with different values will be discarded".format(header_count))

    if options.head and options.val :
        if len(options.head.split(',')) != 1 and len(options.head.split(',')) != len(options.val.split(',')) + 1 :
            print "header number not match dataset number!"
            exit()

    bads, wgt, vnames = parseKeys(options.src, options)
    if not bads :
        print "bad variables parsing error!"
        exit()
    logging.info("key variables parsed...")

    if options.type :
        special_type, keeps, drops = parseType(options)
    else :
        special_type = {}
        keeps = set()
        drops = set()
    logging.info("special variable type parsed...")

    if options.exclude :
        exclude_patterns = options.exclude.lower().split(',')
        exclude_base = [x.lower() for x in vnames]
        excludes = set(findPatterns(exclude_base, exclude_patterns))
        keeps = keeps.difference(excludes)
        drops = drops.union(excludes)

    logging.info("start generating distinct values...")
    name_batch = {}
    variables = {}
    bad_names = {}
    for bad in bads :
        bad_names[bad] = vnames[bad]
    for vind in xrange(len(vnames)) :
        if len(keeps) > 0 :
            if vnames[vind].lower() in keeps :
                name_batch[vind] = vnames[vind]
        else :
            if vnames[vind].lower() not in drops and vind not in bads and vind != wgt :
                name_batch[vind] = vnames[vind]
        if 0 < options.num <= len(name_batch) :
            # TODO: use multiprocessing to accelerate this process
            raw = distinctSortVariables(options, bad_names, wgt, name_batch, special_type, header_count)
            for variable in raw :
                variables[variable] = raw[variable]
            name_batch = {}
    raw = distinctSortVariables(options, bad_names, wgt, name_batch, special_type, header_count)
    for variable in raw :
        variables[variable] = raw[variable]

    if options.val :
        logging.info("start applying fine bin boundaries to other datasets...")
        vals = [x.strip() for x in options.val.split(",")]
        for dind in xrange(len(vals)) :
            for variable in variables :
                variables[variable].newdataset()
            # TODO : combine this with the last on the batch level
            applyBoundaries(vals[dind], options, variables, header_count, dind+1)

    logging.info("start outputing the binning results...")
    bad_names = list(bad_names.values())
    with open(options.out, "w") as fn :
        for variable in variables :
            fn.write(variables[variable].toString(bad_names))
            fn.flush()

    logging.info("fine binning job done!")

