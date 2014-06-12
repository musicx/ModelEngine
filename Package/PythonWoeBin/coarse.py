from collections import defaultdict
import logging
from optparse import OptionParser
import os
from math import log, sqrt
import time

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


class BinWindow:
    __eps = 1e-30

    def __init__(self, isnumeric) :
        # given by user
        self.isnumeric = isnumeric
        # read from file
        self.rank = 0
        if not isnumeric :
            self.rank = 1
        self.value = ""
        self.total = 0
        self.bad = 0
        self.good = 0
        self.rawTotal = 0
        self.rawBad = 0
        self.rawGood = 0
        self.ks = 0.0
        # calculated in variable scale
        self.allTotal = 0
        self.allBad = 0
        self.allGood = 0
        # calculated by update
        self.odds = 0.0
        self.badRate = 0.0
        self.woe = 0.0
        self.iv = 0.0
        self.weight = 0.0
        self.pBad = 0.0
        self.pGood = 0.0

    def UpdateTotal(self, totals) :
        # totals should be in the [total, good, bad] format
        self.allTotal = totals[0]
        self.allGood = totals[1]
        self.allBad = totals[2]
        self.Update()

    def Update(self) :
        self.pBad = self.bad / (self.allBad + self.__eps)
        self.pGood = self.good / (self.allGood + self.__eps)
        self.odds = self.pBad / (self.pGood + self.__eps)
        self.badRate = self.bad / (self.total + self.__eps)
        self.woe = log((self.pBad + self.__eps) / (self.pGood + self.__eps))
        self.iv = self.woe * (self.pBad - self.pGood)
        self.weight = self.woe * (self.pBad + self.pGood)

    def Clone(self, window) :
        if self.isnumeric != window.isnumeric :
            raise ValueError("is numeric error!")
        self.rank = window.rank
        self.value = window.value
        self.total = window.total
        self.bad = window.bad
        self.good = window.good
        self.rawBad = window.rawBad
        self.rawGood = window.rawGood
        self.rawTotal = window.rawTotal
        self.ks = window.ks
        self.allTotal = window.allTotal
        self.allBad = window.allBad
        self.allGood = window.allGood
        self.odds = window.odds
        self.badRate = window.badRate
        self.woe = window.woe
        self.iv = window.iv
        self.weight = window.weight
        self.pBad = window.pBad
        self.pGood = window.pGood

    def Merge(self, windows) :
        for window in windows :
            self.bad += window.bad
            self.good += window.good
            self.total += window.total
            self.rawBad += window.rawBad
            self.rawGood += window.rawGood
            self.rawTotal += window.rawTotal
            if self.allBad < window.allBad :
                self.allBad = window.allBad
            if self.allGood < window.allGood :
                self.allGood = window.allGood
            if self.allTotal < window.allTotal :
                self.allTotal = window.allTotal
            if self.ks < window.ks :
                self.ks = window.ks
        self.Update()


class Bin :
    __eps = 1e-30

    def __init__(self, isnumeric, nwindows) :
        self.isnumeric = isnumeric
        self.windows = []
        self.rank = 0
        if not isnumeric :
            self.rank = 1
        self.value = ""
        for i in xrange(nwindows) :
            window = BinWindow(isnumeric)
            self.windows.append(window)

    def Update(self) :
        lastValue = None  # TODO: check this is right
        init = True
        for window in self.windows :
            self.value = window.value
            if init :
                init = False
            else :
                if self.value is not None and self.value != lastValue:
                    if self.isnumeric :
                        self.value = float('inf')
                    else :
                        raise "Bin Value Mismatch!"
            lastValue = window.value

    def CombineWoe(self) :
        bad = sum([window.bad for window in self.windows])
        good = sum([window.good for window in self.windows])
        allBad = sum([window.allBad for window in self.windows])
        allGood = sum([window.allGood for window in self.windows])
        return log((bad / (allBad + self.__eps) + self.__eps) / (good / (allGood + self.__eps) + self.__eps))

    @staticmethod
    def MergeBins(bins) :
        if not bins or len(bins) == 0 :
            return None
        isnumeric = bins[0].isnumeric
        nwindows = len(bins[0].windows)
        mbin = Bin(isnumeric, nwindows)
        if mbin.isnumeric :
            maxrank = max(bins, key=lambda x : x.rank)
            mbin.rank = maxrank.rank
            mbin.value = maxrank.value
        else :
            mbin.value = ",".join([bin.value for bin in bins])
        for i in xrange(nwindows) :
            mbin.windows[i].rank = mbin.rank
            mbin.windows[i].value = mbin.value
            mbin.windows[i].Merge([bin.windows[i] for bin in bins])
        return mbin

    @staticmethod
    def Clone(obin) :
        nbin = Bin(obin.isnumeric, len(obin.windows))
        nbin.rank = obin.rank
        nbin.value = obin.value
        for i in xrange(len(obin.windows)) :
            nbin.windows[i].Clone(obin.windows[i])
        return nbin


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

    def toString(self, isnum, dlm=","):
        if isnum :
            return dlm.join(["{0}".format(x) for x in [self.mean, self.std,
                                                       self.pct_1, self.pct_5, self.pct_10,
                                                       self.median,
                                                       self.pct_90, self.pct_95, self.pct_99]])
        else :
            return self.mode


class BadRate :
    def __init__(self):
        self.total = 0.0
        self.missing = 0.0
        self.nonmiss = 0.0

    def toString(self, dlm=","):
        return dlm.join(["{0:.4e}".format(x) for x in [self.total, self.missing, self.nonmiss]])


class Variable:
    __eps = 1e-36

    def __init__(self, data, options) :
        self.name = ""
        self.woeName = ""
        self.isnumeric = False
        self.original = defaultdict(list)
        self.solution = defaultdict(list)
        self.numWindow = 0
        self.labels = {}
        self.badrates = {}
        self.distribution = Distribution()
        self.__parse(data, options)
        self.psi = {}
        self.iv = {}
        self.missing = []
        self.missingRate = []
        self.CheckMissing()

    def __parse(self, data, options) :
        binDict = {}
        totalDict = {}
        nfloat = ignore_exception(ValueError)(float)

        badParts = data.split('----')
        stats = badParts[0].strip().split(',')
        self.name = stats[1].strip()
        self.woeName = self.name + options.suf
        self.isnumeric = True if stats[2] == 'num' else False
        if self.isnumeric :
            self.distribution.mean = float(stats[3])
            self.distribution.std = float(stats[4])
            self.distribution.pct_1 = float(stats[5])
            self.distribution.pct_5 = float(stats[6])
            self.distribution.pct_10 = float(stats[7])
            self.distribution.median = float(stats[8])
            self.distribution.pct_90 = float(stats[9])
            self.distribution.pct_95 = float(stats[10])
            self.distribution.pct_99 = float(stats[11])
        else :
            self.distribution.mode = stats[3]

        valueSet = {}
        valueId = 0
        for badPart in badParts[1:] :
            lines = [x for x in badPart.split('\n') if x != ""]
            badStats = lines[0].strip().split(',')
            self.badrates[badStats[1]] = BadRate()
            self.badrates[badStats[1]].total = float(badStats[2])
            self.badrates[badStats[1]].missing = float(badStats[3])
            self.badrates[badStats[1]].nonmiss = float(badStats[4])

            for line in lines[2:] :
                if not line :
                    continue
                parts = line.split(',')
                window = BinWindow(self.isnumeric)
                winLabel = int(float(parts[0]))
                window.rawTotal = float(parts[1])
                window.rawGood = float(parts[2])
                window.rawBad = float(parts[3])
                window.total = float(parts[4])
                window.good = float(parts[5])
                window.bad = float(parts[6])
                window.value = nfloat(parts[7]) if self.isnumeric else parts[7].strip()
                if not self.isnumeric and window.value not in valueSet :
                    valueSet[window.value] = valueId
                    valueId += 1
                if winLabel not in binDict :
                    binDict[winLabel] = []
                    totalDict[winLabel] = [0, 0, 0]
                binDict[winLabel].append(window)
                totalDict[winLabel][0] += window.total
                totalDict[winLabel][1] += window.good
                totalDict[winLabel][2] += window.bad
            self.numWindow = len(binDict)
            if self.isnumeric :
                for rank in xrange(len(binDict[0])) :
                    cbin = Bin(self.isnumeric, len(binDict))
                    cbin.rank = rank
                    for winLabel in xrange(len(binDict)) :
                        cbin.windows[winLabel] = binDict[winLabel][rank]
                        cbin.windows[winLabel].rank = rank
                        cbin.windows[winLabel].UpdateTotal(totalDict[winLabel])
                    self.original[badStats[1]].append(cbin)
            else :
                for winLabel in xrange(len(binDict)) :
                    for rank in xrange(len(binDict[winLabel])) :
                        valueId = valueSet[binDict[winLabel][rank].value]
                        if winLabel == 0 :
                            cbin = Bin(self.isnumeric, len(binDict))
                            cbin.rank = valueId
                            self.original[badStats[1]].append(cbin)
                        else :
                            cbin = self.original[badStats[1]][valueId]
                        cbin.windows[winLabel] = binDict[winLabel][rank]
                        cbin.windows[winLabel].rank = valueId
                        cbin.windows[winLabel].UpdateTotal(totalDict[winLabel])
            for cbin in self.original[badStats[1]] :
                cbin.Update()

    def CheckMissing(self) :
        for i in xrange(self.numWindow) :
            self.missingRate.append(0.0)
            self.missing.append(0)

        bn = list(self.original.keys())
        for bin in self.original[bn[0]] :
            if self.isnumeric :  # TODO : check if the logic is alright
                #if len(bin.value) == 1 and not bin.value.isdigit() :
                if bin.value is None :
                    #bin.rank = 0
                    for i in xrange(self.numWindow) :
                        #bin.windows[i].rank = 0
                        self.missing[i] += bin.windows[i].total
                        self.missingRate[i] = self.missing[i] / (bin.windows[i].allTotal + self.__eps)
            else :
                if bin.value.lower() in {"", "_missing_", "missing"} :
                    #bin.rank = 0
                    for i in xrange(self.numWindow) :
                        #bin.windows[i].rank = 0
                        self.missing[i] = bin.windows[i].total
                        self.missingRate[i] = self.missing[i] / (bin.windows[i].allTotal + self.__eps)
                    break

    def PSI(self, badname) :
        psi = [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
        if len(self.solution[badname]) < 2 or len(self.solution[badname][0].windows) < 2 :
            return psi
        for bin in self.solution[badname] :
            if bin.windows[0].total == 0  :
                psi[0][2] += bin.windows[1].total * 1.0 / bin.windows[1].allTotal
            elif bin.windows[1].total == 0 :
                psi[0][1] += bin.windows[0].total * 1.0 / bin.windows[0].allTotal
            else :
                pd = bin.windows[0].total * 1.0 / bin.windows[0].allTotal
                po = bin.windows[1].total * 1.0 / bin.windows[1].allTotal
                psi[0][0] += (pd - po) * log(pd / po) / 2
            if bin.windows[0].pBad == 0 :
                psi[1][2] += bin.windows[1].pBad
            elif bin.windows[1].pBad == 0 :
                psi[1][1] += bin.windows[0].pBad
            else :
                psi[1][0] += (bin.windows[0].pBad - bin.windows[1].pBad) * log(bin.windows[0].pBad / bin.windows[1].pBad) / 2
            if bin.windows[0].pGood == 0 :
                psi[2][2] += bin.windows[1].pGood
            elif bin.windows[1].pGood == 0 :
                psi[2][1] += bin.windows[0].pGood
            else :
                psi[2][0] += (bin.windows[0].pGood - bin.windows[1].pGood) * log(bin.windows[0].pGood / bin.windows[1].pGood) / 2
        return psi

    def IV(self, badname) :
        # before and after rebin for each time window
        iv = []
        for i in xrange(self.numWindow) :
            iv.append(sum([bin.windows[i].iv for bin in self.original[badname] if bin.windows[i].pGood > 0 and bin.windows[i].pBad > 0]))
            iv.append(sum([bin.windows[i].iv for bin in self.solution[badname] if bin.windows[i].pGood > 0 and bin.windows[i].pBad > 0]))
        return iv

    def Rebin(self, badname, options) :
        for obin in self.original[badname] :
            self.solution[badname].append(Bin.Clone(obin))
        if self.isnumeric :
            (self.solution[badname], status) = numRebin(self.solution[badname], options)
        else :
            (self.solution[badname], status) = charRebin(self.solution[badname], options)
        if len(self.solution[badname]) > 0 :
            self.psi[badname] = self.PSI(badname)
            self.iv[badname] = self.IV(badname)
        return status

    def SimpleRebin(self, badname):
        for obin in self.original[badname] :
            self.solution[badname].append(Bin.Clone(obin))
        if len(self.solution[badname]) > 0 :
            self.psi[badname] = self.PSI(badname)
            self.iv[badname] = self.IV(badname)
        return 0, "OK"

def GreedyAlgorithm(bins, inc) :
    nwindow = len(bins[0].windows)
    for cwin in xrange(nwindow) :
        bins = FindWindowSolution(bins, inc, cwin)
    return bins

def FindWindowSolution(bins, inc, window) :
    start = 0
    end = len(bins)
    rev = []
    while start < end - 1:
        if bins[start].windows[window].woe * inc < bins[start + 1].windows[window].woe * inc:
            start += 1
            continue
        rev.append(bins.pop(start))
        end -= 1
        while start < end - 1 and bins[start].windows[window].woe * inc >= bins[start + 1].windows[window].woe * inc :
            rev.append(bins.pop(start))
            end -= 1
        rev.append(bins.pop(start))
        bins.insert(start, Bin.MergeBins(rev))
        start = 0
        end = len(bins)
        rev = []
    return bins

def SamePattern(left, right) :
    first = True
    flag = True
    for win in xrange(len(left)) :
        if first :
            flag = left[win].woe < right[win].woe
            first = False
        else :
            flag = (flag != (left[win].woe < right[win].woe))
            if flag :
                return False
    return True

def GreedyNoMonotony(bins) :
    start = 0
    end = len(bins)
    rev = []
    while start < end - 1:
        if SamePattern(bins[start].windows, bins[start + 1].windows):
            start += 1
            continue
        rev.append(bins.pop(start))
        bins.insert(start, Bin.MergeBins(rev))
        start = 0
        end = len(bins)
        rev = []
    return bins

def GreedySearchTree(bins, inc) :
    nodes = {(-1, -1) : [None, [], 0]}
    for i in xrange(len(bins)) :
        #nodes[(i, j)] = [bins[i:j], [(0,i-1),(i, j)], iv]
        for j in xrange(i, len(bins)) :
            cbin = Bin.MergeBins(bins[i : j+1])
            if i == 0 :
                nodes[(i, j)] = [cbin, [(i, j)], cbin.windows[0].iv]
            else :
                enodes = [(x, i-1) for x in xrange(i) if (x, i-1) in nodes]
                cmonos = [x for x in enodes if CheckMonotony(nodes[x][0], cbin, inc)]
                if len(cmonos) == 0 :
                    continue
                cmax = [nodes[x] for x in cmonos if nodes[x][2] == max([nodes[y][2] for y in cmonos])][0]
                clist = list(cmax[1])
                clist.append((i, j))
                cmetric = Evaluate(cmax[2], cbin)
                nodes[(i, j)] = [cbin, clist, cmetric]

    enodes = [(x, len(bins)-1) for x in xrange(len(bins)) if (x, len(bins)-1) in nodes]
    mnodes = [nodes[x] for x in enodes if nodes[x][2] == max([nodes[y][2] for y in enodes])]
    if len(mnodes) == 1 :
        snodes = mnodes[0]
    else :
        snodes = mnodes.sort(cmp=lambda x, y : cmp(len(x[1]), len(y[1])), reverse=True)[0]
    return [nodes[x][0] for x in snodes[1]]

def Evaluate(cvalue, cbin) :
    eva = cvalue + cbin.windows[0].iv
    if len(cbin.windows) > 1 :
        eva = eva + cbin.windows[1].iv
    return eva

def CheckMonotony(bina, binb, inc) :
    if bina is None :
        return True
    if binb is None :
        return False
    nw = len(bina.windows)
    for i in xrange(nw) :
        if bina.windows[i].woe * inc >= binb.windows[i].woe * inc :
            return False
    return True

def numRebin(bins, options) :
    st = [0, "OK", 0.0]
    if len(bins) < 2 :
        return bins, (2, "One bin", 0.0)
    # sort the bins by their id
    bins.sort(key=lambda x : x.rank)
    # small bin threshold
    total = bins[0].windows[0].allTotal
    rawTotal = sum([bin.windows[0].rawTotal for bin in bins])
    if options.drpb < 1 :
        options.drpb = rawTotal * options.drpb
    drpThreshold = max(int(options.drpb), 100)
    if options.aggb < 1 :
        options.aggb = rawTotal * options.aggb
    aggThreshold = max(int(options.aggb), 300)
    if options.outb < 1 :
        options.outb = rawTotal * options.outb
    outThreshold = max(int(options.outb), 500)
    # extract special values
    sbins = []
    default = 0.0

    while len(bins) > 0 :
        if bins[0].value is None :  # missing values
        # NOTE: this part does not take different missing values in concern, which should be merged together perhaps
            mbin = bins.pop(0)
            default += mbin.windows[0].total
            if CheckValid(mbin, 0) and mbin.windows[0].rawTotal > drpThreshold :
                sbins.append(mbin)
            continue
        num = float(bins[0].value)
        if num > 0 :
            break
        if num != int(num) :
            break
        if num < -9 and num / 9 != int(num / 9) :
            break
        sbin = bins.pop(0)
        default += sbin.windows[0].total
        if CheckValid(sbin, 0) and sbin.windows[0].rawTotal > drpThreshold :
            sbins.append(sbin)  # continual minus integar and zero
    if len(bins) == 0 :
        return sbins, (1, "No effective bin", 1.0)
    if bins[len(bins) - 1].value in [1e5, 1e6, 1e7, 1e8, 1e9]:
        sbin = bins.pop()
        default += sbin.windows[0].total
        if CheckValid(sbin, 0) and sbin.windows[0].rawTotal > drpThreshold :
            sbins.append(sbin)  # end special value
    if len(bins) == 0 :
        return sbins, (1, "No effective bin", 1.0)
    st[2] = default / total
    # aggregate small bins
    logging.debug("bins before aggregation : %d", len(bins))
    aggbins = AggregateSmallBins(bins, aggThreshold)
    logging.debug("bins after aggregation : %d", len(aggbins))
    # calculate the slope with the mbin.dWeight and mbin.oWeight, considering isolating the U shape extreme outlier
    inc = 0
    outliers = []
    outpos = 0
    nwindow = len(bins[0].windows)
    if len(aggbins) < 3 :
        inc = Slope(aggbins, 0)
    else :
        finc = []
        linc = []
        for i in xrange(nwindow) :
            finc.append(Slope(aggbins[1:], i))
            linc.append(Slope(aggbins[:-1], i))
            if finc[i] == linc[i] :
                inc = finc[i]
                break
        if inc == 0 :
            ainc = Slope(aggbins, 0)
            if ainc == 0 :
                logging.info("Zero slope found!")
            elif ainc == finc[0] :
                outliers.append(aggbins.pop(0))
                outpos = -1
            elif ainc == linc[0] :
                outliers.append(aggbins.pop())
                outpos = 1
            inc = ainc

    # find the optimal solution
    optimal = None
    if options.method == "gst" :
        optimal = GreedySearchTree(aggbins, inc)
    elif options.method == "grd" :
        optimal = GreedyAlgorithm(aggbins, inc)
    elif options.method == "gnp" :
        optimal = GreedyNoMonotony(aggbins)

    # check the sign if optimal has only one bin
    if len(optimal) == 1 and outpos == 0:
        logging.warning("this variable has only one bin left")
        st[0] = 1
        st[1] = "One effective bin"
        if not CheckSign(optimal[0]) :
            logging.error("and the only one left has different signs")
            st[0] = 2
            st[1] = "One unconsistent bin"
    if outpos == 1 :
        optimal.append(outliers[0])
    elif outpos == -1 :
        optimal.insert(0, outliers[0])
    # aggregate the small bins
    logging.debug("bins after optimal solution : %d", len(optimal))
    aggoptimal = AggregateSmallBins(optimal, outThreshold)
    logging.debug("bins after optimal aggregation : %d", len(aggoptimal))
    # check the special value woe and merge them with the solution
    sbins.reverse()
    for bin in sbins :
        if not CheckSign(bin) :
            for i in xrange(nwindow) :
                bin.windows[i].woe = 0
                bin.windows[i].iv = 0
            logging.warning("bin(%s) is neutralized due to different signs of WOEs", bin.value)
        if bin.value in [1e5, 1e6, 1e7, 1e8, 1e9] :
            aggoptimal.append(bin)
        else :
            aggoptimal.insert(0, bin)
    return aggoptimal, tuple(st)

def Slope(bins, window) :
    eps = 1e-30
    weights = [bin.windows[window].weight for bin in bins]
    length = len(weights)
    wgtSum = sum(weights)
    avgWeight = wgtSum * 1.0 / length
    avgY = reduce(lambda x, y : x + y, range(length)) * 1.0 / length
    total = 0.0
    for i in range(length) :
        total += (weights[i] - avgWeight) * (i - avgY)
    if total > eps :
        return 1
    elif total < - eps :
        return -1
    else :
        return 0

def charRebin(bins, options) :
    st = [0, "OK", 0.0]
    # small bin threshold
    total = bins[0].windows[0].allTotal
    rawTotal = sum([bin.windows[0].rawTotal for bin in bins])
    if options.drpb < 1 :
        options.drpb = rawTotal * options.drpb
    drpThreshold = max(int(options.drpb), 100)
    if options.aggb < 1 :
        options.aggb = rawTotal * options.aggb
    aggThreshold = max(int(options.aggb), 300)
    if options.outb < 1 :
        options.outb = rawTotal * options.outb
    outThreshold = max(int(options.outb), 500)
    # first step: clear variable
    # zero bin : if the bin wgtTotal = 0, no matter dev or oot, neutralize it.
    cbins = [bin for bin in bins if CheckValid(bin, 0)]
    # seperate the special value bins and
    # neutralize bins which are smaller than exclude threshold or in the special bins, whose dev and oot have different signs
    specials = ["OTHER", "NO_LAT", "NONE", "UNKNOWN"]
    errors = ["", " ", "_MISSING_", "NO_DATA", "NO_CCS", "NO_BILL_ADDRESS", "NO_LANG", "NO_EMAILS", "NO_IPS", "NO_PHONES", "MM_ERROR", "NOT_COUNTRY"]
    spSet = set(specials)
    spSet = spSet.union(errors)
    sbins = []  # special bins
    prebins = []  # bins ready for sorting
    for bin in cbins :
        if bin.value.upper() in spSet :
            st[2] += bin.windows[0].total * 1.0 / total
            if not CheckSign(bin) :
                logging.warning("bin(%s) is neutralized due to different signs of WOEs", bin.value)
            elif bin.windows[0].rawTotal > drpThreshold :
                sbins.append(bin)
        elif bin.windows[0].rawTotal < aggThreshold:
            if bin.windows[0].rawTotal > drpThreshold and CheckSign(bin) :
                prebins.append(bin)
        else :
            prebins.append(bin)
    if not len(prebins) :
        st[0] = 1
        st[1] = "All error bins"
        return sbins, tuple(st)
    # sort the bins for further sequential optimization
    sortbins = SortCharBinsCombine(prebins)
    # aggregate bins : if a bin's observations are too small, merge it with some other bins.
    logging.debug("bins before aggregation : %d", len(sortbins))
    aggbins = AggregateSmallBins(sortbins, aggThreshold)
    logging.debug("bins after aggregation : %d", len(aggbins))
    # do the sequential merging based on this sequence.
    optimal = None
    if options.method == "gst" :
        optimal = GreedySearchTree(aggbins, 1)
    elif options.method == "grd" :
        optimal = GreedyAlgorithm(aggbins, 1)
    elif options.method == "gnp" :
        optimal = GreedyNoMonotony(aggbins)
    logging.debug("bins after optimal solution : %d", len(optimal))
    # aggregate small bins after optimal solution is found, make sure each has larger than 2% observations.
    aggoptimal = AggregateSmallBins(optimal, outThreshold)
    logging.debug("bins after optimal aggregation : %d", len(aggoptimal))
    # combine optimal with special values
    sbins.extend(aggoptimal)
    return sbins, tuple(st)

def SortCharBinsCombine(pbins) :
    cWoes = [(i, pbins[i].CombineWoe()) for i in range(len(pbins))]
    cWoes.sort(key=lambda x: x[1])
    sbins = []
    for cWoe in cWoes :
        sbins.append(pbins[cWoe[0]])
    return sbins

def AggregateSmallBins(sbins, threshold) :
    if len(sbins) == 0 :
        return []
    total = sum([bin.windows[0].rawTotal for bin in sbins])
    if total < threshold :
        return [Bin.MergeBins(sbins)]
    ind = 0
    length = len(sbins)
    while ind < length :
        if sbins[ind].windows[0].rawTotal >= threshold :
            ind += 1
        else :
            if ind == 0 :
                bina = sbins.pop(0)
                binb = sbins.pop(0)
                binm = Bin.MergeBins([bina, binb])
                sbins.insert(0, binm)
            elif ind == length - 1 :
                bina = sbins.pop()
                binb = sbins.pop()
                binm = Bin.MergeBins([bina, binb])
                sbins.append(binm)
            else :
                bini = sbins.pop(ind)
                diff = sbins[ind].windows[0].rawTotal - sbins[ind - 1].windows[0].rawTotal
                if diff > 0:
                    ind -= 1
                elif diff < 0 :
                    ind = ind
                elif bini.windows[0].woe > 0 :
                    ind -= 1
                else :  # bini.windows[0].woe <= 0
                    ind = ind
                binj = sbins.pop(ind)
                binm = Bin.MergeBins([bini, binj])
                sbins.insert(ind, binm)
            length = len(sbins)
    return sbins

def CheckSign(bin) :
    nw = len(bin.windows)
    if nw == 1 :
        return True
    w0 = bin.windows[0]
    for win in bin.windows[1:] :
        if win.woe * w0.woe < 0 and abs(win.woe - w0.woe) > 0.2 :
            return False
    return True

def CheckValid(bin, valid) :
    nw = len(bin.windows)
    for i in xrange(nw) :
        if bin.windows[i].rawTotal <= valid :
            return False
    return True

def SaveToCsv(basepath, variables, status, badname) :
    zscls = {}
    firstLine = ["Variable #", "Binned", "Variable Name", "Num/Char", "# Obs", "% Miss", "#",
                 "Value/High", "#Wgt Total", "# Bad", "% Bad", "# Good", "% Good",
                 "Odds", "Bad Rate", "WOE", "IV", "Raw Total", "Raw Bad", "Raw Good"]
    nwindow = variables[0].numWindow
    for ind in xrange(nwindow) :
        path = basepath.format(ind)
        with open(path, 'w') as fn :
            fn.write(",".join(firstLine))
            fn.write("\n")
            vid = 1
            for variable in variables :
                rank = 1
                is_num = 1 if variable.isnumeric else 0
                for bin in variable.original[badname] :
                    win = bin.windows[ind]
                    crank = win.rank if variable.isnumeric else rank + 1
                    lineContent = [vid, "N", variable.name, is_num, win.allTotal, variable.missingRate[ind] * 100,
                                   crank, bin.value, win.total, win.bad, win.pBad * 100, win.good, win.pGood * 100,
                                   win.odds * 100, win.badRate * 100, win.woe, win.iv, win.rawTotal, win.rawBad,
                                   win.rawGood]
                    fn.write(",".join([str(x) for x in lineContent]))
                    fn.write("\n")
                    rank += 1
                rank = 1
                for bin in variable.solution[badname] :
                    win = bin.windows[ind]
                    crank = win.rank if variable.isnumeric else rank + 1
                    lineContent = [vid, "Y", variable.name, is_num, win.allTotal, variable.missingRate[ind] * 100,
                                   crank, bin.value, win.total, win.bad, win.pBad * 100, win.good, win.pGood * 100,
                                   win.odds * 100, win.badRate * 100, win.woe, win.iv, win.rawTotal, win.rawBad,
                                   win.rawGood]
                    fn.write(",".join([str(x) for x in lineContent]))
                    fn.write("\n")
                    rank += 1
                    if ind == 0 :
                        if variable.name not in zscls :
                            zsclsTemp = [0.0, 0.0, 0.0]
                            zscls[variable.name] = [0.0, 0.0]
                        capWoe = win.woe
                        if win.woe > 4 :
                            capWoe = 4
                        elif win.woe < -4 :
                            capWoe = -4
                        zsclsTemp[0] += capWoe * (win.good + win.bad)
                        zsclsTemp[1] += (capWoe ** 2) * (win.good + win.bad)
                        zsclsTemp[2] += win.good + win.bad
                # NOTE: notice here that all the discarded bins are missed in the zscaling part.
                if ind == 0 :
                    zscls[variable.name][0] = zsclsTemp[0] / zsclsTemp[2]
                    zscls[variable.name][1] = (abs(zsclsTemp[1] - (zsclsTemp[0] ** 2) / zsclsTemp[2]) / (zsclsTemp[2] - 1)) ** 0.5
                vid += 1

    #output the scorecard sheet, containing the variable information
    path = basepath.format("sum")
    firstLine = ["#", "Variable", "Num/Char", "WOE Variable"]
    for ind in xrange(nwindow) :
        firstLine.append("D{0} IV bef.".format(ind))
        firstLine.append("D{0} IV aft.".format(ind))
    firstLine.extend(["Wgt PSI", "Bad PSI", "Good PSI", "% Missing DEV", "% Missing OOT",
                      "Default %", "Miss %", "Status", "StCode"])
    with open(path, "w") as fn :
        fn.write(",".join(firstLine))
        fn.write("\n")
        vid = 1
        for variable in variables :
            is_num = 1 if variable.isnumeric else 0
            lineContent = [vid, variable.name, is_num, variable.woeName]
            for ivi in xrange(len(variable.iv[badname])) :
                lineContent.append(variable.iv[badname][ivi])
            lineContent.extend([variable.psi[badname][0][0], variable.psi[badname][1][0], variable.psi[badname][2][0],
                                variable.psi[badname][0][1] * 100, variable.psi[badname][0][2] * 100,
                                status[variable.name][2] * 100, max(variable.missingRate) * 100,
                                status[variable.name][1], status[variable.name][0]])
            fn.write(",".join([str(x) for x in lineContent]))
            fn.write("\n")
    return zscls

def SaveToSasWoe(path, variables, excludes, badname) :
    if not excludes :
        excludes = set()
    woe = open(path, "w")
    for variable in variables :
        if variable.name in excludes :
            continue
        oriName = variable.name
        woe.write("/* WOE recoding for %s */\n" % oriName)
        woeName = variable.woeName
        numbins = len(variable.solution[badname])
        if variable.isnumeric :
            woe.write("if %s = . then %s = " % (oriName, woeName))
            if variable.solution[badname][0].rank == 0 and oriName.lower().find("ars_") < 0:
                woeValue = variable.solution[badname][0].windows[0].woe
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                woe.write("%.6f;\n" % woeValue)
            else :
                woe.write("0.000000;\n")
            lastValue = "-1e38"
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                value = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                if i == numbins - 1 :
                    woe.write("else if %s > %s then %s = %.6f;\nelse %s = 0.000000;\n" % (oriName, lastValue, woeName, woeValue, woeName))
                else :
                    woe.write("else if (%s < %s <= %s) then %s = %.6f;\n" % (lastValue, oriName, value, woeName, woeValue))
                    lastValue = value
        else :
            firstFlag = True
            values = []
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                if variable.name.lower().startswith("ars_") and variable.solution[badname][i].value == "" :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                rawValue = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                value = ""
                for token in rawValue.split(",") :
                    rtoken = ' ' if token == "" else repr(token)
                    if value == "" :
                        value = rtoken
                        continue
                    new_value = ", ".join((value, rtoken))
                    if len(new_value) > 128 :
                        values.append(value)
                        value = rtoken
                    else :
                        value = new_value
                values.append(value)
                for value in values :
                    if not firstFlag :
                        woe.write("else ")
                    else :
                        firstFlag = False
                    woe.write("if %s in (%s) then %s = %.6f;\n" % (oriName, value, woeName, woeValue))
                values = []
            woe.write("else %s = 0.000000;\n" % woeName)
        woe.write("\n")
    woe.close()

def SaveToWoe(path, variables, excludes, badname) :
    if not excludes :
        excludes = set()
    woe = open(path, "w")
    for variable in variables :
        if variable.name in excludes :
            continue
        is_num = 1 if variable.isnumeric else 0
        oriName = variable.name
        woe.write("# woe transform %s,%d\n" % (oriName, is_num))
        woeName = variable.woeName
        numbins = len(variable.solution[badname])
        if variable.isnumeric :
            woe.write("if %s is None:\n    %s = " % (oriName, woeName))
            if variable.solution[badname][0].rank == 0 and oriName.lower().find("ars_") < 0:
                woeValue = variable.solution[badname][0].windows[0].woe
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                woe.write("%.6f;\n" % woeValue)
            else :
                woe.write("0.000000;\n")
            lastValue = "-1e38"
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                value = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                if i == numbins - 1 :
                    woe.write("elif %s > %s :\n    %s = %.6f\nelse:\n    %s = 0.000000\n" % (oriName, lastValue, woeName, woeValue, woeName))
                else :
                    woe.write("elif (%s < %s <= %s):\n    %s = %.6f\n" % (lastValue, oriName, value, woeName, woeValue))
                    lastValue = value
        else :
            firstFlag = True
            values = []
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                if variable.name.lower().startswith("ars_") and variable.solution[badname][i].value == "" :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                rawValue = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                values.append(", ".join([repr(x) for x in rawValue.split(",")]))
                for value in values :
                    if not firstFlag :
                        woe.write("el")
                    else :
                        firstFlag = False
                    woe.write("if %s in (%s) :\n    %s = %.6f;\n" % (oriName, value, woeName, woeValue))
                values = []
            woe.write("else:\n    %s = 0.000000;\n" % woeName)
        woe.write("\n")
    woe.close()

def SaveToZWoe(path, variables, excludes, badname, zscls) :
    """ apply z-scaling in the woe transform """
    if not excludes :
        excludes = set()
    woe = open(path, "w")
    for variable in variables :
        if variable.name in excludes :
            continue
        is_num = 1 if variable.isnumeric else 0
        oriName = variable.name
        woe.write("# woe transform %s,%d\n" % (oriName, is_num))
        woeName = variable.woeName + "_zscl"
        numbins = len(variable.solution[badname])
        if variable.isnumeric :
            woe.write("if %s is None:\n    %s = " % (oriName, woeName))
            if variable.solution[badname][0].rank == 0 and oriName.lower().find("ars_") < 0:
                woeValue = variable.solution[badname][0].windows[0].woe
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                woeValue = (woeValue - zscls[variable.name][0]) / zscls[variable.name][1]
                woe.write("%.6f;\n" % woeValue)
            else :
                woe.write("%.6f;\n" % (-zscls[variable.name][0] / zscls[variable.name][1]))
            lastValue = "-1e38"
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                value = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                woeValue = (woeValue - zscls[variable.name][0]) / zscls[variable.name][1]
                if i == numbins - 1 :
                    woe.write("elif %s > %s :\n    %s = %.6f\nelse:\n    %s = %.6f\n" % (oriName, lastValue, woeName, woeValue, woeName, (-zscls[variable.name][0] / zscls[variable.name][1])))
                else :
                    woe.write("elif (%s < %s <= %s):\n    %s = %.6f\n" % (lastValue, oriName, value, woeName, woeValue))
                    lastValue = value
        else :
            firstFlag = True
            values = []
            for i in xrange(numbins) :
                if variable.solution[badname][i].rank == 0 :
                    continue
                if variable.name.lower().startswith("ars_") and variable.solution[badname][i].value == "" :
                    continue
                woeValue = variable.solution[badname][i].windows[0].woe
                rawValue = variable.solution[badname][i].value
                if woeValue > 4 :
                    woeValue = 4
                elif woeValue < -4 :
                    woeValue = -4
                woeValue = (woeValue - zscls[variable.name][0]) / zscls[variable.name][1]
                values.append(", ".join([repr(x) for x in rawValue.split(",")]))
                for value in values :
                    if not firstFlag :
                        woe.write("el")
                    else :
                        firstFlag = False
                    woe.write("if %s in (%s) :\n    %s = %.6f;\n" % (oriName, value, woeName, woeValue))
                values = []
            woe.write("else:\n    %s = %.6f;\n" % (woeName, (-zscls[variable.name][0] / zscls[variable.name][1])))
        woe.write("\n")
    woe.close()

def SaveVarList(path, variables, excludes) :
    if not excludes :
        excludes = set()
    vlist = open(path, "w")
    for variable in variables :
        if variable.name in excludes :
            continue
        vlist.write("%s,%s,%d\n" % (variable.name, variable.woeName, variable.isnumeric))
    vlist.close()

def SaveUniAnalysis(path, variables, status, badname) :
    analysis = open(path, "w")
    firstLine = [str("%-32s" % "Variable"),
                 str("%7s" % "StCode")]
    nwindow = variables[0].numWindow
    for i in xrange(nwindow) :
        ivs = "Data {0} IV b.".format(i)
        firstLine.append(str("%13s" % ivs))
        ivs = "Data {0} IV a.".format(i)
        firstLine.append(str("%13s" % ivs))
        ivs = "Data {0} max IV".format(i)
        firstLine.append(str("%13s" % ivs))
    firstLine.extend([str("%8s" % "Wgt PSI"),
                      str("%8s" % "Bad PSI"),
                      str("%9s" % "Good PSI"),
                      str("%13s" % "% Missing d."),
                      str("%13s" % "% Missing o."),
                      str("%10s" % "Default %"),
                      str("%9s" % "Miss %"),
                      str(" %-32s" % "Woe Name"),
                      " Status"])
    analysis.write(" |".join(firstLine))
    analysis.write("\n")
    for variable in variables :
        maxmr = max(variable.missingRate)
        uniLine = [str("%-32s" % variable.name),
                   str("%7d" % status[variable.name][0])]
        for i in xrange(nwindow) :
            uniLine.append(str("%13.4f" % variable.iv[badname][i * 2]))
            uniLine.append(str("%13.4f" % variable.iv[badname][i * 2 + 1]))
            #calculate the max iv in the time window here!
            maxiv = max([bin.windows[i].iv for bin in variable.solution[badname] if bin.windows[i].pGood > 0 and bin.windows[i].pBad > 0])
            uniLine.append(str("%13.4f" % maxiv))
        uniLine.extend([str("%8.4f" % variable.psi[badname][0][0]),
                        str("%8.4f" % variable.psi[badname][1][0]),
                        str("%9.4f" % variable.psi[badname][2][0]),
                        str("%13.4f" % variable.psi[badname][0][1]),
                        str("%13.4f" % variable.psi[badname][0][2]),
                        str("%10.2f" % (status[variable.name][2] * 100)),
                        str("%9.2f" % (maxmr * 100)),
                        str(" %-32s" % variable.woeName),
                        " " + status[variable.name][1]])
        analysis.write(" |".join(uniLine))
        analysis.write("\n")
    analysis.close()


if __name__ == "__main__" :
    parser = OptionParser()
    parser.add_option("-b", "--bin", dest="bin", help="binning input file", action="store", type="string")
    parser.add_option("-w", "--woe", dest="woe", help="WoE output file", action="store", type="string")
    parser.add_option("-l", "--log", dest="log", help="log file", action="store", type="string")
    parser.add_option("-f", "--suffix", dest="suf", help="suffix except _woe", action="store", type="string")
    parser.add_option("-i", "--iv", dest="iv", help="iv excluding threshold", action="store", type="float", default=0.001)
    parser.add_option("-r", "--mrate", dest="mr", help="missing rate excluding threshold", action="store", type="float", default=0.95)
    parser.add_option("-d", "--drpb", dest="drpb", help="drop bin threshold", action="store", type="float", default=100)
    parser.add_option("-g", "--aggb", dest="aggb", help="aggregation bin threshold", action="store", type="float", default=500)
    parser.add_option("-o", "--outb", dest="outb", help="output min bin threshold", action="store", type="float", default=0.02)
    parser.add_option("-m", "--method", dest="method", help="algorithms to choose: 'gst': greedy search tree; 'grd': greedy monotonic pattern; 'gnp': greedy no monotony constrain", action="store", type="string", default="gst")
    (options, args) = parser.parse_args()

    if not options.bin :
        print "You must specify the fine binning result file!"
        exit()
    file_name = os.path.basename(options.bin)
    file_root_name = os.path.join(os.path.dirname(options.bin), file_name[:file_name.rfind('.')])
    if options.woe and options.woe.find('{0}') == -1 :
        options.woe = options.woe[:options.woe.rfind('.')] + "{0}" + options.woe[options.woe.rfind('.'):]
    else :
        options.woe = file_root_name + "_woe{0}.txt"
    if options.log :
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s', filename=options.log, filemode='w')
    else :
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    options.csv = file_root_name + "_csv{0}_{1}.txt"
    options.uni = file_root_name + "_uni{0}.txt"
    options.lst = file_root_name + "_lst{0}.txt"
    if options.suf :
        options.suf += "_woe"
    else :
        options.suf = "_woe"

    logging.info("program started...")
    varsecs = []
    binfile = open(options.bin, "r")
    content = "\n".join(binfile.readlines())
    binfile.close()
    varsecs = map(str.strip, content.split("===="))
    varsecs = [sec for sec in varsecs if (sec and sec.find("----") >= 0)]

    logging.info("binning files read, start parsing...")
    variables = []
    badnames = set()
    for i in xrange(len(varsecs)) :
        variable = Variable(varsecs[i], options)
        variables.append(variable)
    if len(variables) > 0 :
        badnames = list(variables[0].badrates.keys())
    else :
        logging.error("no data is read!")
        exit()

#    variable.woeName = ResultSaver.GenerateWoeName(variable.name, suffix)

    logging.info("variables initiated, start automatic re-binning...")
    for badname in badnames :
        logging.info("handling bad tagging : {0}".format(badname))
        ts = time.clock()
        excludes = set()
        status = {}
        for variable in variables :
            status[variable.name] = [0, "OK", 0.0]
            logging.info("current variable : %s", variable.name)
            if len(variable.original[badname]) == 1 :
                logging.warning("%s has only one bin before auto re-binning, excluded!", variable.name)
                excludes.add(variable.name)
                variable.SimpleRebin(badname)
                status[variable.name][0] = 2
                status[variable.name][1] = "One bin before"
                continue
            maxmr = max(variable.missingRate)
            if maxmr > options.mr :
                logging.warning("%s has too large missing rate, excluded!", variable.name)
                excludes.add(variable.name)
                variable.SimpleRebin(badname)
                status[variable.name][0] = 2
                status[variable.name][1] = "Large missing rate"
                continue

            # call the rebin function
            rstatus = variable.Rebin(badname, options)
            status[variable.name][0] = rstatus[0]
            status[variable.name][1] = rstatus[1]
            status[variable.name][2] = rstatus[2]

            if len(variable.solution[badname]) == 0 :
                logging.warning("%s has no bin left after re-bin process, excluded!", variable.name)
                excludes.add(variable.name)
                variable.SimpleRebin(badname)
                status[variable.name][0] = 2
                status[variable.name][1] = "No bin left after"
                continue
            if len(variable.solution[badname]) == 1:
                logging.warning("%s has only one bin after auto re-binning, excluded!", variable.name)
                excludes.add(variable.name)
                status[variable.name][0] = 2
                status[variable.name][1] = "One bin after"
                continue
            miniv = min(variable.iv[badname])
            if miniv < options.iv :
                logging.warning("%s has very small IV, excluded!", variable.name)
                excludes.add(variable.name)
                status[variable.name][0] = 2
                status[variable.name][1] = "Small IV"
                continue
        te = time.clock()
        print("%f seconds for rebin" % (te - ts))
        logging.info("auto re-binning for {0} finished. start exporting...".format(badname))

        logging.info("start writing woe file after variable exclusion...")
        bad_suffix = "_" + badname if len(badnames) > 1 else ''
        SaveToWoe(options.woe.format(bad_suffix), variables, excludes, badname)

        logging.info("start writing woe variables list...")
        SaveVarList(options.lst.format(bad_suffix), variables, excludes)

        logging.info("start writing the variable analysis...")
        SaveUniAnalysis(options.uni.format(bad_suffix), variables, status, badname)

        logging.info("start saving the binning details...")
        zscls = SaveToCsv(options.csv.format(bad_suffix, "{0}"), variables, status, badname)

        logging.info("start writing woe file after variable exclusion...")
        SaveToZWoe(options.woe.format("_zscl" + bad_suffix), variables, excludes, badname, zscls)

        #After this process, use the woe file to generate transformed variables. Maybe in another py file.
