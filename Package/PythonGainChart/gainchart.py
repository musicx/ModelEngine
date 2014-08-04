import json
import logging
import itertools
import collections
from optparse import OptionParser
import os
import pandas as pd
import numpy as np

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


def operation_performance(data, score, weights=['dummy'], bads=['is_bad'], groups=['window_name'],
                          catches=[], points=100., rank_base='dummy') :
    raw = data[[score] + weights + bads + groups + catches]
    score_rank = score + "_rank"
    # Assuming higher score indicates higher risk
    raw[score_rank] = raw.groupby(groups)[score].transform(pd.Series.rank, method='min',
                                                           ascending=False, na_option='bottom')
    score_groups = groups + [score_rank]
    raw.sort(columns=score_groups, inplace=True)
    #rank_cumsum = pd.DataFrame(raw.groupby(score_groups)[rank_base].sum().groupby(level=-2).cumsum(),
    #                           columns=["rank_cumsum"]).reset_index()
    rank_cumsum = pd.DataFrame(raw.groupby(score_groups)[rank_base].sum().groupby(level=-2).cumsum().reset_index())
    rank_cumsum.columns = pd.Index(score_groups + ['rank_cumsum'])
    raw = raw.merge(rank_cumsum, how="left", on=score_groups)
    raw.loc[:, score_rank] = raw['rank_cumsum'] / raw.groupby(groups)[rank_base].transform(pd.Series.sum)
    raw.loc[:, score_rank] = raw[score_rank].apply(lambda x: np.ceil(x * points) * (1.0 / points))
    raw.pop('rank_cumsum')

    bad_dummy_names = {}
    full_bads_dummy_names = []
    for bad in bads :
        bad_dummy = pd.get_dummies(raw[bad])
        bad_dummy.rename(columns=dict(zip(bad_dummy.columns,
                                          ["{}|{}".format(x[0], x[1])
                                           for x in zip([bad] * len(bad_dummy.columns), bad_dummy.columns)])),
                         inplace=True)
        raw = raw.merge(bad_dummy, left_index=True, right_index=True)
        bad_dummy_names[bad] = list(bad_dummy.columns)
        full_bads_dummy_names.extend(bad_dummy.columns)

    weights_bads_names = [x + '|' + y for x in weights for y in full_bads_dummy_names]
    weights_bads_raw = pd.concat([raw[full_bads_dummy_names].mul(raw[x], axis=0) for x in weights], axis=1)
    weights_bads_raw.columns = pd.Index(weights_bads_names)
    raw[weights_bads_names] = weights_bads_raw

    score_cut_offs = raw.groupby(score_groups)[score].min()
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    totals = sums.sum(level=-2)       #TODO: what if there is more levels
    sums = sums.groupby(level=-2).cumsum()
    catch_rates = sums.divide(totals + 1e-20, axis=0)
    catch_rates.columns = [x + '|catch_rate' for x in catch_rates.columns]
    hit_rate_list = []
    hit_rate_names = []
    for weight in weights :
        weight_bads_names = [weight + '|' + x for x in full_bads_dummy_names]
        hit_rate = sums[weight_bads_names].divide(sums[weight] + 1e-20, axis=0)
        hit_rate_list.append(hit_rate)
        hit_rate_names.extend([x + '|hit_rate' for x in weight_bads_names])
    hit_rates = pd.concat(hit_rate_list, axis=1)
    hit_rates.columns = pd.Index(hit_rate_names)

    raw_output = pd.concat([score_cut_offs, sums, catch_rates, hit_rates], axis=1)
    raw_output.reset_index(inplace=True)
    raw_output['score_name'] = score
    raw_output['rank_base'] = rank_base
    raw_output['group_name'] = ",".join(groups)
    return raw_output

def score_performance(data, score, weights=['dummy'], bads=['is_bad'], groups=['window_name'],
                      catches=[], low=0, step=5, high=1000):
    raw = data[[score] + weights + bads + groups + catches]
    score_rank = score + "_rank"
    # Assuming higher score indicates higher risk
    raw.loc[(raw[score] > high), score] = high
    raw.loc[(raw[score] < low), score] = low
    raw.fillna({score : low}, inplace=True)
    raw[score_rank] = raw[score].apply(lambda x: np.ceil(x * 1.0 / step) * step)

    bad_dummy_names = {}
    full_bads_dummy_names = []
    for bad in bads :
        bad_dummy = pd.get_dummies(raw[bad])
        bad_dummy.rename(columns=dict(zip(bad_dummy.columns,
                                          ["{}|{}".format(x[0], x[1])
                                           for x in zip([bad] * len(bad_dummy.columns), bad_dummy.columns)])),
                         inplace=True)
        raw = raw.merge(bad_dummy, left_index=True, right_index=True)
        bad_dummy_names[bad] = list(bad_dummy.columns)
        full_bads_dummy_names.extend(bad_dummy.columns)

    weights_bads_names = [x + '|' + y for x in weights for y in full_bads_dummy_names]
    weights_bads_raw = pd.concat([raw[full_bads_dummy_names].mul(raw[x], axis=0) for x in weights], axis=1)
    weights_bads_raw.columns = pd.Index(weights_bads_names)
    raw[weights_bads_names] = weights_bads_raw

    score_groups = groups + [score_rank]
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    sums.sort_index(ascending=False, inplace=True)
    totals = sums.sum(level=-2)
    sums = sums.groupby(level=-2).cumsum()
    catch_rates = sums.divide(totals + 1e-20, axis=0)
    catch_rates.columns = [x + '|catch_rate' for x in catch_rates.columns]
    hit_rate_list = []
    hit_rate_names = []
    for weight in weights :
        weight_bads_names = [weight + '|' + x for x in full_bads_dummy_names]
        hit_rate = sums[weight_bads_names].divide(sums[weight] + 1e-20, axis=0)
        hit_rate_list.append(hit_rate)
        hit_rate_names.extend([x + '|hit_rate' for x in weight_bads_names])
    hit_rates = pd.concat(hit_rate_list, axis=1)
    hit_rates.columns = pd.Index(hit_rate_names)

    raw_output = pd.concat([sums, catch_rates, hit_rates], axis=1)
    raw_output.reset_index(inplace=True)
    raw_output['score_name'] = score
    raw_output['group_name'] = ",".join(groups)
    return raw_output

def parse_json(src_string):
    if os.path.exists(src_string):
        line = open(src_string).read()
    else:
        line = src_string
    try:
        sources = json.loads(line)
    except ValueError:
        return None
    # for source in sources :
    #    if not os.path.exists(source["path"]) :
    #        return None
    if "input" not in sources or "score" not in sources or "bad" not in sources :
        return None
    return sources


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-j", "--json", dest="json", action="store", type="string",
                      help="raw string of scored files encoded with json or json file. example:\n" +
                           '{"input":["score.csv"],"score":["new_score"],"bad":["is_bad"],"weight":["unit","dollar"],' +
                           '"catch":["loss"],"group":{"group_name":["region"]}}')
    # following parts can be ignored if json is given
    parser.add_option("-i", "--input", dest="input", action="store", type="string",
                      help="input datasets, separated with ','")
    parser.add_option("-s", "--score", dest="score", action="store", type="string",
                      help="score variables, separated with ','")
    parser.add_option("-b", "--bad", dest="bad", action="store", type="string",
                      help="name of bad variables")
    parser.add_option("-w", "--wgt", dest="wgt", action="store", type="string", default="dummy",
                      help="names of weight variables, separated by ',', default is dummy")
    parser.add_option("-c", "--catch", dest="catch", action="store", type="string",
                      help="names of extra variables for catch rate calculation, separated with ','")
    parser.add_option("-g", "--group", dest="group", action="store", type="string",
                      help="optional, group combinations in format class_var1,class_var2|class_var3")
    # following parts are optional
    parser.add_option("-l", "--log", dest="log", action="store", type="string",
                      help="log file, if not given, stdout is used")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=','")
    parser.add_option("-n", "--name", dest="name", action="store", type="string",
                      help="names of input datasets, separated with ','")
    parser.add_option("-o", "--out", dest="out", action="store", type="string", default="perf_report",
                      help="output performance file")
    # following parts are for report control
    parser.add_option("-r", "--range", dest="range", action="store", type="string", default="0,5,1000",
                      help="score capping and step check after auto-mapping, given as low,step,high. default is 0,5,1000")
    parser.add_option("-p", "--point", dest="point", action="store", type="float", default=100.,
                      help="number of operation points in reports")
    (options, args) = parser.parse_args()

    nfloat = ignore_exception(ValueError, None)(float)

    input_data = []
    score_vars = []
    bad_vars = []
    weight_vars = []
    catch_vars = []
    groups = {}
    data_names = []
    
    if options.json:
        sources = parse_json(options.json)
        if sources is None:
            logging.error("Error occurs during parsing the source json")
            exit()
        print sources

        if type(sources['input']) is list:
            input_data.extend(sources['input'])
        elif type(sources['input']) is str:
            input_data.append(sources['input'])
        else:
            logging.error("Error parsing input field in json")
            exit()
        
        if type(sources['score']) is list:
            score_vars.extend(sources['score'])
        elif type(sources['score']) is str:
            score_vars.append(sources['score'])
        else:
            logging.error("Error parsing score field in json")
            exit()
            
        if type(sources['bad']) is list:
            bad_vars.extend(sources['bad'])
        elif type(sources['bad']) is str:
            bad_vars.append(sources['bad'])
        else:
            logging.error("Error parsing bad field in json")
            exit()

        if "weight" in sources :
            if type(sources['weight']) is list:
                weight_vars.extend(sources['weight'])
            elif type(sources['weight']) is str:
                weight_vars.append(sources['weight'])
            else:
                logging.error("Error parsing weight field in json")
                exit()
        if len(weight_vars) == 0 :
            weight_vars.append("dummy")

        if "catch" in sources :
            if type(sources['catch']) is list:
                catch_vars.extend(sources['catch'])
            elif type(sources['catch']) is str:
                catch_vars.append(sources['catch'])
            else:
                logging.error("Error parsing catch field in json")
                exit()

        if "group" in sources :
            for group_name, group_vars in sources['group'] :
                if type(group_vars) is str:
                    groups[group_name] = [group_vars]
                elif type(group_vars) is list:
                    groups[group_name] = group_vars
                else :
                    logging.error("Error parsing group fileds in json")
                    exit()
    else :
        if not options.input:
            logging.error("Input datasets must be specified")
            exit()
        input_data = [x.strip() for x in options.input.split(',')]

        if not options.score:
            logging.error("Score variables must be specified")
            exit()
        score_vars = [x.strip() for x in options.score.split(',')]

        if not options.bad:
            logging.error("Bad variables must be specified")
            exit()
        bad_vars = [x.strip() for x in options.bad.split(',')]

        if options.wgt:
            weight_vars = [x.strip() for x in options.wgt.split(',')]
        if len(weight_vars) == 0:
            weight_vars.append('dummy')

        if options.catch:
            catch_vars = [x.strip() for x in options.catch.split(',')]

        if options.group:
            group_names = options.group.split('|')
            for group in group_names :
                groups[group] = [x.strip() for x in group.split(',')]

    delimiter = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm

    data_names = ["data_{}".format(x) for x in xrange(1, max(1, len(input_data))+1)]
    if options.name :
        names = [x.strip().lower() for x in options.name.split(',')]
        for ind in xrange(min(len(names), len(input_data))) :
            data_names[ind] = names[ind]
    add_windows = True if len(data_names) > 1 else False

    score_vars = [x.lower() for x in score_vars]
    bad_vars = [x.lower() for x in bad_vars]
    weight_vars = [x.lower() for x in weight_vars]
    catch_vars = [x.lower() for x in catch_vars]
    group_vars = set()
    for group_name in groups :
        groups[group_name] = [x.lower() for x in groups[group_name]]
        if add_windows :
            groups[group_name].insert(0, 'window_name')
        group_vars = group_vars.union(groups[group_name])
    if add_windows or len(groups) == 0:
        add_windows = add_windows or len(groups) == 0
        groups['windows'] = ['window_name']
        group_vars.add('window_name')
    group_vars = list(group_vars)

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)-15s - %(levelname)s - %(message)s')

    if len(options.range.split(',')) != 3:
        logging.error("Score range must be given in format low,step,high")
        exit()
    low, step, high = [nfloat(x) for x in options.range.split(',')]
    if low is None or step is None or high is None :
        logging.error("Score range parsing error")
        exit()

    point = options.point

    logging.info("program started...")

    data_list = []
    ind = 0
    for filepath in input_data:
        if not os.path.exists(filepath) :
            logging.error("File not exist! " + filepath)
            continue
        cur_data = pd.read_csv(filepath, sep=delimiter)
        cur_data.rename(columns=dict(zip(cur_data.columns, [x.lower() for x in cur_data.columns])), inplace=True)

        # TODO: just for test here
        #cur_data['even'] = cur_data['char_cp_cust_id'].map(lambda x : 1 if x % 10 < 5 else 0)
        cur_data['clsn_scr_new'] = cur_data['clsn_scr']

        if add_windows :
            cur_data['window_name'] = data_names[ind]
        cur_data['dummy'] = 1
        data_list.append(cur_data)
        logging.info("file {0} has been read : {1}".format(data_names[ind], filepath))
        ind += 1
    big_raw = pd.concat(data_list)[score_vars + weight_vars + bad_vars + catch_vars + group_vars]
    logging.info("fill missing weight, bad, and catch variables with 0")
    big_raw.fillna(pd.Series([0] * len([weight_vars + bad_vars + catch_vars]),
                             index=[weight_vars + bad_vars + catch_vars]), inplace=True)

    # score auto-mapping. This step will map [0,1] ranged score to [0,1000], without touching minus scores.
    #mapped_scores = big_raw[score_vars] * ((big_raw[score_vars].max() <= 1) * 999 + 1)
    #big_raw[score_vars] = big_raw[score_vars][big_raw[score_vars] < 0].combine_first(mapped_scores)
    # or
    max_ind = big_raw[score_vars].max() <= 1
    if any(list(max_ind)) :
        logging.info("score scaling is found to be necessary, start...")
        big_raw[score_vars] = big_raw[score_vars].apply(lambda x: x * ((max_ind & (x > 0)) * 999 + 1), axis=1)

    operation_raw_list = collections.defaultdict(list)
    score_raw_list = collections.defaultdict(list)
    for group_name, score_var, weight_var in itertools.product(groups, score_vars, weight_vars) :
        logging.info("handling {0} based on {1} in group {2}".format(score_var, weight_var, group_name))
        operation_raw = operation_performance(data=big_raw, score=score_var,
                                              weights=weight_vars, bads=bad_vars,
                                              groups=groups[group_name], catches=catch_vars,
                                              points=point, rank_base=weight_var)
        logging.info("operation point based analysis done")
        score_raw = score_performance(data=big_raw, score=score_var, weights=weight_vars,
                                      bads=bad_vars, groups=groups[group_name], catches=catch_vars,
                                      low=low, step=step, high=high)
        logging.info("score based analysis done")
        operation_raw_list[group_name].append(operation_raw)
        score_raw_list[group_name].append(score_raw)
    logging.info("all raw analysis done, start creating pivot tables...")
    with pd.ExcelWriter(options.out+'.xlsx') as writer :
        for group_name in groups :
            group_operation_raws = pd.concat(operation_raw_list[group_name])
            group_score_raws = pd.concat(score_raw_list[group_name])


            try :
                group_operation_raws.to_excel(writer, sheet_name="operation_raw", index=False)
                group_score_raws.to_excel(writer, sheet_name="score_raw", index=False)
            except RuntimeError as e :
                logging.error(e.message)

    logging.info("all work done")
    #output_final_results(full_operation_results, full_operation_raws, full_score_results, full_score_raws)


    # test code
simple_test = '''
test = pd.DataFrame(np.random.randn(100,2)+1, columns=['S1','S2'])
test['d'] = pd.Series(['dev']*50+['oot']*50)
test['b'] = test['S1'].map(lambda x: 1 if x > 1.3 and np.random.random() > 0.3 else 0)
test['c'] = test['S2'].map(lambda x: 1 if x > 1.4 and np.random.random() > 0.2 else 0)
test['w'] = 1
test['y'] = 2
test[[x+'_r' for x in ['S1','S2']]] = test.groupby('d').transform(pd.Series.rank, method="min")[['S1','S2']]
test[[x+'_r' for x in ['S1','S2']]] /= test[['d'] + [x+'_r' for x in ['S1','S2']]].groupby('d').transform(pd.Series.count)*1.0 + 1
test[[x+'_r' for x in ['S1','S2']]] = test[[x+'_r' for x in ['S1','S2']]].applymap(lambda x: np.floor(x * 10) * 0.1)
test1 = test[['d','S1','S1_r','w','y','b','c']]
b_dm = pd.get_dummies(test1['b'])
b_dm.rename(columns=dict(zip(b_dm.columns, ["{}{}".format(x[0],x[1]) for x in zip(['b_']*len(b_dm.columns), b_dm.columns)])), inplace=True)
test1 = test1.merge(b_dm, left_index=True, right_index=True)
c_dm = pd.get_dummies(test1['c'])
c_dm.rename(columns=dict(zip(c_dm.columns, ["{}{}".format(x[0],x[1]) for x in zip(['c_']*len(c_dm.columns), c_dm.columns)])), inplace=True)
test1 = test1.merge(c_dm, left_index=True, right_index=True)
sum_columns = [x + '_' + y for x in ['w', 'y'] for y in ['b_0', 'b_1', 'c_0', 'c_1']]
wb1 = pd.concat([test1[['b_0', 'b_1', 'c_0', 'c_1']].mul(test1[x], axis=0) for x in ['w', 'y']], axis=1)
wb1.columns = pd.Index(sum_columns)
test1[sum_columns] = wb1[sum_columns]
sums1 = test1.groupby(['d', 'S1_r'])[['w','y']+sum_columns].sum()
totals = sums1.sum(level=-2)
sums1 = sums1.groupby(level=-2).cumsum()

tg1 = test1.groupby(['d', 'S1_r', 'b'])
sum1 = test.groupby(['d','S1_r','b'])['w','y'].agg(sum).unstack().fillna(0)
sum2 = test.groupby(['d','S2_r','b'])['w','y'].agg(sum).unstack().fillna(0)
sum1.index.set_names(['d','s'],inplace=True)
sum2.index.set_names(['d','s'],inplace=True)
sum1.columns = pd.MultiIndex(levels=[['s1']] + sum1.columns.levels, labels=[[0]*4] + sum1.columns.labels, names=['score','weight','bad'])
sum1.columns=sum1.columns.droplevel()
sum1.stack(level=0).swaplevel(1,2).sortlevel()
'''

