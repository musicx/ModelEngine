import json
import logging
import itertools
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

def aggregation(data, variables, totals, key, higher_worse):
    aggregated = data[variables].aggregate(np.sum).reset_index()
    agg_rank = "AGG_TEMP_RANK"
    aggregated[agg_rank] = aggregated[key].rank(method='min', ascending=not higher_worse)
    agg_sorted = aggregated.sort(columns=agg_rank)

    agg_sorted['CUM_TOTAL_UNIT'] = agg_sorted[variables[0]].cumsum(axis=0)
    agg_sorted['CUM_TOTAL_DOLLAR'] = agg_sorted[variables[1]].cumsum(axis=0)
    agg_sorted['CUM_BAD_UNIT'] = agg_sorted[variables[2]].cumsum(axis=0)
    agg_sorted['CUM_BAD_DOLLAR'] = agg_sorted[variables[3]].cumsum(axis=0)
    return_columns = [key, 'CUM_BAD_UNIT', 'CUM_TOTAL_UNIT', 'CUM_BAD_DOLLAR', 'CUM_TOTAL_DOLLAR']
    for catch_variable in variables[4:]:
        agg_sorted['CUM_' + catch_variable.upper()] = agg_sorted[catch_variable].cumsum(axis=0)
        return_columns.append('CUM_' + catch_variable.upper())
    agg_sorted.drop_duplicates(cols=[agg_rank], take_last=True, inplace=True)

    agg_sorted['OPERATION_UNIT'] = agg_sorted['CUM_TOTAL_UNIT'] / (totals[0] + 1e-30)
    agg_sorted['OPERATION_DOLLAR'] = agg_sorted['CUM_TOTAL_DOLLAR'] / (totals[1] + 1e-30)
    agg_sorted['CATCH_BAD_UNIT'] = agg_sorted['CUM_BAD_UNIT'] / (totals[2] + 1e-30)
    agg_sorted['CATCH_BAD_DOLLAR'] = agg_sorted['CUM_BAD_DOLLAR'] / (totals[3] + 1e-30)
    agg_sorted['HIT_BAD_UNIT'] = agg_sorted['CUM_BAD_UNIT'] / (agg_sorted['CUM_TOTAL_UNIT'] + 1e-30)
    agg_sorted['HIT_BAD_DOLLAR'] = agg_sorted['CUM_BAD_DOLLAR'] / (agg_sorted['CUM_TOTAL_DOLLAR'] + 1e-30)
    return_columns.extend(['CATCH_BAD_UNIT', 'CATCH_BAD_DOLLAR', 'HIT_BAD_UNIT', 'HIT_BAD_DOLLAR',
                           'OPERATION_UNIT', 'OPERATION_DOLLAR'])
    catch_variables_totals = zip(variables[4:], totals[4:])
    for catch_variable, catch_total in catch_variables_totals:
        agg_sorted['CATCH_' + catch_variable.upper()] = agg_sorted['CUM_' + catch_variable.upper()] / (
            catch_total + 1e-30)
        return_columns.append('CATCH_' + catch_variable.upper())
    return agg_sorted[return_columns]


def performance(data, bad=None, unit=None, dollar=None, score=None,
                maximum=None, minimum=None, higher_worse=True,
                catch_variables=None, score_interval=0.01, opt_interval=0.01):
    if score not in data:
        print "score variable cannot be found in the given dataset"
        return None

    if bad not in data:
        print "bad variable cannot be found in the given dataset"
        return None

    if not unit or unit == '1':
        unit = 1
    if unit != 1 and unit not in data:
        print "unit weight cannot be found in the given dataset"
        return None
    elif unit == 1:
        data['dummy_unit'] = 1
        unit = 'dummy_unit'
    data[unit] = data[unit].fillna(0)
    data['BAD_UNIT'] = data[unit] * (data['bad'] == 1)
    total_unit = data[unit].sum()
    total_bad_unit = data['BAD_UNIT'].sum()

    if not dollar or dollar == '1':
        dollar = 1
    if dollar != 1 and dollar not in data:
        print "dollar weight cannot be found in the given dataset"
        return None
    elif dollar == 1:
        data['dummy_dollar'] = 1
        dollar = 'dummy_dollar'
    data[dollar] = data[dollar].fillna(0)
    data['BAD_DOLLAR'] = data[dollar] * (data['bad'] == 1)
    total_dollar = data[dollar].sum()
    total_bad_dollar = data['BAD_DOLLAR'].sum()

    if catch_variables is None:
        catch_variables = []
    total_catch = {}
    for catch_variable in catch_variables:
        data[catch_variable] = data[catch_variable].fillna(0)
        data[catch_variable] = data[catch_variable] * data[unit]
        total_catch[catch_variable] = data[catch_variable].sum()

    if maximum is None:
        maximum = data[score].max()
    else:
        data[score] = data[score].apply(lambda x: maximum if x > maximum else x)

    if minimum is None:
        minimum = data[score].min()
    else:
        data[score] = data[score].apply(lambda x: minimum if x < minimum else x)

    new_score = 'TEMP_NORMALIZED_SCORE'
    new_rank = 'TEMP_RANK'
    data[new_score] = data[score].apply(
        lambda x: np.floor(x * 10000) * 0.0001 if higher_worse else np.ceil(x * 10000) * 0.0001)
    lower_worse = not higher_worse
    data[new_rank] = data[new_score].rank(method='min', ascending=lower_worse)

    sorted_data = data.sort(columns=new_rank)
    sorted_data['CUM_TOTAL_UNIT'] = sorted_data[unit].cumsum(axis=0)
    sorted_data['CUM_TOTAL_DOLLAR'] = sorted_data[dollar].cumsum(axis=0)
    sorted_data['OPERATION_UNIT'] = sorted_data['CUM_TOTAL_UNIT'] / (total_unit + 1e-30)
    sorted_data['OPERATION_DOLLAR'] = sorted_data['CUM_TOTAL_DOLLAR'] / (total_dollar + 1e-30)

    aggregation_variables = [unit, dollar, 'BAD_UNIT', 'BAD_DOLLAR']
    aggregation_variables.extend(catch_variables)
    aggregation_totals = [total_unit, total_dollar, total_bad_unit, total_bad_dollar]
    for catch_variable in catch_variables:
        aggregation_totals.append(total_catch[catch_variable])

    agg_score = 'TEMP_AGGREGATE_SCORE'
    sorted_data[agg_score] = np.floor(sorted_data[new_score] * 1.0 / score_interval) * score_interval if higher_worse \
        else np.ceil(sorted_data[new_score] * 1.0 / score_interval) * score_interval
    score_grouped = sorted_data.groupby(agg_score)
    score_aggregation = aggregation(score_grouped, aggregation_variables, aggregation_totals, agg_score, higher_worse)

    agg_opt_unit = 'TEMP_AGGREGATE_OPT_UNIT'
    sorted_data[agg_opt_unit] = np.ceil(sorted_data['OPERATION_UNIT'] * 1.0 / opt_interval) * opt_interval
    opt_unit_grouped = sorted_data.groupby(agg_opt_unit)
    opt_unit_aggregation = aggregation(opt_unit_grouped, aggregation_variables, aggregation_totals, agg_opt_unit, False)

    agg_opt_dollar = 'TEMP_AGGREGATE_OPT_DOLLAR'
    sorted_data[agg_opt_dollar] = np.ceil(sorted_data['OPERATION_DOLLAR'] * 1.0 / opt_interval) * opt_interval
    opt_dollar_grouped = sorted_data.groupby(agg_opt_dollar)
    opt_dollar_aggregation = aggregation(opt_dollar_grouped, aggregation_variables, aggregation_totals, agg_opt_dollar,
                                         False)

    return score_aggregation, opt_unit_aggregation, opt_dollar_aggregation

def operation_performance(data, score, weights=['dummy'], bads=['is_bad'], groups=['window_name'],
                          catches=[], points=100., rank_base='dummy') :
    raw = data[[score] + weights + bads + groups + catches]
    score_rank = score + "_rank"
    # Assuming higher score indicates higher risk
    # TODO: wrong here
    raw[score_rank] = raw.groupby(groups)[score].transform(pd.Series.rank, method='min',
                                                           ascending=False, na_option='bottom')
    score_groups = groups + [score_rank]
    raw.sort(columns=score_groups, inplace=True)
    raw[score_rank] = raw.groupby(groups)[rank_base].cumsum() / raw.groupby(groups)[rank_base].transform(pd.Series.sum)
    raw[score_rank] = raw[score_rank].apply(lambda x: np.ceil(x * points) * (1.0 / points))

    bad_dummy_names = {}
    full_bads_dummy_names = []
    for bad in bads :
        bad_dummy = pd.get_dummies(raw[bad])
        bad_dummy.rename(columns=dict(zip(bad_dummy.columns,
                                          ["{}_{}".format(x[0], x[1])
                                           for x in zip([bad] * len(bad_dummy.columns), bad_dummy.columns)])),
                         inplace=True)
        raw = raw.merge(bad_dummy, left_index=True, right_index=True)
        bad_dummy_names[bad] = list(bad_dummy.columns)
        full_bads_dummy_names.extend(bad_dummy.columns)

    weights_bads_names = [x + '_' + y for x in weights for y in full_bads_dummy_names]
    weights_bads_raw = pd.concat([raw[full_bads_dummy_names].mul(raw[x], axis=0) for x in weights], axis=1)
    weights_bads_raw.columns = pd.Index(weights_bads_names)
    raw[weights_bads_names] = weights_bads_raw

    score_cut_offs = raw.groupby(score_groups)[score].min()
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    totals = sums.sum(level=-2) #TODO: what if there is no base level such as dataset, etc
    sums = sums.groupby(level=-2).cumsum()
    catch_rates = sums.divide(totals + 1e-20, axis=0)
    catch_rates.columns = [x + '_cr' for x in catch_rates.columns]
    hit_rate_list = []
    hit_rate_names = []
    for weight in weights :
        weight_bads_names = [weight + '_' + x for x in full_bads_dummy_names]
        hit_rate = sums[weight_bads_names].divide(sums[weight] + 1e-20, axis=0)
        hit_rate_list.append(hit_rate)
        hit_rate_names.extend([x + '_hr' for x in weight_bads_names])
    hit_rates = pd.concat(hit_rate_list, axis=1)
    hit_rates.columns = pd.Index(hit_rate_names)

    raw_output = pd.concat([score_cut_offs, sums, catch_rates, hit_rates], axis=1)
    raw_output.reset_index()
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
    raw[score].fillna(low, inplace=True)
    raw[score_rank] = raw[score].apply(lambda x: np.ceil(x * 1.0 / step) * step)

    bad_dummy_names = {}
    full_bads_dummy_names = []
    for bad in bads :
        bad_dummy = pd.get_dummies(raw[bad])
        bad_dummy.rename(columns=dict(zip(bad_dummy.columns,
                                          ["{}_{}".format(x[0], x[1])
                                           for x in zip([bad] * len(bad_dummy.columns), bad_dummy.columns)])),
                         inplace=True)
        raw = raw.merge(bad_dummy, left_index=True, right_index=True)
        bad_dummy_names[bad] = list(bad_dummy.columns)
        full_bads_dummy_names.extend(bad_dummy.columns)

    weights_bads_names = [x + '_' + y for x in weights for y in full_bads_dummy_names]
    weights_bads_raw = pd.concat([raw[full_bads_dummy_names].mul(raw[x], axis=0) for x in weights], axis=1)
    weights_bads_raw.columns = pd.Index(weights_bads_names)
    raw[weights_bads_names] = weights_bads_raw

    score_groups = groups + [score_rank]
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    sums.sort_index(ascending=False)
    totals = sums.sum(level=-2)
    sums = sums.groupby(level=-2).cumsum()
    catch_rates = sums.divide(totals + 1e-20, axis=0)
    catch_rates.columns = [x + '_cr' for x in catch_rates.columns]
    hit_rate_list = []
    hit_rate_names = []
    for weight in weights :
        weight_bads_names = [weight + '_' + x for x in full_bads_dummy_names]
        hit_rate = sums[weight_bads_names].divide(sums[weight] + 1e-20, axis=0)
        hit_rate_list.append(hit_rate)
        hit_rate_names.extend([x + '_hr' for x in weight_bads_names])
    hit_rates = pd.concat(hit_rate_list, axis=1)
    hit_rates.columns = pd.Index(hit_rate_names)

    raw_output = pd.concat([sums, catch_rates, hit_rates], axis=1)
    raw_output.reset_index()
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
            groups = options.group.split('|')
            for group in groups :
                groups[group] = [x.strip() for x in group.split(',')]

    delimiter = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm

    data_names = ["data_{}".format(x) for x in xrange(1, len(input_data)+1)]
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
    if add_windows :
        groups['windows'] = ['window_name']
        group_vars.add('window_name')
    group_vars = list(group_vars)

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

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
        if add_windows :
            cur_data['window_name'] = data_names[ind]
        cur_data['dummy'] = 1
        data_list.append(cur_data)
        logging.info("file {0} has been read : {1}".format(data_names[ind], filepath))
        ind += 1
    big_raw = pd.concat(data_list)[score_vars + weight_vars + bad_vars + catch_vars + group_vars]
    big_raw[weight_vars + bad_vars + catch_vars].fillna(0, inplace=True)

    # score auto-mapping. This step will map [0,1] ranged score to [0,1000], without touching minus scores.
    #mapped_scores = big_raw[score_vars] * ((big_raw[score_vars].max() <= 1) * 999 + 1)
    #big_raw[score_vars] = big_raw[score_vars][big_raw[score_vars] < 0].combine_first(mapped_scores)
    # or
    max_ind = big_raw[score_vars].max() <= 1
    big_raw[score_vars] = big_raw[score_vars].apply(lambda x: x * ((max_ind & (x > 0)) * 999 + 1), axis=1)

    operation_raw_list = []
    score_raw_list = []
    #TODO: empty groups need to be taken care of
    for group_name, score_var, weight_var in itertools.product(groups, score_vars, weight_vars) :
        operation_raw = operation_performance(data=big_raw, score=score_var,
                                              weights=weight_vars, bads=bad_vars,
                                              groups=groups[group_name], catches=catch_vars,
                                              points=point, rank_base=weight_var)
        score_raw = score_performance(data=big_raw, score=score_var, weights=weight_vars,
                                      bads=bad_vars, groups=groups[group_name], catches=catch_vars,
                                      low=low, step=step, high=high)
        operation_raw_list.append(operation_raw)
        score_raw_list.append(score_raw)
    full_operation_raws = pd.concat(operation_raw_list)
    full_score_raws = pd.concat(score_raw_list)
    #output_final_results(full_operation_results, full_operation_raws, full_score_results, full_score_raws)

    with pd.ExcelWriter(options.output+'.xlsx') as writer :
        full_operation_raws.to_excel(writer, sheet_name="operation_raw")
        full_score_raws.to_excel(writer, sheet_name="score_raw")


    # test code
    if False:
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


