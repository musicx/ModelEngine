import json
import logging
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
                           '{"input":["score.csv"],"score":["new_score"],"bad":["is_bad"],"weight":["unit","dollar"],"catch":["loss"],"group":{"group_name":["region"]}}')
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
    parser.add_option("-o", "--out", dest="out", action="store", type="string",
                      help="output performance file")
    (options, args) = parser.parse_args()

    nfloat = ignore_exception(ValueError, None)(float)

    config = {"input"  : [],
              "score"  : [],
              "bad"    : [],
              "weight" : [1,1],
              "catch"  : [],
              "group"  : {},
              "dlm"    : ',',
              "name"   : []}
    if options.json:
        sources = parse_json(options.json)
        if sources is None:
            logging.error("Error occurs during parsing the source json")
            exit()
        print sources

        if type(sources['input']) is list:
            config['input'].extend(sources['input'])
        elif type(sources['input']) is str:
            config['input'].append(sources['input'])
        else:
            logging.error("Error parsing input field in json")
            exit()
        
        if type(sources['score']) is list:
            config['score'].extend(sources['score'])
        elif type(sources['score']) is str:
            config['score'].append(sources['score'])
        else:
            logging.error("Error parsing score field in json")
            exit()
            
        if type(sources['bad']) is list:
            config['bad'].extend(sources['bad'])
        elif type(sources['bad']) is str:
            config['bad'].append(sources['bad'])
        else:
            logging.error("Error parsing bad field in json")
            exit()

        if "weight" in sources :
            if type(sources['weight']) is list:
                config['weight'].extend(sources['weight'])
            elif type(sources['weight']) is str:
                config['weight'].append(sources['weight'])
            else:
                logging.error("Error parsing weight field in json")
                exit()
        if len(config['weight']) == 0 :
            config['weight'].append("dummy")

        if "catch" in sources :
            if type(sources['catch']) is list:
                config['catch'].extend(sources['catch'])
            elif type(sources['catch']) is str:
                config['catch'].append(sources['catch'])
            else:
                logging.error("Error parsing catch field in json")
                exit()

        if "group" in sources :
            for group_name, group_vars in sources['group'] :
                if type(group_vars) is str:
                    config['group'][group_name] = [group_vars]
                elif type(group_vars) is list:
                    config['group'][group_name] = group_vars
                else :
                    logging.err("Error parsing group fileds in json")
                    exit()
    else :
        if not options.input:
            logging.error("Input datasets must be specified")
            exit()
        config['input'] = [x.strip() for x in options.input.split(',')]

        if not options.score:
            logging.error("Score variables must be specified")
            exit()
        config['score'] = [x.strip() for x in options.score.split(',')]

        if not options.bad:
            logging.error("Bad variables must be specified")
            exit()
        config['bad'] = [x.strip() for x in options.bad.split(',')]

        if options.wgt:
            config['weight'] = [x.strip() for x in options.wgt.split(',')]
        if len(config['weight']) == 0:
            config['weight'].append('dummy')

        if options.catch:
            config['catch'] = [x.strip() for x in options.catch.split(',')]

        if options.group:
            groups = options.group.split('|')
            for group in groups :
                config['group'][group] = [x.strip() for x in group.split(',')]

    config['dlm'] = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm

    config['name'] = ["data_{}".format(x) for x in xrange(1, len(config['input'])+1)]
    names = [x.strip() for x in options.name.split(',')]
    for ind in xrange(min(len(names), len(config['input']))) :
        config['name'][ind] = names[ind]

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    logging.info("program started...")
