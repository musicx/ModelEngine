import json
import logging
import itertools
import collections
from optparse import OptionParser
import os
import pandas as pd
import numpy as np
import bokeh.plotting as bplt
from bokeh.objects import Range1d, HoverTool, ColumnDataSource

__author__ = 'yijiliu'

PREDEFINE_COLORS = ['blue', 'red', 'green', 'yellow', 'steelblue', 'orangered', 'springgreen', 'gold']


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


def operation_performance(data, score, weights=['dummy_weight'], bads=['is_bad'], groups=['window_name'],
                          catches=[], points=100., rank_base='dummy_weight') :
    raw = data[[score] + weights + bads + groups + catches]
    score_rank = "cut_off"
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
    score_cut_offs.name = 'cut_off_score'
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    totals = sums.sum(level=-2)       # TODO: what if there is more levels
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

def score_performance(data, score, weights=['dummy_weight'], bads=['is_bad'], groups=['window_name'],
                      catches=[], low=0, step=5, high=1000):
    raw = data[[score] + weights + bads + groups + catches]
    score_rank = "cut_off"
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
    parser.add_option("-w", "--wgt", dest="wgt", action="store", type="string", default="dummy_weight",
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
            weight_vars.append("dummy_weight")

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
            weight_vars.append('dummy_weight')

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
        cur_data['dummy_weight'] = 1
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

    pivot_index_name = ['cut_off']
    pivot_column_name = ['score_name']
    pivot_bad_rate_names = ['{}|{}|1|{}'.format(x[2], x[0], x[1])
                            for x in itertools.product(bad_vars, ['catch_rate', 'hit_rate'], weight_vars)]
    pivot_catch_names = ['{}|catch_rate'.format(x) for x in catch_vars]
    pivot_pop_names = ['{}|catch_rate'.format(x) for x in weight_vars]
    pivot_base_names = pivot_index_name + pivot_column_name + pivot_bad_rate_names + pivot_catch_names + pivot_pop_names
    writer = pd.ExcelWriter(options.out+'.xlsx')

    bplt.output_file(options.out+'_gainchart.html', title=options.out+' gainchart report')
    TOOLS = "pan, wheel_zoom, box_zoom, reset, hover, previewsave"
    has_drawed_something = False
    operation_x_range = Range1d(start=-0.1, end=1.1)
    score_x_range = Range1d(start=1010, end=-10)
    common_y_range = Range1d(start=-0.1, end=1.1)

    for group_name in groups :
        pivot_raw_columns = groups[group_name] + pivot_base_names
        group_operation_raws = pd.concat(operation_raw_list[group_name])[pivot_raw_columns + ['rank_base']]
        group_score_raws = pd.concat(score_raw_list[group_name])[pivot_raw_columns]

        #group_operation_raws['rank_base'] = group_operation_raws['rank_base'].apply(
        #    lambda x : "ranked based on {}".format(x)
        #)

        if len(weight_vars) > 1 and len(bad_vars) > 1 :
            bad_rate_show_names = ["{0}_rate of {1} based on {2}".format(x[1], x[0], x[2])
                                   for x in itertools.product(bad_vars, ['catch', 'hit'], weight_vars)]
        elif len(weight_vars) > 1 and len(bad_vars) == 1 :
            bad_rate_show_names = ["{0}_rate based on {1}".format(x[0], x[1])
                                   for x in itertools.product(['catch', 'hit'], weight_vars)]
        elif len(weight_vars) == 1 and len(bad_vars) > 1 :
            bad_rate_show_names = ["{0}_rate of {1}".format(x[1], x[0])
                                   for x in itertools.product(bad_vars, ['catch', 'hit'])]
        else :
            bad_rate_show_names = ['catch_rate', 'hit_rate']
        bad_rate_rename_map = dict(zip(pivot_bad_rate_names, bad_rate_show_names))
        catch_rename_map = dict(zip(pivot_catch_names, ['{} catch_rate'.format(x) for x in catch_vars]))
        pop_rename_map = dict(zip(pivot_pop_names, ['{} wise operation_point'.format(x) for x in weight_vars]))

        group_operation_raws.to_csv(options.out + '_operation_raw.csv', index=False)
        group_score_raws.to_csv(options.out + '_score_raw.csv', index=False)

        for name_maps in [bad_rate_rename_map, catch_rename_map, pop_rename_map] :
            group_operation_raws.rename(columns=name_maps, inplace=True)
            group_score_raws.rename(columns=name_maps, inplace=True)

        group_operation_pivot = pd.pivot_table(group_operation_raws, index=['cut_off'],
                                               columns=['rank_base']+groups[group_name]+['score_name'])
        group_score_pivot = pd.pivot_table(group_score_raws, index=['cut_off'],
                                           columns=groups[group_name]+['score_name'])
        group_score_pivot.sort_index(ascending=False, inplace=True)

        group_operation_pivot.fillna(method='pad', inplace=True)
        group_score_pivot.fillna(method='pad', inplace=True)

        group_operation_pivot.to_csv(options.out + '_operation_pivot.csv')
        group_score_pivot.to_csv(options.out + '_score_pivot.csv')

        group_operation_pivot.index.name = None
        group_score_pivot.index.name = None
        excel_operation_columns_names = list(group_operation_pivot.columns.names)
        group_operation_columns_names_backup = list(group_operation_pivot.columns.names)
        excel_operation_columns_names.pop()
        excel_operation_columns_names.append('cut_off')
        excel_score_columns_names = list(group_score_pivot.columns.names)
        group_score_columns_names_backup = list(group_score_pivot.columns.names)
        excel_score_columns_names.pop()
        excel_score_columns_names.append('cut_off')
        group_operation_pivot.columns.names = excel_operation_columns_names
        group_score_pivot.columns.names = excel_score_columns_names

        if group_name == "windows" :
            group_sheet_name = ""
        else :
            group_names_for_sheet = [x for x in groups[group_name] if x != 'window_name']
            group_sheet_name = ','.join(group_names_for_sheet) + ' '
        try :
            group_operation_pivot.to_excel(writer, sheet_name=group_sheet_name + "operation_pivot")
            group_score_pivot.to_excel(writer, sheet_name=group_sheet_name + "score_pivot")
            group_operation_raws.to_excel(writer, sheet_name=group_sheet_name + "operation_raw", index=False)
            group_score_raws.to_excel(writer, sheet_name=group_sheet_name + "score_raw", index=False)
        except RuntimeError as e :
            logging.error(e.message)

        group_operation_pivot.columns.names = group_operation_columns_names_backup
        group_score_pivot.columns.names = group_score_columns_names_backup

        # TODO: for x in product(group, bad, weight, (rank_base + 1(score))),
        # draw score * window * (group - 1) lines for the catch_rate,
        # hover the hit rate and other rates / operation points
        for bad_name, weight_name in itertools.product(bad_vars, weight_vars) :
            catch_rate_name = bad_rate_rename_map['{}|{}|1|catch_rate'.format(weight_name, bad_name)]
            hit_rate_name = bad_rate_rename_map['{}|{}|1|hit_rate'.format(weight_name, bad_name)]
            chart_columns_names = [catch_rate_name, hit_rate_name] + catch_rename_map.values() + pop_rename_map.values()

            for base_name in weight_vars :
                if has_drawed_something:
                    bplt.figure()
                else :
                    has_drawed_something = True
                bplt.hold()

                # replace because    it will have group variables as multiple levels in this position \|/
                #draw_data = group_operation_pivot.loc[:, pd.IndexSlice[chart_columns_names, base_name, :, :]]
                #draw_data = draw_data.stack(level=1).reset_index(level=1, drop=True)
                draw_data = group_operation_pivot.xs(base_name, level='rank_base', axis=1)[chart_columns_names]

                data_x = list(draw_data.index.values)
                try:
                    line_size = len(draw_data[catch_rate_name].columns)
                except AttributeError as e:
                    line_size = 1

                for line_ind in xrange(line_size):
                    if line_size > 1:
                        data_y = list(draw_data[catch_rate_name].iloc[:, line_ind].values)
                        line_names = list(draw_data[catch_rate_name].iloc[:, line_ind].name)
                    else :
                        data_y = list(draw_data[catch_rate_name].values)
                        line_names = list(draw_data[catch_rate_name].name)  # TODO: not sure if [1:] should be removed
                    column_data = {'hit_rate': list(draw_data.loc[:, tuple([hit_rate_name] + line_names)].values)}
                    catch_lists = {}
                    hover_tips = [("catch rate", "$y"), ("hit rate", "@hit_rate")]
                    for catch_rename_name in catch_rename_map.values() :
                        column_data[catch_rename_name.replace(' ', '_')] = list(draw_data.loc[:, tuple([catch_rename_name] + line_names)].values)
                        hover_tips.append((catch_rename_name, '@'+catch_rename_name.replace(' ', '_')))
                    pop_lists = []
                    for pop_rename_name in pop_rename_map.values():
                        column_data[pop_rename_name.replace(' ', '_')] = list(draw_data.loc[:, tuple([pop_rename_name] + line_names)].values)
                        hover_tips.append((pop_rename_name, '@'+pop_rename_name.replace(' ', '_')))
                    hover_source = ColumnDataSource(column_data)
                    bplt.scatter(data_x, data_y, source=hover_source, tools=TOOLS,
                                 size=7, fill_alpha=.5, color=PREDEFINE_COLORS[line_ind],
                                 legend=', '.join(line_names), title=catch_rate_name,
                                 #line_width=2, line_join='round',
                                 x_range=operation_x_range, y_range=common_y_range)
                    cur_hover = [t for t in bplt.curplot().tools if isinstance(t, HoverTool)][0]
                    cur_hover.tooltips = collections.OrderedDict(hover_tips)

    logging.info("start saving report...")
    writer.close()
    bplt.save()
    logging.info("all work done")



