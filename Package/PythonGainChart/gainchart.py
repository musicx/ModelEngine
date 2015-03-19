import json
import logging
import itertools
import collections
from optparse import OptionParser
import os
import pandas as pd
import numpy as np
import math
# import bokeh.plotting as bplt
# from bokeh.objects import Range1d, HoverTool, ColumnDataSource
# -i test_data1.csv,test_data2.csv -s clsn_scr -b is_unauth_collusion -w unit_weight,dollar_weight -c net_loss -p 1000 -o test -g segment

__author__ = 'yijiliu'

HIGHCHART_BASE_BEGIN = '''<!DOCTYPE html>
<html>
    <head>
        <script src="http://cdn.bootcss.com/jquery/2.1.1/jquery.min.js"></script>
        <script src="http://cdn.bootcss.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
        <link href="http://cdn.bootcss.com/bootstrap/3.2.0/css/bootstrap.min.css" rel="stylesheet">
        <script src="http://cdn.bootcss.com/highcharts/4.0.4/highcharts.js"></script>
        <style type="text/css">
        .sidebar {
            position: fixed;
            top: 0px;
            bottom: 0px;
            left: 0px;
            z-index: 1000;
            display: block;
            padding: 10px;
            overflow-x: hidden;
            overflow-y: auto;
            background-color: #F5F5F5;
            border-right: 1px solid #EEE;
        }
        </style>
        <title>Advanced Gain Chart</title>
    </head>
    <body>
    <div class="container-fluid">
    <div class="row">
    <div class="col-sm-3 col-md-2 sidebar">%s</div>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
'''

HIGHCHART_SIDE_TEMPLATE = '''%(btn_panel)s
%(lst_panel)s
'''

HIGHCHART_BUTTON_PANEL_TEMPLATE = '''<div class="panel panel-info">
  <div class="panel-heading">
    <h3 class="panel-title">%(title)s</h3>
  </div>
  <div class="panel-body">
    %(button)s
  </div>
</div>
'''

HIGHCHART_LIST_PANEL_TEMPLATE = '''<div class="panel panel-info">
  <div class="panel-heading">
    <h3 class="panel-title">%(base)s</h3>
  </div>
  <ul class="list-group">
    %(weight)s
  </ul>
</div>
'''

HIGHCHART_WGT_LIST_TEMPLATE = '<a id="%s" href="#" class="list-group-item%s">%s</a>\n'

HIGHCHART_BUTTON_TEMPLATE = '<button id="%s" type="button" class="btn btn-success">%s</button>'

HIGHCHART_DIV = '\t<div id="container%d" sytle="height: 400px; min-width: 600px" class="show">\n</div>\n'

HIGHCHART_DATA_TEMPLATE = '<script>\n%s\n'

HIGHCHART_FUNCTION_TEMPLATE = '''$(function () {
%s
});

$(document).ready(function() {
%s
});
</script>
'''

HIGHCHART_SERIES_TEMPLATE = '{\n\t\t\tdata: data_%d,\n\t\t\tname: \'%s\',\n\t\t\tturboThreshold:0\n\t\t}'

HIGHCHART_TOOLTIP_TEMPLATE = 's += \'%s: \' + this.point.%s + \'%%'

HIGHCHART_CHART_TEMPLATE = '''\t$('#container%(cid)d').highcharts({
        chart: {
            borderWidth: 1,
            zoomType: 'x',
            resetZoomButton: {
                position: {
                     align: 'left',
                    // verticalAlign: 'top', // by default
                    x: 10,
                    y: 10
                },
                relativeTo: 'chart'
            },
            marginRight: 80
        },

        title: {
            text : '%(title)s'
        },
        subtitle : {
            text: '%(subtitle)s'
        },

        tooltip: {
            shared: true,
            useHTML: true,
            headerFormat: '%(score)s <table> %(tiphead)s',
            pointFormat: %(tiptable)s,
            footerFormat: '</table>',
            crosshairs: true
        },

        yAxis: {
            title: {
                text: 'catch rate'
            },
            labels: {
                formatter: function() {
                    return this.value + '%%';
                }
            },
            gridLineWidth: 1,
            ceiling : 100,
            floor : 0
        },

        xAxis: {
            title: {
                text: '%(x)s'
            },
            %(suf)s
            reversed: %(reverse)s,
            gridLineWidth: 1
        },

        credits : {
            enabled: false
        },

        legend: {
            align : 'right',
            verticalAlign: 'middle',
            layout : 'vertical',
            borderWidth : 1,
            floating: true,
            backgroundColor: 'white'
        },

        series: [%(series)s]
    });
'''

HIGHCHART_FILTER_TOGGLE_TEMPLATE = '''    $('#%(bid)s').click(function() {
        var cs = [%(cids)s];
        var cl = cs.length;
        for (var i = 0; i < cl; i++) {
            if (! ($(cs[i]).hasClass('ls_chosen') || $(cs[i]).hasClass('bd_chosen') || $(cs[i]).hasClass('gp_chosen'))) {
                $(cs[i]).toggleClass('show');
                $(cs[i]).toggleClass('hidden');
            }
            $(cs[i]).toggleClass('ft_chosen');
        };
        $('#%(bid)s').toggleClass('btn-success');
        $('#%(bid)s').toggleClass('btn-default');
    });
'''

HIGHCHART_GROUP_TOGGLE_TEMPLATE = '''    $('#%(bid)s').click(function() {
        var cs = [%(cids)s];
        var cl = cs.length;
        for (var i = 0; i < cl; i++) {
            if (! ($(cs[i]).hasClass('ls_chosen') || $(cs[i]).hasClass('bd_chosen') || $(cs[i]).hasClass('ft_chosen'))) {
                $(cs[i]).toggleClass('show');
                $(cs[i]).toggleClass('hidden');
            }
            $(cs[i]).toggleClass('gp_chosen');
        };
        $('#%(bid)s').toggleClass('btn-success');
        $('#%(bid)s').toggleClass('btn-default');
    });
'''

HIGHCHART_BAD_TOGGLE_TEMPLATE = '''    $('#%(bid)s').click(function() {
        var cs = [%(cids)s];
        var cl = cs.length;
        for (var i = 0; i < cl; i++) {
            if (! ($(cs[i]).hasClass('ls_chosen') || $(cs[i]).hasClass('gp_chosen') || $(cs[i]).hasClass('ft_chosen'))) {
                $(cs[i]).toggleClass('show');
                $(cs[i]).toggleClass('hidden');
            }
            $(cs[i]).toggleClass('bd_chosen');
        };
        $('#%(bid)s').toggleClass('btn-success');
        $('#%(bid)s').toggleClass('btn-default');
    });
'''

HIGHCHART_LIST_TOGGLE_TEMPLATE = '''    $('#%(lid)s').click(function() {
        var cs = [%(cids)s];
        var cl = cs.length;
        for (var i = 0; i < cl; i++) {
            if (! ($(cs[i]).hasClass('gp_chosen') || $(cs[i]).hasClass('bd_chosen') || $(cs[i]).hasClass('ft_chosen'))) {
                $(cs[i]).toggleClass('show');
                $(cs[i]).toggleClass('hidden');
            }
            $(cs[i]).toggleClass('ls_chosen');
        };
        $('#%(lid)s').toggleClass('list-group-item-success');
    });
'''

HIGHCHART_READY_TOGGLE_TEMPLATE = '''
    var ics = [%s];
    var icl = ics.length;
    for (var i = 0; i < icl; i++) {
        $(ics[i]).toggleClass('show');
        $(ics[i]).toggleClass('hidden');
        $(ics[i]).toggleClass('ls_chosen');
    };
'''

HIGHCHART_BASE_END = '''\t</div>
\t</body>
</html>'''


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


def operation_performance(data, score, weights=None, bads=None, groups=None,
                          filters=None, catches=None, points=100., rank_base='dummy_weight') :
    if weights is None:
        weights = ['dummy_weight']
    if bads is None:
        bads = ['is_bad']
    if groups is None:
        groups = ['data_file_name']
    if catches is None:
        catches = []
    if filters is None :
        filters = ['all']
    if filters[0] == 'all' and len(filters) == 1 :
        raw = data[[score] + weights + bads + groups + catches]
    else :
        if filters[1] == '==' :
            raw = data[data[filters[0]] == filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '!=' :
            raw = data[data[filters[0]] != filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '>' :
            raw = data[data[filters[0]] > filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '>=' :
            raw = data[data[filters[0]] >= filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '<' :
            raw = data[data[filters[0]] < filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '<=' :
            raw = data[data[filters[0]] <= filters[2]][[score] + weights + bads + groups + catches]
        else :
            raw = data[[score] + weights + bads + groups + catches]

    score_rank = "cut_off"
    # Assuming higher score indicates higher risk
    raw[score_rank] = raw.groupby(groups)[score].transform(pd.Series.rank, method='min',
                                                           ascending=False, na_option='bottom')
    score_groups = groups + [score_rank]
    raw.sort(columns=score_groups, inplace=True)
    #rank_cumsum = pd.DataFrame(raw.groupby(score_groups)[rank_base].sum().groupby(level=-2).cumsum(),
    #                           columns=["rank_cumsum"]).reset_index()
    rank_cumsum = pd.DataFrame(raw.groupby(score_groups)[rank_base].sum().groupby(level=groups).cumsum().reset_index())
    rank_cumsum.columns = pd.Index(score_groups + ['rank_cumsum'])
    raw = raw.merge(rank_cumsum, how="left", on=score_groups)
    raw.loc[:, score_rank] = raw['rank_cumsum'] / raw.groupby(groups)[rank_base].transform(pd.Series.sum)
    raw.loc[:, score_rank] = raw[score_rank].apply(lambda x: math.ceil(x * points) * (1.0 / points))
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
    totals = sums.sum(level=groups)
    sums = sums.groupby(level=groups).cumsum()
    if type(totals.index) is pd.MultiIndex :
        catch_rates_parts = []
        for tidx in totals.index :
            catch_rates_part = sums.loc[tidx, :].divide(totals.loc[tidx, :] + 1e-20, axis=1)
            new_idx = [[x] for x in tidx]
            new_idx.append(catch_rates_part.index)
            catch_rates_part.index = pd.MultiIndex.from_product(new_idx)
            catch_rates_part.index.names = score_groups
            catch_rates_parts.append(catch_rates_part)
        catch_rates = pd.concat(catch_rates_parts)
    else :
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
    raw_output['filter_name'] = filters if filters == 'all' else ' '.join(filters)
    return raw_output

def score_performance(data, score, weights=None, bads=None, groups=None,
                      filters=None, catches=None, low=0, step=5, high=1000, cap_one=False):
    if weights is None:
        weights = ['dummy_weight']
    if bads is None:
        bads = ['is_bad']
    if groups is None:
        groups = ['data_file_name']
    if catches is None:
        catches = []
    if filters is None :
        filters = ['all']
    if filters[0] == 'all' and len(filters) == 1 :
        raw = data[[score] + weights + bads + groups + catches]
    else :
        if filters[1] == '==' :
            raw = data[data[filters[0]] == filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '!=' :
            raw = data[data[filters[0]] != filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '>' :
            raw = data[data[filters[0]] > filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '>=' :
            raw = data[data[filters[0]] >= filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '<' :
            raw = data[data[filters[0]] < filters[2]][[score] + weights + bads + groups + catches]
        elif filters[1] == '<=' :
            raw = data[data[filters[0]] <= filters[2]][[score] + weights + bads + groups + catches]
        else :
            raw = data[[score] + weights + bads + groups + catches]
    score_rank = "cut_off"
    # Assuming higher score indicates higher risk
    high = high * 0.001 if cap_one else high
    low = low * 0.001 if cap_one else low
    step = step * 0.001 if cap_one else step

    raw.loc[(raw[score] > high), score] = high
    raw.loc[(raw[score] < low), score] = low
    raw.fillna({score : low}, inplace=True)
    raw[score_rank] = raw[score].apply(lambda x: math.ceil(x * 1.0 / step) * step)

    bad_dummy_names = {}
    full_bads_dummy_names = []
    for bad in bads :
        bad_dummy = pd.get_dummies(raw[bad])
        bad_dummy.rename(columns=dict(zip(bad_dummy.columns,
                                          ["{}|{}".format(x[0], x[1])
                                           for x in zip([bad] * len(bad_dummy.columns), bad_dummy.columns)])),
                         inplace=True)
        #raw = raw.merge(bad_dummy, left_index=True, right_index=True)
        raw = pd.concat([raw, bad_dummy], axis=1)
        bad_dummy_names[bad] = list(bad_dummy.columns)
        full_bads_dummy_names.extend(bad_dummy.columns)

    weights_bads_names = [x + '|' + y for x in weights for y in full_bads_dummy_names]
    weights_bads_raw = pd.concat([raw[full_bads_dummy_names].mul(raw[x], axis=0) for x in weights], axis=1)
    weights_bads_raw.columns = pd.Index(weights_bads_names)
    raw[weights_bads_names] = weights_bads_raw

    score_groups = groups + [score_rank]
    sums = raw.groupby(score_groups)[weights_bads_names + weights + catches].sum()
    sums.sort_index(ascending=False, inplace=True)
    totals = sums.sum(level=groups)
    sums = sums.groupby(level=groups).cumsum()
    if type(totals.index) is pd.MultiIndex :
        catch_rates_parts = []
        sums.sort_index(inplace=True)
        totals.sort_index(inplace=True)
        for tidx in totals.index :
            catch_rates_part = sums.loc[tidx, :].divide(totals.loc[tidx, :] + 1e-20, axis=1)
            new_idx = [[x] for x in tidx]
            new_idx.append(catch_rates_part.index)
            catch_rates_part.index = pd.MultiIndex.from_product(new_idx)
            catch_rates_part.index.names = score_groups
            catch_rates_parts.append(catch_rates_part)
        catch_rates = pd.concat(catch_rates_parts)
        sums.sort_index(ascending=False, inplace=True)
        totals.sort_index(ascending=False, inplace=True)
    else :
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
    if cap_one:
        raw_output.loc[:, score_rank] = raw_output.loc[:, score_rank] * 1000
    raw_output.loc[:, score_rank] = raw_output.loc[:, score_rank].astype(int)
    raw_output['score_name'] = score
    raw_output['group_name'] = ",".join(groups)
    raw_output['filter_name'] = filters if filters == 'all' else ' '.join(filters)
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

def parse_filter(filter_string) :
    filter_string = filter_string.strip()
    if filter_string.lower().startswith('not ') :
        raise NotImplementedError('AND, OR, NOT are not supported yet')
    for keyword in [' and ', ' or ', ' not '] :
        if filter_string.lower().find(keyword) >= 0 :
            raise NotImplementedError('AND, OR, NOT are not supported yet')

    func_string = ''
    for func_item in ['==', '!=', '>', '>=', '<', '<='] :
        if filter_string.find(func_item) > 0 :
            func_string = func_item
            break
    if func_string == '' :
        logging.error("Error parsing filters")

    key_string, value_string = [x.strip() for x in filter_string.split(func_string)]
    return key_string.lower(), func_string, value_string


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-j", "--json", dest="json", action="store", type="string",
                      help="raw string of scored files encoded with json or json file. example:\n" +
                           '{"input":["score.csv"],"score":["new_score"],"bad":["is_bad"],"weight":["unit","dollar"],' +
                           '"catch":["loss"],"group":[{"group_name":["region"]}],"filter":[{"filter_name":"flag==1"}]}')
    # json will cover all following options. but it will be overriden if specified in command line
    parser.add_option("-i", "--input", dest="input", action="store", type="string",
                      help="input datasets, separated with ','")
    parser.add_option("-e", "--head", dest="head", action="store", type="string",
                      help="header file path. if given, all input datasets share it. must use same separator as input")
    parser.add_option("-s", "--score", dest="score", action="store", type="string",
                      help="score variables, separated with ','")
    parser.add_option("-b", "--bad", dest="bad", action="store", type="string",
                      help="name of bad variables")
    parser.add_option("-w", "--wgt", dest="wgt", action="store", type="string",
                      help="names of weight variables, separated by ',', default is dummy")
    parser.add_option("-c", "--catch", dest="catch", action="store", type="string",
                      help="names of extra variables for catch rate calculation, separated with ','")
    parser.add_option("-g", "--group", dest="group", action="store", type="string",
                      help="optional, group combinations in format class_var1,class_var2|class_var3")
    parser.add_option("-f", "--filter", dest="filter", action="store", type="string",
                      help="optional, filter criteria in python format, seperate with '|', comparison is done as string, 'AND', 'OR' and 'NOT' are not supported")
    # following parts are optional
    parser.add_option("-l", "--log", dest="log", action="store", type="string",
                      help="log file, if not given, stdout is used")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string",
                      help="delimiter char, accept xASCII format, default=','")
    parser.add_option("-n", "--name", dest="name", action="store", type="string",
                      help="names of input datasets, separated with ','")
    parser.add_option("-o", "--out", dest="out", action="store", type="string",
                      help="output performance file, default='output")
    # following parts are for report control
    parser.add_option("-r", "--range", dest="range", action="store", type="string",
                      help="score capping and step check after auto-mapping scores to [0,1000], given as low,step,high. default is 0,5,1000")
    parser.add_option("-p", "--point", dest="point", action="store", type="float",
                      help="number of operation points in reports, default=100")
    (options, args) = parser.parse_args()

    nfloat = ignore_exception(ValueError, None)(float)

    input_data = []
    score_vars = []
    bad_vars = []
    weight_vars = []
    catch_vars = []
    groups = collections.OrderedDict()
    filters = collections.OrderedDict()
    data_names = []
    delimiter = None
    log_filename = None
    output_filename = None
    point = None
    low = None
    step = None
    high = None
    header_path = None

    if options.json:
        sources = parse_json(options.json)
        if sources is None:
            logging.error("Error occurs during parsing the source json")
            exit()
        print sources

        if type(sources['input']) is list:
            input_data.extend(sources['input'])
        elif type(sources['input']) in [str, unicode]:
            input_data.append(sources['input'])
        else:
            logging.error("Error parsing input field in json")
            exit()

        if "head" in sources :
            header_path = sources['head']

        if type(sources['score']) is list:
            score_vars.extend(sources['score'])
        elif type(sources['score']) in [str, unicode]:
            score_vars.append(sources['score'])
        else:
            logging.error("Error parsing score field in json")
            exit()

        if type(sources['bad']) is list:
            bad_vars.extend(sources['bad'])
        elif type(sources['bad']) in [str, unicode]:
            bad_vars.append(sources['bad'])
        else:
            logging.error("Error parsing bad field in json")
            exit()

        if "weight" in sources :
            if type(sources['weight']) is list:
                weight_vars.extend(sources['weight'])
            elif type(sources['weight']) in [str, unicode]:
                weight_vars.append(sources['weight'])
            else:
                logging.error("Error parsing weight field in json")
                exit()
        if len(weight_vars) == 0 :
            weight_vars.append("dummy_weight")

        if "catch" in sources :
            if type(sources['catch']) is list:
                catch_vars.extend(sources['catch'])
            elif type(sources['catch']) is [str, unicode]:
                catch_vars.append(sources['catch'])
            else:
                logging.error("Error parsing catch field in json")
                exit()

        if len(input_data) > 1 :
            groups['data files'] = ['data_file_name']
        if "group" in sources :
            if type(sources['group']) is list :
                group_list = sources['group']
            else :
                group_list = [sources['group']]
            for group_item in group_list :
                for group_name, group_vars in group_item.iteritems() :
                    if type(group_vars) in [str, unicode]:
                        groups[group_name] = [group_vars]
                    elif type(group_vars) is list:
                        groups[group_name] = group_vars
                    else :
                        logging.error("Error parsing group fileds in json")
                        exit()

        filters['all'] = 'all'
        if "filter" in sources :
            if type(sources['filter']) is list :
                filter_list = sources['filter']
            else :
                filter_list = [sources['filter']]
            for filter_item in filter_list :
                for filter_name, filter_content in filter_item.iteritems() :
                    filters[filter_name] = parse_filter(filter_content)

        if 'delimiter' in sources :
            delimiter = chr(int(sources['delimiter'][1:])) if sources['delimiter'].startswith('x') else sources['delimiter']

        if 'log' in sources :
            log_filename = sources['log']

        if 'output' in sources:
            output_filename = sources['output']

        if 'point' in sources :
            point = nfloat(sources['point'])

        if 'high' in sources:
            high = nfloat(sources['high'])

        if 'step' in sources:
            step = nfloat(sources['step'])

        if 'low' in sources:
            low = nfloat(sources['low'])

    if not options.input and len(input_data) == 0:
        logging.error("Input datasets must be specified")
        exit()
    elif options.input:
        input_data = [x.strip() for x in options.input.split(',')]
    input_data = [x for x in input_data if x != '']

    if delimiter is None:
        delimiter = ','
    if options.dlm:
        delimiter = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm

    if options.head :
        header_path = options.head
    if header_path :
        header_line = open(header_path).readline().strip()
        headers = [x.lower() for x in header_line.split(delimiter)]

    if not options.score and len(score_vars) == 0:
        logging.error("Score variables must be specified")
        exit()
    elif options.score :
        score_vars = [x.strip() for x in options.score.split(',')]
    score_vars = list(set([x.lower() for x in score_vars if x != '']))

    if not options.bad and len(bad_vars) == 0:
        logging.error("Bad variables must be specified")
        exit()
    elif options.bad:
        bad_vars = [x.strip() for x in options.bad.split(',')]
    bad_vars = list(set([x.lower() for x in bad_vars if x != '']))

    if options.wgt:
        weight_vars = [x.strip() for x in options.wgt.split(',')]
    weight_vars = list(set([x.lower() for x in weight_vars if x != '']))
    if len(weight_vars) == 0:
        weight_vars.append('dummy_weight')

    if options.catch:
        catch_vars = [x.strip() for x in options.catch.split(',')]
    catch_vars = list(set([x.lower() for x in catch_vars if x != '']))

    if len(groups) > 0 and options.group :
        groups = collections.OrderedDict()
        if len(input_data) > 1 :
            groups['data files'] = ['data_file_name']
        group_names = options.group.split('|')
        for group_name in group_names :
            groups[group_name] = [x.lower().strip() for x in group_name.split(',')]
    elif len(groups) == 0 :
        if len(input_data) > 1 :
            groups['data files'] = ['data_file_name']
        if options.group:
            group_names = options.group.split('|')
            for group_name in group_names :
                groups[group_name] = [x.lower().strip() for x in group_name.split(',')]
    if len(groups) == 0 :
        groups['data files'] = ['data_file_name']

    if len(filters) > 0 and options.filter :
        filters = collections.OrderedDict()
        filters['all'] = ['all']
        filter_names = options.filter.split('|')
        for filter_name in filter_names :
            key_string, func_string, value_string = parse_filter(filter_name)
            filters["%s %s %s" % (key_string, func_string, value_string)] = (key_string, func_string, value_string)
    elif len(filters) == 0 :
        filters['all'] = ['all']
        if options.filter :
            filter_names = options.filter.split('|')
            for filter_name in filter_names :
                key_string, func_string, value_string = parse_filter(filter_name)
                filters["%s %s %s" % (key_string, func_string, value_string)] = (key_string, func_string, value_string)

    if output_filename is None:
        output_filename = 'output'
    if options.out:
        output_filename = options.out

    if options.log:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=options.log, filemode='w')
    elif log_filename is not None :
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(message)s',
                            filename=log_filename, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)-15s - %(levelname)s - %(message)s')

    if options.range :
        if len(options.range.split(',')) != 3:
            logging.error("Score range must be given in format low,step,high")
            exit()
        low, step, high = [nfloat(x) for x in options.range.split(',')]
    low = 0. if low is None else low
    step = 5. if step is None else step
    high = 1000. if high is None else high

    point = nfloat(options.point) if options.point is not None else point
    point = 100. if point is None else point

    data_names = ["data_{}".format(x) for x in xrange(1, len(input_data)+1)]
    if options.name :
        names = [x.strip().lower() for x in options.name.split(',')]
        for ind in xrange(min(len(names), len(input_data))) :
            data_names[ind] = names[ind]
    add_windows = True if 'data files' in groups else False

    group_vars = set()
    for group_name in groups :
        groups[group_name] = [x.lower() for x in groups[group_name] if x != '']
        if add_windows and group_name != 'data files':
            groups[group_name].append('data_file_name')
        group_vars = group_vars.union(groups[group_name])
    filter_vars = set()
    for filter_name in filters :
        if filter_name == 'all' :
            continue
        filter_vars.add(filters[filter_name][0])
    group_filter_vars = group_vars.union(filter_vars)
    group_vars = list(group_vars)
    filter_vars = list(filter_vars)
    group_filter_vars_type = dict([(x, np.object) for x in group_filter_vars])

    logging.info("program started...")

    data_list = []
    ind = 0
    for filepath in input_data:
        if not os.path.exists(filepath) :
            logging.error("File not exist! " + filepath)
            continue
        if header_path :
            cur_data = pd.read_csv(filepath, sep=delimiter, dtype=group_filter_vars_type,
                                   keep_default_na=False, na_values=['', '.'], names=headers,
                                   skipinitialspace=True, skiprows=1)
        else :
            cur_data = pd.read_csv(filepath, sep=delimiter, dtype=group_filter_vars_type,
                                   keep_default_na=False, na_values=['', '.'],
                                   skipinitialspace=True)
        cur_data.rename(columns=dict(zip(cur_data.columns, [x.lower().strip() for x in cur_data.columns])), inplace=True)

        if add_windows :
            cur_data['data_file_name'] = data_names[ind]
        cur_data['dummy_weight'] = 1
        data_list.append(cur_data)
        logging.info("file {0} has been read : {1}".format(data_names[ind], filepath))
        ind += 1
    big_raw = pd.concat(data_list)[score_vars + weight_vars + bad_vars + catch_vars + group_vars + filter_vars]
    logging.info("fill missing weight, bad, and catch variables with 0")
    big_raw.fillna(pd.Series([0] * len([weight_vars + bad_vars + catch_vars]),
                             index=[weight_vars + bad_vars + catch_vars]), inplace=True)
    big_raw.loc[:, bad_vars] = pd.DataFrame(big_raw.loc[:, bad_vars]).applymap(np.int)

    # score auto-mapping. This step will map [0,1] ranged score to [0,1000], without touching minus scores.
    # FIXED: this takes too much time. need to be removed
    max_ind = big_raw[score_vars].max() <= 1 + 1e-10
    # if max_ind.any() :
    #     logging.info("score scaling is found to be necessary, start...")
    #     big_raw[score_vars] = big_raw[score_vars].apply(lambda x: x * ((max_ind & (x > 0)) * 999 + 1), axis=1)

    operation_raw_list = {}
    score_raw_list = {}
    for filter_name, group_name, score_var in itertools.product(filters, groups, score_vars) :
        logging.info("handling '{0}' in group '{1}' with filter '{2}'".format(score_var, group_name, filter_name))
        for weight_var in weight_vars:
            operation_raw = operation_performance(data=big_raw, score=score_var, weights=weight_vars, bads=bad_vars,
                                                  groups=groups[group_name], filters=filters[filter_name],
                                                  catches=catch_vars, points=point, rank_base=weight_var)
            if filter_name not in operation_raw_list :
                operation_raw_list[filter_name] = collections.defaultdict(list)
            operation_raw_list[filter_name][group_name].append(operation_raw)
        logging.info("operation point based analysis done")
        score_raw = score_performance(data=big_raw, score=score_var, weights=weight_vars,
                                      bads=bad_vars, groups=groups[group_name], filters=filters[filter_name],
                                      catches=catch_vars, low=low, step=step, high=high, cap_one=max_ind[score_var])
        logging.info("score based analysis done, {} is a {} score".format(score_var, "NN" if max_ind[score_var] else "Normal"))
        if filter_name not in score_raw_list :
            score_raw_list[filter_name] = collections.defaultdict(list)
        score_raw_list[filter_name][group_name].append(score_raw)
    logging.info("all raw analysis is done, start creating pivot tables...")

    pivot_index_name = ['cut_off']
    pivot_column_name = ['score_name']
    pivot_bad_rate_names = ['{}|{}|1|{}'.format(x[2], x[0], x[1])
                            for x in itertools.product(bad_vars, ['catch_rate', 'hit_rate'], weight_vars)]
    pivot_catch_names = ['{}|catch_rate'.format(x) for x in catch_vars]
    pivot_pop_names = ['{}|catch_rate'.format(x) for x in weight_vars]
    pivot_base_names = pivot_index_name + pivot_column_name + pivot_bad_rate_names + pivot_catch_names + pivot_pop_names
    writer = pd.ExcelWriter(output_filename+'.xlsx')

    # initialization for bokeh usage
    # bplt.output_file(output_file+'_gainchart.html', title=output_file+' gainchart report')
    # TOOLS = "pan, wheel_zoom, box_zoom, resize, reset, hover, previewsave"
    # has_drawed_something = False
    # operation_x_range = Range1d(start=-0.04, end=1.04)
    # score_x_range = Range1d(start=1040, end=-40)
    # common_y_range = Range1d(start=-0.04, end=1.04)

    # initialization for highchart output
    hc_file = open(output_filename + '_charts.html', 'w')
    high_line_ind = 0
    high_container_ind = 0
    high_container_matrix = []
    high_div_string = {}
    high_side_string = []
    high_data_string = []
    high_chart_string = []
    high_filter_button_string = []
    high_group_button_string = []
    high_bad_button_string = []
    high_button_panel_string = []
    high_list_panel_string = []
    high_func_string = []

    for filter_name in filters:
        logging.info('creating pivot tables with filter : ' + filter_name)
        filter_sheet_name = '' if filter_name == 'all' else ''.join(filters[filter_name]) + ' '
        filter_subtitle = '' if filter_name == 'all' else ', ' + filter_name

        for group_name in groups :
            logging.info('creating pivot tables with group : ' + group_name)
            pivot_raw_columns = groups[group_name] + pivot_base_names
            group_operation_raws = pd.concat(operation_raw_list[filter_name][group_name])
            pivot_operation_raws = group_operation_raws[pivot_raw_columns + ['rank_base']]
            group_score_raws = pd.concat(score_raw_list[filter_name][group_name])
            pivot_score_raws = group_score_raws[pivot_raw_columns]

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

            group_operation_raws.to_csv(output_filename + '_operation_raw-{}-{}.csv'.format(''.join(filters[filter_name]), '-'.join(groups[group_name])), index=False)
            group_score_raws.to_csv(output_filename + '_score_raw-{}-{}.csv'.format(''.join(filters[filter_name]), '-'.join(groups[group_name])), index=False)
            logging.info("raw table saved to flat file for group '{}' with filter '{}'".format(group_name, filter_name))

            # sub = raw.loc[(raw['rank_base']=='unit_weight'), ['data_file_name', 'cut_off', 'score_name', 'unit_weight|is_unauth_collusion|1|catch_rate']]
            # line = sub.loc[(sub.data_file_name == 'data_1') & (sub.score_name == 'clsn_scr'), ['cut_off','unit_weight|is_unauth_collusion|1|catch_rate']]
            # data = '[' + ','.join(["[{:.2f},{:.4f}]".format(x[0], x[1]) for x in line.to_records(index=False)]) + ']'

            for name_maps in [bad_rate_rename_map, catch_rename_map, pop_rename_map] :
                pivot_operation_raws.rename(columns=name_maps, inplace=True)
                pivot_score_raws.rename(columns=name_maps, inplace=True)

            group_operation_pivot = pd.pivot_table(pivot_operation_raws, index=['cut_off'],
                                                   columns=['rank_base'] + groups[group_name] + ['score_name'])
            group_score_pivot = pd.pivot_table(pivot_score_raws, index=['cut_off'],
                                               columns=groups[group_name] + ['score_name'])
            group_score_pivot.sort_index(ascending=False, inplace=True)

            group_operation_pivot.fillna(method='pad', inplace=True)
            group_score_pivot.fillna(method='pad', inplace=True)
            group_operation_pivot.fillna(0, inplace=True)
            group_score_pivot.fillna(0, inplace=True)

            group_operation_pivot.index.name = None
            group_score_pivot.index.name = None

            group_operation_pivot.to_csv(output_filename + '_operation_pivot-{}-{}.csv'.format(''.join(filters[filter_name]), '-'.join(groups[group_name])))
            group_score_pivot.to_csv(output_filename + '_score_pivot-{}-{}.csv'.format(''.join(filters[filter_name]), '-'.join(groups[group_name])))
            logging.info("pivot table saved to flat file for group '{}' with filter '{}'".format(group_name, filter_name))

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

            if group_name == "data files" :
                group_sheet_name = ""
            else :
                group_names_for_sheet = [x for x in groups[group_name] if x != 'data_file_name']
                group_sheet_name = ','.join(group_names_for_sheet) + ' '
            sheet_name = filter_sheet_name + group_sheet_name

            op_sheet_name = (sheet_name + "operation_pivot") if len(sheet_name) <= 16 else (sheet_name + 'op')
            sp_sheet_name = (sheet_name + "score_pivot") if len(sheet_name) <= 20 else (sheet_name + 'sp')
            or_sheet_name = (sheet_name + "operation_raw") if len(sheet_name) <= 18 else (sheet_name + 'or')
            sr_sheet_name = (sheet_name + "score_raw") if len(sheet_name) <= 22 else (sheet_name + 'sr')

            try :
                group_operation_pivot.to_excel(writer, sheet_name=op_sheet_name[-31:])
                group_score_pivot.to_excel(writer, sheet_name=sp_sheet_name[-31:])
                group_operation_raws.to_excel(writer, sheet_name=or_sheet_name[-31:], index=False)
                group_score_raws.to_excel(writer, sheet_name=sr_sheet_name[-31:], index=False)
            except RuntimeError as e :
                logging.error(e.message)
            logging.info("excel sheets written for group '{}' with filter '{}'".format(group_name, filter_name))

            group_operation_pivot.columns.names = group_operation_columns_names_backup
            group_score_pivot.columns.names = group_score_columns_names_backup

            # draw carts using highcharts
            #high_tooltip_string = ["s += 'catch: ' + this.y + '%",
            #                       "s += 'hit: ' + this.point.hit_rate + '%"]
            high_tiptable_head = '<tr><th>line</th><th>&nbsp;catch&nbsp;</th><th>&nbsp;fpr&nbsp;</th>'
            high_tiptable_string = "'<tr><td style=\"color:{series.color}\">{series.name}&nbsp;:</td><td>&nbsp;{point.y}%&nbsp;</td><td>,&nbsp;{point.hit_rate}%&nbsp;</td>'"
            for catch_rename_name in catch_rename_map.values() :
                series_catch_name = catch_rename_name.replace(' catch_rate', '')
                #high_tooltip_string.append(HIGHCHART_TOOLTIP_TEMPLATE % (series_catch_name, series_catch_name))
                high_tiptable_head += '<th>&nbsp;%s&nbsp;</th>' % series_catch_name
                high_tiptable_string += " + '<td>,&nbsp;{point.%s}%%&nbsp;</td>'" % series_catch_name
            for pop_rename_name in pop_rename_map.values() :
                series_pop_name = pop_rename_name.replace(' wise operation_point', '_opt')
                if series_pop_name.lower().find('unit') >= 0 or series_pop_name.startswith('dummy') :
                    series_pop_short_name = '# opt'
                elif series_pop_name.lower().find('dol') >= 0:
                    series_pop_short_name = '$ opt'
                else :
                    series_pop_short_name = series_pop_name
                #high_tooltip_string.append(HIGHCHART_TOOLTIP_TEMPLATE % (series_pop_short_name, series_pop_name))
                high_tiptable_head += '<th>&nbsp;%s&nbsp;</th>' % series_pop_short_name
                high_tiptable_string += " + '<td>,&nbsp;{point.%s}%%&nbsp;</td>'" % series_pop_name
            #high_tooltip_string = '<br/>\';\n\t\t\t\t'.join(high_tooltip_string) + '\';'
            high_tiptable_head += "</tr>"
            high_tiptable_string += " + '</tr>'"
            high_tiptable_string = high_tiptable_string.replace('::', '_')

            # for x in product(group, bad, weight, (rank_base + 1(score))),
            # draw score * window * (group - 1) lines for the catch_rate,
            # hover the hit rate and other rates / operation points
            for bad_name, weight_name in itertools.product(bad_vars, weight_vars) :
                catch_rate_name = bad_rate_rename_map['{}|{}|1|catch_rate'.format(weight_name, bad_name)]
                hit_rate_name = bad_rate_rename_map['{}|{}|1|hit_rate'.format(weight_name, bad_name)]
                chart_columns_names = [catch_rate_name, hit_rate_name] + catch_rename_map.values() + pop_rename_map.values()

                for base_name in weight_vars :
                    # if has_drawed_something:
                    #     bplt.figure()
                    # else :
                    #     has_drawed_something = True
                    # bplt.hold()
                    high_container = {'cid' : high_container_ind,
                                      'bad' : bad_name,
                                      'weight' : weight_name,
                                      'base' : base_name,
                                      'group' : group_name,
                                      'filter' : filter_name}
                    high_container_matrix.append(high_container)

                    high_div_string[(filter_name, group_name, bad_name, base_name, weight_name)] = HIGHCHART_DIV % high_container_ind
                    chart_content = dict(cid=high_container_ind,
                                         title='%s wise catch rate' % weight_name,
                                         subtitle=bad_name + filter_subtitle,
                                         x='%s wise operation point' % base_name,
                                         #tooltip=high_tooltip_string,
                                         tiphead=high_tiptable_head,
                                         tiptable=high_tiptable_string,
                                         suf="labels: {formatter: function() {return this.value + '%';}},",
                                         reverse='false',
                                         score='')

                    # replace because    it will have group variables as multiple levels in this position \|/
                    #draw_data = group_operation_pivot.loc[:, pd.IndexSlice[chart_columns_names, base_name, :, :]]
                    #draw_data = draw_data.stack(level=1).reset_index(level=1, drop=True)
                    draw_data = group_operation_pivot.xs(base_name, level='rank_base', axis=1)[chart_columns_names]

                    try:
                        line_names_list = list(draw_data[catch_rate_name].columns)
                    except AttributeError as e:
                        line_names_list = list(draw_data[catch_rate_name].name)

                    high_series_string = []
                    for line_names_tuple in line_names_list :
                        line_names = list(line_names_tuple)
                        series_columns = [tuple([catch_rate_name] + line_names)]
                        series_columns += [tuple([hit_rate_name] + line_names)]
                        series_columns_names = ['y', 'hit_rate']
                        for catch_rename_name in catch_rename_map.values() :
                            series_columns.append(tuple([catch_rename_name] + line_names))
                            series_columns_names.append(catch_rename_name.replace(' catch_rate', ''))
                        for pop_rename_name in pop_rename_map.values() :
                            series_columns.append(tuple([pop_rename_name] + line_names))
                            series_columns_names.append(pop_rename_name.replace(' wise operation_point', '_opt'))
                        series_data = draw_data.loc[:, series_columns]
                        series_data.columns = series_columns_names
                        series_data.loc[:, "hit_rate"] = series_data.loc[:, "hit_rate"].apply(lambda h: (1 - h) / (h + 1e-10))
                        series_data.index.name = 'x'
                        series_data = series_data.reset_index().applymap(lambda x: "{:.2f}".format(float(x) * 100))
                        series_dict = series_data.transpose().to_dict().values()
                        series_string = json.dumps(series_dict)
                        high_data_string.append('var data_{0} = {1};\n'.format(high_line_ind, series_string.replace('"', '').replace('::', '_')))
                        legend_names = ['{}={}'.format(group_key, group_value) if group_key != 'data_file_name' else group_value
                                        for group_key, group_value in zip(groups[group_name], line_names[:-1])] + line_names[-1:]
                        high_series_string.append(HIGHCHART_SERIES_TEMPLATE % (high_line_ind, ', '.join(legend_names)))
                        high_line_ind += 1

                    chart_content['series'] = ', '.join(high_series_string)
                    high_chart_string.append(HIGHCHART_CHART_TEMPLATE % chart_content)
                    high_container_ind += 1

                    # line_ind = 0
                    # data_x = list(draw_data.index.values)
                    # color_map = bplt.brewer['Spectral'][len(line_names_list)]
                    # for line_names_tuple in line_names_list:
                    #     line_names = list(line_names_tuple)
                    #     data_y = list(draw_data[tuple([catch_rate_name] + line_names)].values)
                    #     column_data = {'hit_rate': list(draw_data.loc[:, tuple([hit_rate_name] + line_names)].values)}
                    #     catch_lists = {}
                    #     hover_tips = [("catch rate", "$y"), ("hit rate", "@hit_rate")]
                    #     for catch_rename_name in catch_rename_map.values() :
                    #         column_data[catch_rename_name.replace(' ', '_')] = list(draw_data.loc[:, tuple([catch_rename_name] + line_names)].values)
                    #         hover_tips.append((catch_rename_name.replace('catch_rate', 'catch'), '@'+catch_rename_name.replace(' ', '_')))
                    #     pop_lists = []
                    #     for pop_rename_name in pop_rename_map.values():
                    #         column_data[pop_rename_name.replace(' ', '_')] = list(draw_data.loc[:, tuple([pop_rename_name] + line_names)].values)
                    #         hover_tips.append((pop_rename_name.replace('wise operation_point', 'opt'), '@'+pop_rename_name.replace(' ', '_')))
                    #     hover_source = ColumnDataSource(column_data)
                    #     bplt.scatter(data_x, data_y, source=hover_source, tools=TOOLS,
                    #                  size=7, fill_alpha=.5, color=color_map[line_ind],
                    #                  legend=', '.join(line_names), title=catch_rate_name,
                    #                  plot_width=800, plot_height=450,
                    #                  #line_width=2, line_join='round',
                    #                  x_range=operation_x_range, y_range=common_y_range)
                    #     cur_hover = [t for t in bplt.curplot().tools if isinstance(t, HoverTool)][0]
                    #     cur_hover.tooltips = collections.OrderedDict(hover_tips)
                    #     bplt.legend().orientation = "bottom_right"
                    #     bplt.xaxis().axis_label = base_name + " operation points"
                    #     bplt.yaxis().axis_label = 'catch rate'
                    #     line_ind += 1

                high_container = {'cid' : high_container_ind,
                                  'bad' : bad_name,
                                  'weight' : weight_name,
                                  'base' : 'Model Score',
                                  'group' : group_name,
                                  'filter' : filter_name}
                high_container_matrix.append(high_container)

                high_div_string[(filter_name, group_name, bad_name, 'Model Score', weight_name)] = HIGHCHART_DIV % high_container_ind
                chart_content = {'cid': high_container_ind,
                                 'title': '%s wise catch rate' % weight_name,
                                 'subtitle': bad_name + filter_subtitle,
                                 'x': 'model score',
                                 #'tooltip' : "s += 'score: ' + this.x + '<br/>';\n\t\t\t\t" + high_tooltip_string,
                                 'tiphead' : high_tiptable_head,
                                 'tiptable' : high_tiptable_string,
                                 'suf': '',
                                 'score' : "<b>Score: {point.x}</b></br>",
                                 'reverse': 'true'}

                draw_data = group_score_pivot.loc[:, chart_columns_names]

                try:
                    line_names_list = list(draw_data[catch_rate_name].columns)
                except AttributeError as e:
                    line_names_list = list(draw_data[catch_rate_name].name)

                high_series_string = []
                for line_names_tuple in line_names_list :
                    line_names = list(line_names_tuple)
                    series_columns = [tuple([catch_rate_name] + line_names)]
                    series_columns += [tuple([hit_rate_name] + line_names)]
                    series_columns_names = ['y', 'hit_rate']
                    for catch_rename_name in catch_rename_map.values() :
                        series_columns.append(tuple([catch_rename_name] + line_names))
                        series_columns_names.append(catch_rename_name.replace(' catch_rate', ''))
                    for pop_rename_name in pop_rename_map.values() :
                        series_columns.append(tuple([pop_rename_name] + line_names))
                        series_columns_names.append(pop_rename_name.replace(' wise operation_point', '_opt'))
                    series_data = draw_data.loc[:, series_columns]
                    series_data.columns = series_columns_names
                    series_data.index.name = 'x'
                    series_data = series_data.applymap(lambda x: "{:.2f}".format(float(x) * 100)).sort_index().reset_index()
                    series_dict = series_data.transpose().to_dict().values()
                    series_string = json.dumps(series_dict)
                    high_data_string.append('var data_{0} = {1};\n'.format(high_line_ind, series_string.replace('"', '').replace('::', '_')))
                    legend_names = ['{}={}'.format(group_key, group_value) if group_key != 'data_file_name' else group_value
                                    for group_key, group_value in zip(groups[group_name], line_names[:-1])] + line_names[-1:]
                    high_series_string.append(HIGHCHART_SERIES_TEMPLATE % (high_line_ind, ', '.join(legend_names)))
                    high_line_ind += 1

                chart_content['series'] = ', '.join(high_series_string)
                high_chart_string.append(HIGHCHART_CHART_TEMPLATE % chart_content)
                high_container_ind += 1
            logging.info("highcharts created for group '{}' with filter '{}'".format(group_name, filter_name))

    logging.info("start saving excel report...")
    writer.close()

    logging.info("start writing chart html file...")
    high_container = pd.DataFrame(high_container_matrix)

    button_ind = 0
    panel_list_ind = 0

    if len(filters) > 1 :
        for filter_name in filters :
            high_filter_button_string.append(HIGHCHART_BUTTON_TEMPLATE % ('btn{}'.format(button_ind), filter_name))
            cids = list(high_container[high_container['filter'] == filter_name]['cid'])
            high_func_string.append(HIGHCHART_FILTER_TOGGLE_TEMPLATE % {'bid' : 'btn{}'.format(button_ind),
                                                                        'cids' : ','.join(["'#container%s'" % x for x in cids])})
            button_ind += 1
        high_button_panel_string.append(HIGHCHART_BUTTON_PANEL_TEMPLATE % {'title' : 'Filters',
                                                                           'button' : '\n'.join(high_filter_button_string)})

    if len(groups) > 1 :
        for group_name in groups :
            high_group_button_string.append(HIGHCHART_BUTTON_TEMPLATE % ('btn{}'.format(button_ind), group_name))
            cids = list(high_container[high_container['group'] == group_name]['cid'])
            high_func_string.append(HIGHCHART_GROUP_TOGGLE_TEMPLATE % {'bid' : 'btn{}'.format(button_ind),
                                                                       'cids' : ','.join(["'#container%s'" % x for x in cids])})
            button_ind += 1
        high_button_panel_string.append(HIGHCHART_BUTTON_PANEL_TEMPLATE % {'title' : 'Groups',
                                                                           'button' : '\n'.join(high_group_button_string)})
    if len(bad_vars) > 1 :
        for bad_name in bad_vars :
            high_bad_button_string.append(HIGHCHART_BUTTON_TEMPLATE % ('btn{}'.format(button_ind), bad_name))
            cids = list(high_container[high_container['bad'] == bad_name]['cid'])
            high_func_string.append(HIGHCHART_BAD_TOGGLE_TEMPLATE % {'bid' : 'btn{}'.format(button_ind),
                                                                     'cids' : ','.join(["'#container%s'" % x for x in cids])})
            button_ind += 1
        high_button_panel_string.append(HIGHCHART_BUTTON_PANEL_TEMPLATE % {'title' : 'Bad variables',
                                                                           'button' : '\n'.join(high_bad_button_string)})

    for base_name in weight_vars + ["Model Score"] :
        weight_list = ''
        for weight_name in weight_vars :
            if weight_name.lower().find('unit') >= 0 or weight_name.lower().startswith('dummy') :
                weight_short_name = 'Unit-wise'
            elif weight_name.lower().find('dol') >= 0 :
                weight_short_name = 'Dollar-wise'
            else :
                weight_short_name = weight_name
            success_ind = ' list-group-item-success' if weight_name == base_name else ''
            weight_list += HIGHCHART_WGT_LIST_TEMPLATE % ('lst{}'.format(panel_list_ind), success_ind, weight_short_name + " Catch Rate")
            cids = list(high_container[(high_container['base'] == base_name) & (high_container['weight'] == weight_name)]['cid'])
            high_func_string.append(HIGHCHART_LIST_TOGGLE_TEMPLATE % {'lid' : 'lst{}'.format(panel_list_ind),
                                                                      'cids' : ','.join(["'#container%s'" % x for x in cids])})
            panel_list_ind += 1
        if base_name.lower().find('unit') >= 0 or base_name.lower().startswith('dummy') :
            base_short_name = 'Unit-wise'
        elif base_name.lower().find('dol') >= 0 :
            base_short_name = 'Dollar-wise'
        else :
            base_short_name = base_name
        base_short_name = base_short_name + ' Operation Point' if base_short_name != 'Model Score' else base_short_name
        high_list_panel_string.append(HIGHCHART_LIST_PANEL_TEMPLATE % {'base' : base_short_name, 'weight': weight_list})

    cids = list(high_container[(high_container['base'] != high_container['weight'])]['cid'])
    high_func_string.append(HIGHCHART_READY_TOGGLE_TEMPLATE % (','.join(["'#container%s'" % x for x in cids])))

    high_side_string = HIGHCHART_SIDE_TEMPLATE % {'btn_panel' : '\n'.join(high_button_panel_string),
                                                  'lst_panel' : '\n'.join(high_list_panel_string)}

    hc_file.write(HIGHCHART_BASE_BEGIN % high_side_string)
    hc_file.write('\n'.join([high_div_string[x] for x in itertools.product(filters.keys(),
                                                                           groups.keys(),
                                                                           bad_vars,
                                                                           weight_vars + ["Model Score"],
                                                                           weight_vars)]))
    hc_file.write(HIGHCHART_DATA_TEMPLATE % '\n'.join(high_data_string))
    hc_file.write(HIGHCHART_FUNCTION_TEMPLATE % ('\n'.join(high_chart_string), '\n'.join(high_func_string)))
    hc_file.write(HIGHCHART_BASE_END)

    # bplt.save()
    hc_file.close()
    logging.info("all work done, check {0}.xlsx and {0}_charts.html".format(output_filename))



