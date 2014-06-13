import fnmatch
import json
from optparse import OptionParser
from collections import OrderedDict
import os
import sqlite3

__author__ = 'yijiliu'


def ignore_exception(ignored_exception=Exception, default_value=None):
    """ Decorator for ignoring exception from a function
    e.g.   @ignore_exception(DivideByZero)
    e.g.2. ignore_exception(DivideByZero)(Divide)(2/0)
    """
    def dec(function):
        def _dec(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except ignored_exception:
                return default_value
        return _dec
    return dec


class IterRows:
    def __init__(self, file_path, delimiter, valid_inds):
        self.file_handler = open(file_path)
        self.delimiter = delimiter
        self.valid_inds = valid_inds
        self.file_handler.readline()

    def __iter__(self):
        return self

    def next(self):
        line = self.file_handler.readline()
        if not line :
            self.file_handler.close()
            raise StopIteration
        parts = line.strip().split(self.delimiter)
        variable_values = [parts[i].strip() for i in xrange(len(parts)) if i in self.valid_inds]
        return tuple(variable_values)


def parse_type(type_string) :
    # this special type should contain "num, char, drop, keep" 4 types
    # the logic of keep / drop is : if keep, the drop all others, otherwise, drop listed.
    ikeeps = set()
    idrops = set()
    if os.path.exists(type_string) :
        lines = open(type_string).readlines()
        for line in lines:
            parts = [x.lower().strip() for x in line.split(',')]
            if "drop" in parts or 'd' in parts:
                idrops.add(parts[0])
            if "keep" in parts or 'k' in parts:
                ikeeps.add(parts[0])
    else :
        try :
            variables = json.loads(type_string.lower())
            for name in variables :
                if 'drop' in variables[name] or 'd' in variables[name] :
                    idrops.add(name)
                if 'keep' in variables[name] or 'k' in variables[name] :
                    ikeeps.add(name)
        except ValueError :
            pass
    if len(ikeeps) > 0 :
        idrops = set()
    return ikeeps, idrops


def findPatterns(base, patterns) :
    selected = set()
    for pattern in patterns :
        selected = selected.union(fnmatch.filter(base, pattern))
    return selected


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="key", action="store", type="string",
                      help="merging key variables separated with ','")
    parser.add_option("-m", "--merge", dest="data", action="store", type="string",
                      help="data files separated with ';'")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=,")
    parser.add_option("-t", "--type", dest="type", action="store", type="string",
                      help="optional variable type files / json strings in format var_name[,k[eep]|d[rop]] on each line. " +
                           "Multiple files should be separated with ';' in the same order of the input data files")
    parser.add_option("-s", "--base", dest="keep", action="store", type="string",
                      help="optional variable list for simple keeping, whilchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-x", "--excl", dest="drop", action="store", type="string",
                      help="optional variable list for simple exclusion, wildchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output merged filename")
    (options, args) = parser.parse_args()

    if options.key.find(',') >= 0 :
        keys = [x.lower() for x in options.key.split(',')]
    else :
        keys = [options.key.lower()]

    if options.dlm.startswith('x') :
        options.dlm = chr(int(options.dlm[1:]))

    data_files = options.data.split(';')
    type_strings = options.type.split(";") if options.type else None
    keep_strings = options.keep.split(';') if options.keep else None
    drop_strings = options.drop.split(';') if options.drop else None
    if type_strings is not None and len(type_strings) != len(data_files) :
        raise ValueError("The type list files must have 1-to-1 relationship with the dataset files")
    if keep_strings is not None and len(keep_strings) != len(data_files) :
        raise ValueError("The keep strings must have 1-to-1 relationship with the dataset files")
    if drop_strings is not None and len(drop_strings) != len(data_files) :
        raise ValueError("The drop strings must have 1-to-1 relationship with the dataset files")

    for data_file in data_files :
        if not os.path.exists(data_file) :
            raise IOError("file does not exist! {}".format(data_file))

    global_variable_set = set()
    file_headers = []
    for dind in xrange(len(data_files)) :
        data_file = data_files[dind]

        # The logic for keep/drop is :
        # 1. go through the type file
        #    if the type file has keep list, keep only the variables in the keep list, and drop ALL other.
        #    otherwise, if the type file has only drop list, drop those variables
        # 2. go through the command line input drop patterns
        #    the keep / drop lists from type file are modified.
        #    drop variables will be removed from earlier keep list, and add to the new drop list,
        #    or simply add to the existing drop list.
        # 3. go through the command line input keep patterns
        #    continue to modify the existing keep / drop lists
        #    keep variables will be removed from earlier drop list, OR add to the new/existing keep list
        # 4. Keep list, if any, will override the drop list at last
        if type_strings is not None :
            keeps, drops = parse_type(type_strings[dind])
        else :
            keeps = set()
            drops = set()

        with open(data_file) as fn :
            line = fn.readline()
            variable_names = [x.strip() for x in line.split(options.dlm)]
            variable_names_lower = [x.lower() for x in variable_names]
            if drop_strings is not None :
                drop_patterns = drop_strings[dind].lower().split(',')
                drop_variables = findPatterns(variable_names_lower, drop_patterns)
                drops = drops.union(drop_variables) if len(keeps) == 0 else drops
                keeps = keeps.difference(drop_variables)
            if keep_strings is not None :
                keep_patterns = keep_strings[dind].lower().split(',')
                keep_variables = findPatterns(variable_names_lower, keep_patterns)
                keeps = keeps.union(keep_variables) if len(drops) == 0 else keeps
                drops = drops.difference(keep_variables)

            new_variable_names = OrderedDict()
            for ind in xrange(len(variable_names)) :
                variable_name = variable_names[ind]
                if ((len(drops) > 0 and variable_name.lower() not in drops)
                        or (len(keeps) > 0 and variable_name.lower() in keeps)
                        or (len(drops) == 0 and len(keeps) == 0)
                        or variable_name.lower() in keys):
                    if variable_name.lower() in keys :
                        new_variable_names[ind] = variable_name
                    elif variable_name.lower() not in global_variable_set :
                        new_variable_names[ind] = variable_name
                        global_variable_set.add(variable_name.lower())
                    else :
                        ext_variable_name = variable_name + "_dup"
                        while ext_variable_name.lower() in global_variable_set :
                            ext_variable_name += "_dup"
                        new_variable_names[ind] = ext_variable_name
                        global_variable_set.add(ext_variable_name.lower())
            file_headers.append(new_variable_names)


    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    select_variables = ""
    from_clause = ""
    output_variables = []
    for hind in xrange(len(file_headers)) :
        variable_names = file_headers[hind]
        variabls_definition = ",".join(["{0} text".format(variable_names[ind]) for ind in variable_names])
        table_constraint = "primary key ({})".format(",".join(keys))
        create_sql = "create table dataset_{0} ({1}, {2});".format(hind, variabls_definition, table_constraint)
        print create_sql
        cur.execute(create_sql)
        conn.commit()
        the_iter = IterRows(data_files[hind], options.dlm, variable_names)
        cur.executemany("insert into dataset_{0} values ({1});".format(hind, ",".join("?" * len(variable_names))), the_iter)
        conn.commit()
        cur.execute('select * from dataset_{};'.format(hind))
        row = cur.fetchone()
        print "dataset_{}".format(hind), row
        if hind > 0 :
            from_clause += "join "
        else :
            select_variables += ",".join(["d0.{}".format(key) for key in keys])
            output_variables.extend(keys)
        select_variables += ","
        select_variables += ",".join(["d{0}.{1}".format(hind, variable_names[ind]) for ind in variable_names if variable_names[ind] not in keys])
        output_variables.extend([variable_names[ind] for ind in variable_names if variable_names[ind] not in keys])
        from_clause += "dataset_{0} as d{0} ".format(hind)
        if hind > 0 :
            from_clause += "on " + "and ".join(["d0.{1} = d{0}.{1} ".format(hind, key) for key in keys])

    print "create table dataset_merge as select {0} from {1};".format(select_variables, from_clause)
    cur.execute("create table dataset_merge as select {0} from {1};".format(select_variables, from_clause))
    conn.commit()
    cur.execute('select * from dataset_merge')
    fout = open(options.output, 'w')
    fout.write(options.dlm.join(output_variables))
    fout.write("\n")
    while True:
        row = cur.fetchone()
        if not row :
            break
        #print "dataset_merge", row
        fout.write(options.dlm.join([str(x) for x in row]))
        fout.write("\n")
    fout.close()
    cur.close()
    conn.close()

