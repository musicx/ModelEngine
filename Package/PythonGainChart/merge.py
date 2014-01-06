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
        variable_values = [parts[x].strip() for x in xrange(len(parts)) if x in self.valid_inds]
        return tuple(variable_values)


def parse_type(file_path) :
    # this special type should contain "num, char, drop, keep" 4 types
    # the logic of keep / drop is : if keep, the drop all others, otherwise, drop listed.
    special = {}
    ikeeps = set()
    idrops = set()
    with open(file_path) as fn:
        while True:
            line = fn.readline()
            if not line :
                break
            parts = [x.lower().strip() for x in line.split(',')]
            if "num" in parts :
                special[parts[0]] = "num"
            elif "char" in parts :
                special[parts[0]] = "char"
            if "drop" in parts :
                idrops.add(parts[0])
            if "keep" in parts :
                ikeeps.add(parts[0])
        if len(ikeeps) > 0 :
            idrops = set()
    return special, ikeeps, idrops


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="key", action="store", type="string",
                      help="merging key variables separated with ',")
    parser.add_option("-m", "--merge", dest="data", action="store", type="string",
                      help="data files separated with ';'")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=,")
    parser.add_option("-t", "--type", dest="type", action="store", type="string",
                      help="variable type / keep / drop list ordered same as data files. files separated with ';'")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output merged filename")
    (options, args) = parser.parse_args()

    if options.key.find(',') >= 0 :
        keys = [x.lower() for x in options.key.split(',')]
    else :
        keys = [options.key.lower()]

    if options.dlm.startswith('x') :
        options.dlm = chr(int(options.dlm[1:]))

    if options.type :
        type_files = options.type.split(";")
    else :
        type_files = None
    data_files = options.data.split(';')
    if type_files is not None and len(type_files) != len(data_files) :
        raise ValueError("The type list files must have 1-to-1 relationship with the dataset files")

    if type_files is not None :
        for type_file in type_files :
            if not os.path.exists(type_file) :
                raise IOError("file does not exist! {}".format(type_file))
    for data_file in data_files :
        if not os.path.exists(data_file) :
            raise IOError("file does not exist! {}".format(data_file))

    type_float = ignore_exception(ValueError, None)(float)
    known_error_value = {'', 'null', 'none', 'missing', '.', '_missing_'}

    variable_set = set()
    file_headers = []
    character_headers = []
    for dind in xrange(len(data_files)) :
        data_file = data_files[dind]
        if type_files is not None :
            specials, keeps, drops = parse_type(type_files[dind])
        else :
            specials = set()
            keeps = set()
            drops = set()
        with open(data_file) as fn :
            line = fn.readline()
            variable_names = [x.strip() for x in line.split(options.dlm)]
            new_variable_names = OrderedDict()
            for ind in xrange(len(variable_names)) :
                variable_name = variable_names[ind]
                if variable_name.lower() not in drops or (len(keeps) > 0 and variable_name in keeps):
                    if variable_name.lower() not in variable_set :
                        new_variable_names[ind] = variable_name
                        variable_set.add(variable_name.lower())
                if variable_name.lower() in keys :
                    new_variable_names[ind] = variable_name
            file_headers.append(new_variable_names)
            character_variables = {}
            numeric_variables = set()
            for ln in xrange(10000) :
                line = fn.readline()
                if not line :
                    break
                parts = [x.strip() for x in line.split(options.dlm)]
                for pind in xrange(len(parts)) :
                    if pind not in new_variable_names or pind in character_variables or pind in numeric_variables:
                        continue
                    if variable_names[pind].lower() in keys :
                        character_variables[pind] = variable_names[pind]
                        continue
                    if variable_names[pind].lower() in specials :
                        if specials[variable_names[pind].lower()] == "char" :
                            character_variables[pind] = variable_names[pind]
                        else :
                            numeric_variables.add(pind)
                        continue
                    parsed = type_float(parts[pind])
                    if parsed is None and parts[pind].lower() not in known_error_value:
                        character_variables[pind] = variable_names[pind]
            character_headers.append(character_variables)

    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    select_variables = ""
    from_clause = ""
    output_variables = []
    for hind in xrange(len(file_headers)) :
        variable_names = file_headers[hind]
        character_variables = character_headers[hind]
        variabls_definition = ",".join(["{0} {1}".format(variable_names[ind], "text" if ind in character_variables else "real") for ind in variable_names])
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
        print "dataset_merge", row
        fout.write(options.dlm.join([str(x) for x in row]))
        fout.write("\n")
    fout.close()
    cur.close()
    conn.close()

