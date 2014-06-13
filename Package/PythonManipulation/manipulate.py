import fnmatch
import json
from optparse import OptionParser
import os

__author__ = 'yijiliu'

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

def generateOutputName(filepath) :
    if filepath is None or filepath == '' :
        return filepath
    base, ext = os.path.splitext(filepath)
    return base + '_mani' + ext

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input", action="store", type="string",
                      help="input datasets separated with ';'")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output file names separated with ';', if not given, '_mani' will be appended by default")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=,")
    parser.add_option("-t", "--type", dest="type", action="store", type="string",
                      help="optional variable type files / json strings in format var_name[,k[eep]|d[rop]] on each line. " +
                           "Multiple files should be separated with ';' in the same order of the input data files")
    parser.add_option("-k", "--keep", dest="keep", action="store", type="string",
                      help="optional variable list for simple keeping, whilchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-x", "--excl", dest="drop", action="store", type="string",
                      help="optional variable list for simple exclusion, wildchar supported, variables separated with ',', files separated with ';'")
    parser.add_option("-f", "--filter", dest="filter", action="store", type="string",
                      help="optional filtering criteria, support multiple 'and' only, eg, var1=1&&var2!=abc&&var3!=. means 'var1 = 1 and var2 != abc and var3 is not missing'")
    (options, args) = parser.parse_args()

    if options.dlm.startswith('x') :
        options.dlm = chr(int(options.dlm[1:]))

    input_files = options.input.split(';')
    output_files = options.output.split(';') if options.output else [generateOutputName(x) for x in input_files]
    if len(input_files) != len(output_files) :
        raise ValueError("The output file lists must have 1-to-1 relationship with the dataset files")
    type_strings = options.type.split(";") if options.type else None
    keep_strings = options.keep.split(';') if options.keep else None
    drop_strings = options.drop.split(';') if options.drop else None
    if type_strings is not None and len(type_strings) != len(input_files) :
        raise ValueError("The type list files must have 1-to-1 relationship with the dataset files")
    if keep_strings is not None and len(keep_strings) != len(input_files) :
        raise ValueError("The keep strings must have 1-to-1 relationship with the dataset files")
    if drop_strings is not None and len(drop_strings) != len(input_files) :
        raise ValueError("The drop strings must have 1-to-1 relationship with the dataset files")

    for data_file in input_files :
        if not os.path.exists(data_file) :
            raise IOError("file does not exist! {}".format(data_file))

    filter_strings = options.filter.split('&&') if options.filter else None
    filters = {}
    for filter_string in filter_strings :
        var_name, var_value = filter_string.split('=', 2)
        var_name = var_name.strip().lower()
        var_value = var_value.strip()
        if var_name.endswith('!') :
            var_name = var_name[:-1]
            equal = False
        else :
            equal = True
        if var_value == '.' :
            var_value = ''
        filters[var_name] = (var_value, equal)

    global_variable_set = set()
    file_headers = []
    for find in xrange(len(input_files)) :

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
            keeps, drops = parse_type(type_strings[find])
        else :
            keeps = set()
            drops = set()
        with open(input_files[find]) as fn :
            # Read the header
            line = fn.readline()
            variable_names = [x.strip() for x in line.split(options.dlm)]
            variable_names_lower = [x.lower() for x in variable_names]

            # Generate the variable list after keeping / dropping, and the filter criteria
            if drop_strings is not None :
                drop_patterns = drop_strings[find].lower().split(',')
                drop_variables = findPatterns(variable_names_lower, drop_patterns)
                drops = drops.union(drop_variables) if len(keeps) == 0 else drops
                keeps = keeps.difference(drop_variables)
            if keep_strings is not None :
                keep_patterns = keep_strings[find].lower().split(',')
                keep_variables = findPatterns(variable_names_lower, keep_patterns)
                keeps = keeps.union(keep_variables) if len(drops) == 0 else keeps
                drops = drops.difference(keep_variables)
            output_index = []
            filter_index = {}
            for vind in xrange(len(variable_names)) :
                if ((len(drops) > 0 and variable_names_lower[vind] not in drops)
                        or (len(keeps) > 0 and variable_names_lower[vind] in keeps)
                        or (len(drops) == 0 and len(keeps) == 0)) :
                    output_index.append(vind)
                if variable_names_lower[vind] in filters :
                    filter_index[vind] = filters[variable_names_lower[vind]]

            # Start reading and writing data
            with open(output_files[find]) as fo :
                while True :
                    line = fn.readline()
                    if not line :
                        break
                    values = line.split(options.dlm)
                    skip = False
                    for cind, cval in filter_index :
                        if (values[cind].strip() == cval[0]) != cval[1] :
                            skip = True
                            break
                    if skip :
                        continue
                    fo.write(options.dlm.join([values[x] for x in output_index]))
                    fo.write("\n")
                    fo.flush()
