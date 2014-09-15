from optparse import OptionParser
from subprocess import call


__author__ = 'yijiliu'

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="key", action="store", type="string",
                      help="primary key variables separated with ','")
    parser.add_option("-b", "--bad", dest="bad", action="store", type="string",
                      help="target bad tagging variable")
    parser.add_option("-i", "--input", dest="input", action="store", type="string",
                      help="training dataset file")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=','")
    parser.add_option("-v", "--vars", dest="vars", action="store", type="string",
                      help="model variable list separated with ',' and wild characters is supported such as '*_zscl'")
    parser.add_option("-r", "--drop", dest="drop", action="store", type="string",
                      help="drop variable list separated with ','")
    parser.add_option("-n", "--num", dest="num", action="store", type="int", default=500,
                      help="target number of variables, final list will contain less than this number, no guarantee of equalism")
    parser.add_option("-f", "--prefix", dest="prefix", action="store", type="string",
                      help="optional prefix for output files")
    (options, args) = parser.parse_args()

    if not (options.bad and options.input and options.key) :
        print "data, bad and key are required"
        exit()

    pre_string = options.prefix + '_' if options.prefix is not None and options.prefix.strip() != '' else ''
    config_string = {'bad' : options.bad.lower(),
                     'input' : options.input,
                     'pre' : pre_string}

    if options.key.find(',') >= 0 :
        keys = [x.lower() for x in options.key.split(',')]
    else :
        keys = [options.key.lower()]
    config_string['keys'] = " ".join(keys)

    if options.dlm.startswith('x') :
        config_string['delimiter'] = '\\' + oct(int(options.dlm[1:], 16))
    else:
        config_string['delimiter'] = options.dlm

    try :
        vnum = int(options.num)
        if vnum <= 0 :
            vnum = 500
    except ValueError as e:
        vnum = 500
    config_string['vnum'] = '{}'.format(vnum)

    delimiter = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm
    heads = open(options.input).readline().split(delimiter)

    var_table = 'name, type, role\n'
    key_pos = ''
    config_string['key_pos'] = ''
    for head in heads:
        var = head.strip().lower()
        if var == config_string['bad'] :
            role = 'target'
            var_type = 'numeric'
        elif var in keys :
            role = 'recordID'
            var_type = 'string'
        else :
            role = 'predictor'
            var_type = 'numeric'
        no_setid_head = head.strip() if var != 'setid' else 'no_set_id'
        key_pos += "\tvar( name : \"{0}\", type : \"{1}\", role : \"{2}\")\n".format(no_setid_head, var_type, role)
        var_table += "{},{},{}\n".format(no_setid_head, var_type, role)
    with open('var_table.txt', 'w') as fv :
        fv.write(var_table)
#    config_string['role_ids'] = ''
#    for key in keys :
#        config_string['role_ids'] += "\telse if (it.getName().toLowerCase() == '{0}')\n\t\tit.getVariableRoles().add('recordID')\n".format(key)

    if not options.vars :
        options.vars = '*_woe_zscl'
    if options.vars.find(',') > 0 :
        options.vars = options.vars.replace(',', ' ')
    config_string['keep_vars'] = " ".join([options.vars, options.bad, " ".join(keys)])
    config_string['model_vars'] = "'{}'".format(options.vars)

    if not options.drop :
        options.drop = ""
    if options.drop.find(',') > 0 :
        options.drop = options.drop.replace(',', ' ')
    config_string['drop_vars'] = "" if not options.drop else options.drop

    fr = open('run_mb_sense.sh', 'w')
    cmd = r"/export/mb/share1/MB7.3.1/mbsh {0}.mb -Dmb.logging.suppress=true -Xmx40g  > {0}.log 2>&1"

    part_a = open('A_ReadData.mb.template').read()
    with open('A_ReadData.mb', 'w') as fn :
        fn.write(part_a.format(opt=config_string))
    fr.write(cmd.format("A_ReadData"))
    fr.write("\n\n")

    part_b = open('B_Manager.mb.template').read()
    with open('B_Manager.mb', 'w') as fn :
        fn.write(part_b.format(opt=config_string))
    fr.write(cmd.format("B_Manager"))
    fr.write("\n\n")

    part_c = open('C_Worker.mb.template').read()
    with open('C_Worker.mb', 'w') as fn :
        fn.write(part_c.format(opt=config_string))

    fr.write('more +2 scripts/models/{0}sensitivity_n5_l1.csv | cut -d, -f1 > {0}sensitivity_list.csv \n'.format(pre_string))

    fr.flush()
    fr.close()
    call(["chmod", "777", "run_mb_sense.sh"])

    call(['bash', "run_mb_sense.sh"])
