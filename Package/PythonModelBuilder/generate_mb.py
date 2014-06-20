from optparse import OptionParser
from subprocess import call


__author__ = 'yijiliu'

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="key", action="store", type="string",
                      help="primary key variables separated with ','")
    parser.add_option("-b", "--bad", dest="bad", action="store", type="string",
                      help="target bad tagging variable")
    parser.add_option("-t", "--train", dest="train", action="store", type="string",
                      help="training dataset file")
    parser.add_option("-e", "--test", dest="test", action="store", type="string",
                      help="test dataset files separated with ';'")
    parser.add_option("-d", "--dlm", dest="dlm", action="store", type="string", default=",",
                      help="delimiter char, accept xASCII format, default=','")
    parser.add_option("-v", "--vars", dest="vars", action="store", type="string",
                      help="model variable list separated with ',' and wild characters is supported such as '*_zscl'")
    parser.add_option("-s", "--base", dest="base", action="store", type="string",
                      help="non-training variables need to be kept, separated with ','")
    parser.add_option("-r", "--drop", dest="drop", action="store", type="string",
                      help="drop variable list separated with ','")
    parser.add_option("-n", "--node", dest="node", action="store", type="string",
                      help="nodes in the hidden layer, separated with ','")
    parser.add_option("-f", "--prefix", dest="prefix", action="store", type="string",
                      help="optional prefix for output files")
    (options, args) = parser.parse_args()

    if not (options.bad and options.train and options.key) :
        print "train, bad and key are required"
        exit()

    config_string = {'bad' : options.bad.lower(),
                     'train' : options.train,
                     'pre' : options.prefix + '_' if options.prefix is not None and options.prefix.strip() != '' else ''}

    if options.test :
        if options.test.find(';') >= 0 :
            tests = options.test.split(';')
        else :
            tests = [options.test]

    if options.key.find(',') >= 0 :
        keys = [x.lower() for x in options.key.split(',')]
    else :
        keys = [options.key.lower()]
    config_string['keys'] = " ".join(keys)

    if options.dlm.startswith('x') :
        config_string['delimiter'] = '\\' + oct(int(options.dlm[1:], 16))
    else:
        config_string['delimiter'] = options.dlm

    if options.node.find(',') >= 0 :
        nodes = [x.strip() for x in options.node.split(',')]
    else :
        nodes = [options.node.strip()]

    delimiter = chr(int(options.dlm[1:])) if options.dlm.startswith('x') else options.dlm
    heads = open(options.train).readline().split(delimiter)
    bases = [x.lower() for x in options.base.split(',')] if options.base else []

    config_string['key_pos'] = ''
    for head in heads:
        var = head.strip().lower()
        if var == config_string['bad'] :
            role = 'target'
            var_type = 'numeric'
        elif var in keys :
            role = 'recordID'
            var_type = 'string'
        elif var in bases :
            role = 'uninformative'
            var_type = 'string'
        else :
            role = 'predictor'
            var_type = 'numeric'
        no_setid_head = head.strip() if var != 'setid' else 'no_set_id'
        config_string['key_pos'] += "\tvar( name : \"{0}\", type : \"{1}\", role : \"{2}\")\n".format(no_setid_head, var_type, role)

#    config_string['role_ids'] = ''
#    for key in keys :
#        config_string['role_ids'] += "\telse if (it.getName().toLowerCase() == '{0}')\n\t\tit.getVariableRoles().add('recordID')\n".format(key)

    if not options.vars :
        options.vars = '*_woe_zscl'
    if options.vars.find(',') > 0 :
        options.vars = options.vars.replace(',', ' ')
    config_string['keep_vars'] = " ".join([options.vars, options.bad, " ".join(keys), " ".join(bases)])

    config_string['nodes'] = options.node
    config_string['nodes_num'] = min(len(nodes), 3)
    config_string['model_vars'] = "'{}'".format(options.vars)

    if not options.drop :
        options.drop = ""
    if options.drop.find(',') > 0 :
        options.drop = options.drop.replace(',', ' ')
    config_string['drop_vars'] = "" if not options.drop else options.drop

    fr = open('run_mb_nn.sh', 'w')
    cmd = r"/export/mb/share1/MB7.3.1/mbsh {0}.mb -Dmb.logging.suppress=true -Xmx40g  > {0}.log 2>&1"

    part_a = open('A_ReadTrainData.mb.template').read()
    with open('A_ReadTrainData.mb', 'w') as fn :
        fn.write(part_a.format(opt=config_string))
    fr.write(cmd.format("A_ReadTrainData"))
    fr.write("\n\n")

    part_b = open('B_TrainManager.mb.template').read()
    with open('B_TrainManager.mb', 'w') as fn :
        fn.write(part_b.format(opt=config_string))
    fr.write(cmd.format("B_TrainManager"))
    fr.write("\n\n")

    part_c = open('C_TrainWorker.mb.template').read()
    with open('C_TrainWorker.mb', 'w') as fn :
        fn.write(part_c.format(opt=config_string))

    if options.test:
        part_d = open('D_ReadData.mb.template').read()
        for ind in xrange(len(tests)) :
            cmd_d = "D_ReadData_d{}".format(ind)
            config_string['source'] = tests[ind]
            config_string['target'] = "test_d{}".format(ind)
            with open('{}.mb'.format(cmd_d), 'w') as fn :
                fn.write(part_d.format(opt=config_string))
            fr.write(cmd.format(cmd_d))
            fr.write("\n\n")

    part_e = open('E_ScoreData.mb.template').read()
    for node in nodes :
        spec = r"./scripts/models/{}spec_nn_n{}_l1.pmml".format(config_string['pre'], node)
        cmd_e = "E_ScoreData_{0}_n{1}".format("train", node)
        config_string['spec'] = spec
        config_string['name'] = "{}nn_n{}".format(config_string['pre'], node)
        config_string['input'] = r"./data/{}nn_train.mbd".format(config_string['pre'])
        config_string['output'] = r"./data/{}scored_train_n{}.csv".format(config_string['pre'], node)
        with open('{}.mb'.format(cmd_e), 'w') as fn :
            fn.write(part_e.format(opt=config_string))
        fr.write(cmd.format(cmd_e))
        fr.write("\n")

        if options.test :
            for ind in xrange(len(tests)) :
                cmd_e = "E_ScoreData_d{0}_n{1}".format(ind, node)
                config_string['input'] = r"./data/{}test_d{}.mbd".format(config_string['pre'], ind)
                config_string['output'] = r"./data/{0}scored_test_d{1}_n{2}.csv".format(config_string['pre'], ind, node)
                with open('{}.mb'.format(cmd_e), 'w') as fn :
                    fn.write(part_e.format(opt=config_string))
                fr.write(cmd.format(cmd_e))
                fr.write("\n")
            fr.write("\n")

    fr.flush()
    fr.close()
    call(["chmod", "777", "run_mb_nn.sh"])

    call(['bash', "run_mb_nn.sh"])
