###prepare data and do stats
/export/mb/share1/MB7.3.1/mbsh dev_nn_r_f_eric.mb $1 -Dmb.logging.suppress=true -Xmx40g  > dev_nn_r_f.log 2>&1
/export/mb/share1/MB7.3.1/mbsh A1_ProduceStats.mb -Dmb.logging.suppress=true -Xmx40g > A1_run.log 2>&1

###create variable library
if [[ -d "./variableLibrary" ]] ; then
        \rm -rf ./variableLibrary
fi
/export/mb/share1/MB7.3.1/mbsh A2_CreateVarLib.mb -Dmb.logging.suppress=true -Xmx40g > A2_run.log 2>&1

###modeling step
if [[ -d "./variableLibrary" ]] ; then
        /export/mb/share1/MB7.3.1/mbsh B_PrepareData.mb -Dmb.logging.suppress=true -Xmx40g > B_run.log 2>&1
        /export/mb/share1/MB7.3.1/mbsh C_TrainManager.mb -Dmb.logging.suppress=true -Xmx40g > C_run.log 2>&1
fi




