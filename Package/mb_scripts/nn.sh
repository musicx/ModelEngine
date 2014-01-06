###prepare data and do stats
/export/mb/share1/MB7.3.1/mbsh dev_nn_r_f.mb -Dmb.logging.suppress=true -Xmx40g  > dev_nn_r_f.log 2>&1
#/export/mb/share1/MB7.3.1/mbsh setid.mb -Dmb.logging.suppress=true -Xmx40g > setid_run.log 2>&1
/export/mb/share1/MB7.3.1/mbsh A1_ProduceStats.mb -Dmb.logging.suppress=true -Xmx40g > A1_run.log 2>&1

###create variable library
if [[ -d "../variableLibrary" ]] ; then
        \rm -r ../variableLibrary
fi
/export/mb/share1/MB7.3.1/mbsh A2_CreateVarLib.mb -Dmb.logging.suppress=true -Xmx40g > A2_run.log 2>&1

###modeling step
if [[ -d "../variableLibrary" ]] ; then
        /export/mb/share1/MB7.3.1/mbsh B_PrepareData.mb -Dmb.logging.suppress=true -Xmx40g > B_run.log 2>&1
        /export/mb/share1/MB7.3.1/mbsh C_TrainManager.mb -Dmb.logging.suppress=true -Xmx40g > C_run.log 2>&1
fi


###prepare deliver files
#/export/mb/share1/MB7.3.1/mbsh perf.mb  -Dmb.logging.suppress=true -Xmx40g >perf.log 2>&1
#/export/mb/share1/MB7.3.1/mbsh genxml2.mb  -Dmb.logging.suppress=true -Xmx40g >genxml2.log 2>&1
#/export/mb/share1/MB7.3.1/mbsh gen_unit_test.mb -Dmb.logging.suppress=true -Xmx40g >gen_unit_test.log 2>&1


###package and send via email
#rm ./deliver/*.*
#cp ./models/*.* ./deliver
#cp ./perf/*.* ./deliver
#tar -cvzf deliver_seg1.tar.gz ./deliver
#echo "seg1" | mutt -s "seg1 deliver " -a "./deliver_seg1.tar.gz" huayin@ebay.com 


