/export/mb/share1/MB7.3.1/mbsh C_TrainManager.mb -Dmb.logging.suppress=true -Xmx40g > C_run.log 2>&1

###prepare data and do stats
#/export/mb/share1/MB7.3.1/mbsh oot_import.mb -Dmb.logging.suppress=true -Xmx40g  > oot_impt.log 2>&1

###modeling step
#if [[ -d "../variableLibrary" ]] ; then
#        /export/mb/share1/MB7.3.1/mbsh oot_zscl.mb -Dmb.logging.suppress=true -Xmx40g > oot_zscl.log 2>&1
#fi


###prepare deliver files
/export/mb/share1/MB7.3.1/mbsh oot_perf.mb  -Dmb.logging.suppress=true -Xmx40g > oot_perf.log 2>&1

###package and send via email
###tar -cvzf oot_seg1.tar.gz ./oot
#echo "seg1" | mutt -s "seg1 woe oot " -a "./oot/perf_oot_dol_nowgt.pdf" huayin@ebay.com 


