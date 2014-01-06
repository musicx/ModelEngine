
/export/mb/share1/MB7.3.1/mbsh C_train_score.mb -Dmb.logging.suppress=true -Xmx40g > C_run.log 2>&1



###prepare deliver files
/export/mb/share1/MB7.3.1/mbsh perf_score.mb  -Dmb.logging.suppress=true -Xmx40g >perf.log 2>&1
#/export/mb/share1/MB7.3.1/mbsh genxml2.mb  -Dmb.logging.suppress=true -Xmx40g >genxml2.log 2>&1
#/export/mb/share1/MB7.3.1/mbsh gen_unit_test.mb -Dmb.logging.suppress=true -Xmx40g >gen_unit_test.log 2>&1


###package and send via email
#rm ./deliver/*.*
#cp ./models/*.* ./deliver
#cp ./perf/*.* ./deliver
#tar -cvzf deliver_seg1.tar.gz ./deliver
echo "seg1" | mutt -s "seg1 deliver " huayin@ebay.com 


