
###prepare deliver files
/export/mb/share1/MB7.3.1/mbsh perf.mb  -Dmb.logging.suppress=true -Xmx40g > gm_perf.log 2>&1
/export/mb/share1/MB7.3.1/mbsh perf_oot.mb  -Dmb.logging.suppress=true -Xmx40g > gm_oot_perf.log 2>&1


###package and send via email
###tar -cvzf oot_seg1.tar.gz ./oot
echo "all" | mutt -s "all finished " huayin@ebay.com 


