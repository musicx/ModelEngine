%inc "config.sas";

data readin;
infile "data2.dat" firstobs=1;
input my_var;
%manipulate(my_var, my_var_neg);
drop my_var;
run;

proc print data=readin; run;

proc export data=readin outfile="neg.dat" dbms=dlm replace;
run;

