FILENAME REFFILE DISK '/home/magnim.farouh@ca.ey.com/casuser/base_25mb.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=work.table_100K;
    GETNAMES=YES;
RUN;


libname mylib '/home/magnim.farouh@ca.ey.com/casuser/';

proc datasets lib=mylib;

proc means data=mylib.base_25mb;
run;

proc contents data=mylib.base_25mb;
run;

proc sql;
select * from dictionary.tables;
quit;