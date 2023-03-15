cas; 

/*proc printto log="/home/magnim.farouh@ca.ey.com/casuser/log.txt";
run;*/

caslib mys3 datasource=(srctype="s3"
               accesskeyid="AKIA3KCDRIDX4HXA7XX6"
               secretaccesskey="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"
               region="useast"
               bucket="viya-bucket"
               objectpath="/demo/"
               usessl=true);

caslib _all_ assign;

data mys3.base;
set casuser.base;
run;

proc casutil incaslib="mys3";
save casdata="base" casout="base.csv";
run;

proc casutil incaslib="mys3";
deletesource casdata="table_10K_new_cas.sashdat";
run;

proc casutil incaslib="mys3"  outcaslib="public";
    droptable casdata="cities" incaslib="public"  quiet;
       load casdata="cities.csv" casout="cities" promote ;
run;


/* import table from sas server */
proc sql;
%if %sysfunc(exist(CASUSER.table_100K)) %then %do;
    drop table CASUSER.table_100K;
%end;
%if %sysfunc(exist(CASUSER.table_100K,VIEW)) %then %do;
    drop view CASUSER.table_100K;
%end;
quit;

proc sql;
%if %sysfunc(exist(CASUSER.table_10K)) %then %do;
    drop table CASUSER.table_10K;
%end;
%if %sysfunc(exist(CASUSER.table_10K,VIEW)) %then %do;
    drop view CASUSER.table_10K;
%end;
quit;

FILENAME REFFILE DISK '/home/magnim.farouh@ca.ey.com/casuser/table_100K.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=CASUSER.table_100K;
    GETNAMES=YES;
RUN;

FILENAME REFFILE DISK '/home/magnim.farouh@ca.ey.com/casuser/table_10K.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=CASUSER.table_10K;
    GETNAMES=YES;
RUN;

/* Copying the tables to the SPRE */

data table_10K;
set casuser.table_10K;
run;

data table_100K;
set casuser.table_100K;
run;

/* CAS processing */

data casuser.table_10K_new;
set casuser.table_10K;
amount = amount*3;
if state1='' then
   do;
      state1='TBD';
   end;
if domain1='' then
   do;
      domain1='TBD';
   end;
run;

data casuser.table_100K_new;
set casuser.table_100K;
amount = amount*3;
if state1='' then
   do;
      state1='TBD';
   end;
if domain1='' then
   do;
      domain1='TBD';
   end;
run;

/* SAS Compute processing */
data table_10K_new;
set table_10K;
amount = amount*3;
if state1='' then
   do;
      state1='TBD';
   end;
if domain1='' then
   do;
      domain1='TBD';
   end;
run;

data table_100K_new;
set table_100K;
amount = amount*3;
if state1='' then
   do;
      state1='TBD';
   end;
if domain1='' then
   do;
      domain1='TBD';
   end;
run;

/* saving results to a SAS Server*/

proc export data=table_10K_new
   outfile="/home/magnim.farouh@ca.ey.com/casuser/table_10K_new.csv"
   dbms=dlm replace;   
   delimiter='&';
run;

proc export data=table_100K_new
   outfile="/home/magnim.farouh@ca.ey.com/casuser/table_100K_new.csv"
   dbms=dlm replace;   
   delimiter='&';
run;

proc export data=casuser.table_10K_new
   outfile="/home/magnim.farouh@ca.ey.com/casuser/table_10K_new_cas.csv"
   dbms=dlm replace;   
   delimiter='&';
run;

proc export data=casuser.table_100K_new
   outfile="/home/magnim.farouh@ca.ey.com/casuser/table_100K_new_cas.csv"
   dbms=dlm replace;   
   delimiter='&';
run;

/* saving results to a S3*/

proc casutil  incaslib="casuser" outcaslib="mys3";
   casout= "table_10K_new_cas" replace ;
quit;


proc casutil  incaslib="casuser" outcaslib="mys3";
   save casdata="table_10K_new"  casout= "table_10K_new_cas" replace ;
quit;

proc casutil  incaslib="casuser" outcaslib="mys3";
   save casdata="table_100K_new"  casout= "table_100K_new_cas" replace ;
quit;

data mys3.table_10K_new;
set table_10K_new;
run;
proc casutil outcaslib="mys3";         
   save casdata="table_10K_new";
quit;

data mys3.table_100K_new;
set table_100K_new;
run;
proc casutil outcaslib="mys3";         
   save casdata="table_100K_new";
quit;

proc casutil;
   list files incaslib="mys3";
   list tables incaslib="mys3"; 
quit ;

proc printto;
run;



































