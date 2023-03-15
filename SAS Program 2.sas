cas; 
options casdatalimit=5000M;

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

data mys3.base_25mb;
set casuser.base_25mb;
run;

data mys3.base_95mb;
set mys3.base_95mb casuser.base_25mb;
run;

proc casutil incaslib="mys3";
save casdata="base_95mb" casout="base_95mb.csv";
run;

proc casutil incaslib="mys3";
save casdata="base1" casout="base1.csv";
run;

proc casutil incaslib="mys3";
deletesource casdata="base_50mb_new_spre.csv";
run;

/* Load the tables from S3 bucket */

%let file=base_95mb;

proc casutil incaslib="mys3"  outcaslib="mys3";
droptable casdata="&file." incaslib="mys3" quiet;
load casdata="&file..csv" casout="&file.";
run;

/* CAS processing */

data mys3.&file._new;
set mys3.&file.;
income = income*3;
if payment_type ='' then payment_type ='TBD';
run;

/* Save results to S3 bucket */

proc casutil incaslib="mys3";
save casdata="&file._new" casout="&file._new.csv";
run;




/* COMPUTE */
/*************/

libname mylib "/home/magnim.farouh@ca.ey.com/casuser/";

/* load file from S3 bucket to sas server */

%let file=base_25mb;

proc s3 keyid="AKIA3KCDRIDX4HXA7XX6" secret="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" region="useast" nossl;
        get "/viya-bucket/demo/&file..csv" "/home/magnim.farouh@ca.ey.com/casuser/&file..csv";
run;

proc import datafile="/home/magnim.farouh@ca.ey.com/casuser/&file..csv" 
		REPLACE DBMS=CSV OUT=mylib.&file.;
	getnames=yes;
run;

/* processing */

data mylib.&file._new;
set mylib.&file.;
income = income*3;
if payment_type ='' then payment_type ='TBD';
run;

/* Save results to S3 bucket */

proc export data=mylib.&file._new
   outfile="/home/magnim.farouh@ca.ey.com/casuser/&file._new.csv"
   dbms=dlm replace;   
   delimiter='&';
run;


proc s3 keyid="AKIA3KCDRIDX4HXA7XX6" secret="PC+iBjDOHHTdmUlc400VPhONNceAQI0n28KQydy0" region="useast" nossl;
        put "/home/magnim.farouh@ca.ey.com/casuser/&file._new.csv" "/viya-bucket/demo/&file._new_spre.csv";
run;










