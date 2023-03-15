option castimeout=max;

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/************************************************************************/
cas mySession sessopts=(caslib=casuser timeout=36000 locale="en_US" metrics=true);


/*DEFINE CAS ENGINE LIBREF FOR CAS IN-MEMORY TABLES*/  
libname mycas cas caslib=casuser;

/*DEFINE LIBREF to LOCAL TABLES*/
libname locallib '/Users/magnimzy@gmail.com/My Folder/';

/*SPECIFY FOLDER PATH FOR score code OUTPUT FILES*/
%let outdir = /Users/magnimzy@gmail.com/My Folder/;

/* Macro to turn of and turn on displays in the log file.*/

%macro ODSOff();
                ods graphics off;
                ods exclude all;
                ods noresults;
    options nonotes;
%mend;

%macro ODSOn();
                ods graphics on;
                ods exclude none;
                ods results;
    options notes;
%mend;

/*Import the creditcard.csv file*/

%if %sysfunc(exist(MYCAS.EHR)) %then %do;
    drop table MYCAS.EHR;
%end;
%if %sysfunc(exist(MYCAS.EHR,VIEW)) %then %do;
    drop view MYCAS.EHR;
%end;

FILENAME REFFILE FILESRVC FOLDERPATH='/Users/magnimzy@gmail.com/My Folder'  FILENAME='EHR.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX
	OUT=MYCAS.EHR;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=MYCAS.EHR; RUN;


FILENAME REFFILE FILESRVC FOLDERPATH='/Users/magnimzy@gmail.com/My Folder'  FILENAME='Claims.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX
	OUT=MYCAS.Claims;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=MYCAS.Claims; RUN;

data public.Claims (promote=yes);
set mycas.claims;
run;

data public.EHR (promote=yes);
set mycas.EHR;
run;


/* Teradata */


libname tlib teradata server="localhost" 
database="HR" user="dbc" 
password="dbc";


caslib tdlib 
     datasource=(srctype='teradata'
                 dataTransferMode='parallel'
                 server='localhost'
                 username='dbc'
                 password='dbc'
                 database='HR');

