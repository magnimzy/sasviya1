option castimeout=max;

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/************************************************************************/
cas mySession sessopts=(caslib=casuser timeout=360000 locale="en_US" metrics=true);

 
/*DEFINE CAS ENGINE LIBREF FOR CAS IN-MEMORY TABLES*/  
libname mycas cas caslib=casuser datalimit=100G ;

/*DEFINE LIBREF to LOCAL TABLES*/
libname locallib '/users/eybmotmp/casuser/';

/*SPECIFY FOLDER PATH FOR score code OUTPUT FILES*/
%let outdir = /users/eybmotmp/casuser/;

/*Define inputs for models using macro variables. This way you dont 
necessarily have to drop variables from the data you can use specific 
variables in the data and reference them using &inputs when it comes to modeling.  */

%let IntInputs= V1 V2 V3 V4 V5 V6 V7 V8 V9 V10 V11 V12 V13 V14 V15 V16 V17 V18 V19 V20
                V21 V22 V23 V24 V25 V26 V27 V28 Amount;

/*In SAS a macro is first compiled --that is everything between the %macro  and the %mend at the bottom.
Compiling a macro does not execute it but just makes it ready to execute. So everytime you start a session
you would run the code between these two commands, and only do it once. From then on all you need to do 
is call the macro, and pass parameters to it if different than the defaults you compile

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
proc import datafile='/users/eybmotmp/casuser/creditcard.csv'
	dbms=CSV
	out=mycas.creditcard;
	getnames=YES;
run;

/*** Drop the Time column and rename Class inzto target and Add an ID variable****/
data mycas.creditcard;
  set mycas.creditcard(drop= Time rename=(Class=target));
  ID=_n_ + (_threadid_ * 1E4);
run;

/*** Partition of the database ***/
proc partition data=mycas.creditcard partition samppct=70 seed=303;
	output out=mycas.creditcard copyvars=(_ALL_);
run;

/***  Test and training datasets  ****/

data mycas.training(drop=_partind_) mycas.test (drop=_partind_);
   set mycas.creditcard;
   if _partind_=1 then output mycas.training;
   else output mycas.test;
run;

/*** Unlabelled and true positives for Test and training datasets  ****/

data mycas.test_tp mycas.test_ul;
  set mycas.test;
  if target=1 then output mycas.test_tp;
   else output mycas.test_ul;
run;

data mycas.train_tp mycas.train_ul;
  set mycas.training;
  if target=1 then output mycas.train_tp;
   else output mycas.train_ul;
run;

%let ratio=3;

/*****************************************************/

%let valid=mycas.test;

data svm_ensemble;
	attrib metric length=$20 model length=$20;
	stop;
run;

data mycas.f1_svm_eval(keep=ID target) mycas.ase_svm_eval(keep=ID target) 
		mycas.auc_svm_eval(keep=ID target);
	set &valid;
	output mycas.f1_svm_eval;
	output mycas.ase_svm_eval;
	output mycas.auc_svm_eval;
run;

data gboost_ensemble;
	attrib metric length=$20 model length=$20;
	stop;
run;

data mycas.f1_gboost_eval(keep=ID target) mycas.ase_gboost_eval(keep=ID target) 
		mycas.auc_gboost_eval(keep=ID target);
	set &valid.;
	output mycas.f1_gboost_eval;
	output mycas.ase_gboost_eval;
	output mycas.auc_gboost_eval;
run;

%macro svmtrainSelect(data=mycas.training, target=target, IntIn=&IntInputs., ul_tp = &ratio., metric=f1);

		proc datasets lib=mycas;
			delete ParmAssess:;
		run;

		%do samp=1 %to 10;

			data cd_ul cd_tp;
				set &data.;		
				if &target.=1 then output cd_tp;
				else output cd_ul;
			run;
	
			proc sql noprint;
				select count(*) into :N separated by ' ' from cd_tp;
			quit;
			
			proc surveyselect data=work.cd_ul method=urs seed=&samp out=work.train1 outhits sampsize=%sysevalf(&ul_tp.*&N.);
			run;
			
			data mycas.btsp;
				set train1(drop=numberhits) cd_tp;
			run;
	
			proc partition data=mycas.btsp partition samppct=70 seed=303;
				output out=mycas.part copyvars=(_ALL_);
			run;
	
			proc svmachine data=mycas.part;
				partition rolevar=_partind_(train='1' validate='0');
				autotune searchmethod=grid objective=&metric. tuningparameters=(c(values=0.01 0.1 1 10 100) degree(values=1 2 3));
				input &IntIn./level=interval;
				target &target.;
				savestate rstore=mycas.svm_&metric._&samp.;
	            ods output BestConfiguration= bestconfig;
			run;
	
	        data bestconfig;
				set bestconfig; model="svm_model_&samp"; metric="&metric.";
			run;
	
			data svm_ensemble;
				set svm_ensemble bestconfig;
			run;
			
			proc astore;
				score data=&valid. out=mycas.svm_pred_&metric._&samp.
			        rstore=mycas.svm_&metric._&samp. copyvars=(_ALL_);
			run;
	
			data mycas.out;
				set mycas.svm_pred_&metric._&samp.;
				keep ID p_&target.1 p_&target.0 i_&target. &target.;
				rename p_&target.0=p&target.0_svm_&samp. p_&target.1=p&target.1_svm_&samp. i_&target.=i&target._svm_&samp.;
			run;
			
			data mycas.&metric._svm_eval;
				merge mycas.&metric._svm_eval mycas.out;
				by ID;
			run;

			proc gradboost data=mycas.part earlystop(stagnation=0);
				partition rolevar=_partind_(train='1' validate='0');
				autotune searchmethod=grid 
					objective=&metric. tuningparameters=(ntrees(values=100 250 500) 
					maxdepth(values=1 2 3 4) learningrate(values=0.1 0.25 0.5 1) lasso(EXCLUDE) 
					ridge(EXCLUDE) numbin(EXCLUDE) VARS_TO_TRY(EXCLUDE) samplingrate(EXCLUDE));
				input &IntIn./level=interval;
				target &target.;
				savestate rstore=mycas.gboost_&metric._&samp.;
				ods output BestConfiguration=bestconfig;
			run;
			
			data bestconfig;
				set bestconfig;
				model="gboost_model_&samp";
				metric="&metric.";
			run;
			
			data gboost_ensemble;
				set gboost_ensemble bestconfig;
			run;
			
			proc astore;
				score data=&valid. out=mycas.gboost_pred_&metric._&samp.
					        rstore=mycas.gboost_&metric._&samp. copyvars=(_ALL_);
			run;
			
			data mycas.out;
				set mycas.gboost_pred_&metric._&samp.;
				keep ID p_&target.1 p_&target.0 i_&target. &target.;
				rename p_&target.0=p&target.0_gboost_&samp. p_&target.1=p&target.1_gboost_&samp. i_&target.=i&target._gboost_&samp.;
			run;
			
			data mycas.&metric._gboost_eval;
				merge mycas.&metric._gboost_eval mycas.out;
				by ID;
			run;

         %end;

%mend svmtrainselect;

%let _timer_start = %sysfunc(datetime()); 

%svmtrainselect(data=mycas.training, target=target, IntIn=&IntInputs., ul_tp = &ratio., metric=f1);

data _null_;
	dur=datetime() - &_timer_start;
	put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;