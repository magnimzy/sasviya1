options castimeout=28800;

/************************************************************************/
cas mySession sessopts=(caslib=casuser timeout=1800 locale="en_US");
cas;
 
/*DEFINE CAS ENGINE LIBREF FOR CAS IN-MEMORY TABLES*/  
libname mycas cas caslib=casuser;

/*DEFINE LIBREF to LOCAL TABLES*/
libname locallib '/users/eybmotmp/casuser/';

/*SPECIFY FOLDER PATH FOR score code OUTPUT FILES*/
%let outdir = /users/eybmotmp/casuser/;

/*Define inputs for models using macro variables. This way you dont 
necessarily have to drop variables from the data you can use specific 
variables in the data and reference them using &inputs when it comes to modeling.  */

%let IntInputs= V1 V2 V3 V4 V5 V6 V7 V8 V9 V10 V11 V12 V13 V14 V15 V16 V17 V18 V19 V20
                V21 V22 V23 V24 V25 V26 V27 V28 Amount;
/* %let ClassInputs=  Res;*/

/*Import the creditcard.csv file*/
proc import datafile='/users/eybmotmp/casuser/creditcard.csv'
	dbms=CSV
	out=mycas.creditcard;
	getnames=YES;
run;

/*** Drop the Time column and rename Class ino target and Add an ID variable****/
data mycas.creditcard;
  set mycas.creditcard(drop= Time rename=(Class=target));
  ID=_n_;
run;

/*** Partition of the database ***/
proc partition data=mycas.creditcard partition samppct=70 seed=303;
	output out=mycas.creditcard copyvars=(_ALL_);
run;

/***     Test and training datasets    ****/
data mycas.training;
  set mycas.creditcard;
  if _PartInd_=1;
  drop _PartInd_;
run;

data mycas.test;
  set mycas.creditcard;
  if _PartInd_=0;
  drop _PartInd_;
run;

/*** Unlabelled and true positives for Test and training datasets  ****/

data mycas.test_tp;
  set mycas.test;
  if target=1;
run;

data mycas.test_ul;
  set mycas.test;
  if target=0;
run;

data mycas.train_tp;
  set mycas.training;
  if target=1;
run;

data mycas.train_ul;
  set mycas.training;
  if target=0;
run;


/*In SAS a macro is first compiled --that is everything between the %macro logreg and the %mend at the bottom.
Compiling a macro does not execute it but just makes it ready to execute. So everytime you start a session
you would run the code between these two commands, and only do it once. From then on all you need to do 
is call the macro, and pass parameters to it if different than the defaults you compile

/* Bootstrap macro */
%macro bootstrap(data=mycas.training, target=target, i=1);

	proc partition data=&data samppctevt=100 eventprop=0.25 event="1" seed= %eval(&i.);
		by &target;
		ods output OVERFREQ=outFreq;
		output out=mycas.over copyvars=(_all_);
	run;

	/*data partition after sampling*/
	proc partition data=mycas.over partition samppct=70 SEED=5;
		by &target;
		output out=mycas.part copyvars=(_ALL_) partindname=_PartInd_;
	run;

	/*move the data from in memory to the WORK library on disk.  part&i. is the same as WORK.part&i.*/
	data part&i.;
		set mycas.part;
	run;
%mend bootstrap;

%bootstrap;

dm log "clear";

/* Recommanded number of models */
/***********************************************************/

data ul_tp_ratios;
	input index ratio;
	datalines;
1 3
2 5
3 10
;
run;

%let majcov=0.8;

data number_models;
	attrib index length=8 ratio length=8 number_models length=8;
	stop;
run;

%macro number_model(data=mycas.training, majcov=0.8, ultprt=ul_tp_ratios);

	proc sql noprint;
		select count(*) into :N_ratio separated by ' ' from &ultprt;
	quit;

	%do i=1 %to &N_ratio;

        %let iterator = %sysevalf(0);

		data _null_;
			set ul_tp_ratios;
			if index=&i.;
			call symput('ratio', ratio);
		run;

		data train_data;
			set &data;
		run;

		data train_data_ul_or;
			set train_data;
			if target=0;
		run;

		data train_data_ul;
			set train_data;
			if target=0;
		run;

		data train_data_tp;
			set train_data;
			if target=1;
		run;

		proc sql noprint;
			select count(*) into :N separated by ' ' from train_data_ul;
		quit;

		proc sql noprint;
			select count(*) into :N1 separated by ' ' from train_data_tp;
		quit;

		%let M = %sysevalf(&N);

		%DO %WHILE (%sysevalf(1.0 - (&N / &M)) lt &majcov);

			proc surveyselect data=train_data_ul_or method=urs seed=&iterator out=train1 
					sampsize=%sysevalf(&ratio*&N1);
			run;

			data train1;
				set train1 (drop=numberhits);
			run;

			proc fedsql;
				create table work.btsp as select * from work.train1 union select * from 
					work.train_data_tp;
				quit;

			data mycas.btsp;
				set btsp;
			run;

			proc partition data=mycas.btsp partition samppct=70 seed=303;
				output out=mycas.btsp copyvars=(_ALL_);
			run;

			data train1;
				set mycas.btsp;

				if _partind_=1;
				drop _partind_;
			run;

			proc fedsql;
				create table train2 as select * from train_data_ul except all select * from 
					train1;
				quit;

			data train_data_ul;
				set train2;
			run;

			proc sql noprint;
				select count(*) into :N separated by ' ' from train_data_ul;
			quit;

			%let iterator = %sysevalf(1.0 + &iterator);

			proc delete data=train2;
			run;

			proc delete data=btsp;
			run;

		%END;

		%if %sysfunc(mod(&iterator, 2)) = 0 %then %do;
				%let iterator = %sysevalf(1.0 + &iterator);
			%end;

		proc sql;
			insert into work.number_models set index=&i., ratio=&ratio., 
				number_models=&iterator.;
		quit;

	%end;
%mend number_model;

%number_model;



/*Create the c and gamma combinations in a SAS dataset.The index
is simply an ID that makes it easier to reference SVM parameter combinations.
Such data could be imported from an excel sheet or csv*/ 
data svmparms;
	input index c gamma;
	datalines;
1 0.01 0.01
2 0.01 0.1
3 0.01 1
4 0.01 10
5 0.01 100
6 0.1 0.01
7 0.1 0.1
8 0.1 1
9 0.1 10
10 0.1 100
11 1 0.01
12 1 0.1
13 1 1
14 1 10
15 1 100
16 10 0.01
17 10 0.1
18 10 1
19 10 10
20 10 100
21 100 0.01
22 100 0.1
23 100 1
24 100 10
25 100 100
;
run;

/*Create the c and penalty parameters combinations in a SAS dataset.The index
is simply an ID that makes it easier to reference logreg parameter combinations.
Such data could be imported from an excel sheet or csv*/ 

data logregparms;
	input index c;
	datalines;
1 0.1
2 1
3 10
4 25
5 100
;
run;

/*Create the depth, number of tree and eta combinations in a SAS dataset.The index
is simply an ID that makes it easier to reference GBOOST parameter combinations.
Such data could be imported from an excel sheet or csv*/ 
data gboostparms;
	input index dpth nb_tr eta;
	datalines;
1 1 100 0.1
2 1 100 0.25
3 1 100 0.5
4 1 100 1
5 1 250 0.1
6 1 250 0.25
7 1 250 0.5
8 1 250 1
9 1 500 0.1
10 1 500 0.25
11 1 500 0.5
12 1 500 1
13 2 100 0.1
14 2 100 0.25
15 2 100 0.5
16 2 100 1
17 2 250 0.1
18 2 250 0.25
19 2 250 0.5
20 2 250 1
21 2 500 0.1
22 2 500 0.25
23 2 500 0.5
24 2 500 1
25 3 100 0.1
26 3 100 0.25
27 3 100 0.5
28 3 100 1
29 3 250 0.1
30 3 250 0.25
31 3 250 0.5
32 3 250 1
33 3 500 0.1
34 3 500 0.25
35 3 500 0.5
36 3 500 1
37 4 100 0.1
38 4 100 0.25
39 4 100 0.5
40 4 100 1
41 4 250 0.1
42 4 250 0.25
43 4 250 0.5
44 4 250 1
45 4 500 0.1
46 4 500 0.25
47 4 500 0.5
48 4 500 1
;
run;


/*In SAS a macro is first compiled --that is everything between the %macro logreg and the %mend at the bottom.
Compiling a macro does not execute it but just makes it ready to execute. So everytime you start a session
you would run the code between these two commands, and only do it once. From then on all you need to do 
is call the macro, and pass parameters to it (%logreg) if different than the defaults you compile. */


/***********************************************************************/
/**************************** SVM *************************************/
/**************************** SVM *************************************/
/**************************** SVM *************************************/
/***********************************************************************/

/*define the svm_ensemble table that is going to have all the chosen models*/

data svm_ensemble;
	attrib metric length=$20 index length=8 c length=8 gamma length=8
    cutoff length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro SVM(data=mycas.training, target=target, IntIn=V1-V28 Amount, metric=precision, num_models=5);
	/*Delete various work files before running*/

	    proc datasets;
		delete part: best&metric.: svm_roc: out: valassess: data: i_:;
		run;

		/*Outer do loop over 5 'oversamples', varying the seed in each (seed = &i)*/
				
		%do i=1 %to &num_models;

        %bootstrap(data=&data, target=&target, i=&i.);

		/*create the table to store the metric value for each of the 25 combination of C and gamma*/
		data &metric._store;
			attrib c length=8 gamma length=8 cutoff length=8 f1 length=8 precision length=8 recall length=8;
			stop;
		run;

		/*Inner Do Loop to pass SVM parameters to the routine for each of the 5 samples created above.
		Use the parameters dataset created above (svmparms data) to pass various parameter combinations
		to the HPSVM procedure*/ 		
				
		
		%do index=1 %to 25;

			/*Create a macro variable from a value in a data set*/
			data _null_;
				set svmparms;

				if index=&index.;
				call symput('c', c);
				call symput('kpar', gamma);
			run;

			proc hpsvm data=part&i. method=activeset c=&c.;
				where _partind_=1;
				kernel rbf / k_par=&kpar.;
				*input &CatIn./level=nominal;
				input &IntIn.;
				target &target;
				*output outclass=outclass outfit=outfit outest=outest;
				savestate file="/users/eybmotmp/casuser/svm&i._&index.";
                *savestate rstore=mycas.svm&i._&index.;
				*id &id.;
			run;

			/**** Scoring the data.  NOTE AND CHANGE THE PATH to location on server
			/users/eybmotmp/casuser/ ***********************/
			proc astore;
				score data=part&i. out=out_S&i._P&index. store="/users/eybmotmp/casuser/svm&i._&index."
					copyvars=(_ALL_);
			run;

 			/*Assessment procedure for binary or interval targets run on the scored table*/
			data mycas.assess (keep=&target p_&target.1 p_&target.0 i_&target _partind_);
				set out_S&i._P&index.;
                where _partind_=0;
			run;

			ods exclude all;

			proc assess data=mycas.assess maxiter=50 nbins=2 ncuts=10;
				input p_&target.1;
				target &target / level=nominal event='1';
				fitstat pvar=p_&target.0/ pevent='0';
				by _partind_;
				ods output /*fitstat=svm_fitstat*/
				rocinfo=svm_roc /*liftinfo=svm_liftinfo*/;
			run;

			ods exclude none;

			/*Precision & Recall*/
			data svm_roc_S&i._P&index.;
				set svm_roc;
				Precision=TP/(TP+FP);
				Recall=sensitivity;
			run;

			/*Extract and store the optimal metric statistic on validation data, and the cutoff that yielded it*/
			proc sql;
				select cutoff into :cutoff from svm_roc_S&i._P&index.
                having &metric=max(&metric);
			quit;

			data best&metric._S&i._P&index.;
				set svm_roc_S&i._P&index.;
				if cutoff-0.001 lt &cutoff. lt cutoff+0.001;
			run;

			data _null_;
				set best&metric._S&i._P&index.;
				call symput('f1', f1);
                call symput('precision', precision);
                call symput('recall', recall);
			run;

			proc sql;
				insert into work.&metric._store set c=&c., gamma=&kpar., cutoff=&cutoff., f1=&f1.,
                precision=&precision., recall=&recall.;
			quit;

            proc datasets;
	    	delete out_S&i._P&index.;
		    run;

            proc datasets;
	    	delete svm_roc_S&i._P&index.;
		    run;

     		/*End of outer do loop*/	
		%end;

		proc sql;
			select c, gamma, cutoff, precision, recall into :c, :gamma, :cutoff, :precision, :recall from &metric._store having 
				&metric=max(&metric);
		quit;

		proc sql;
			insert into work.svm_ensemble set metric="&metric", index=&i., c=&c., gamma=&kpar., 
				cutoff=&cutoff., f1=&f1., precision=&precision., recall=&recall.;
		quit;

		/*End of outer do loop*/
	%end;

	/*throw out additional datasets you dont need*/
	proc datasets;
		delete data: best&metric.: svm_roc;
		run;

	proc delete data=mycas.assess;
	run;

	/*Macro to be compiled ends at the %mend */
%mend svm;

/*Call and run the macro (%svm). I didnt change any defaults so didnt include new macro parameters*/
/*%let CatIn= ;*/
%svm;
%svm(data=mycas.training, target=target, IntIn=V1-V28 Amount, metric=f1);


/***********************************************************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/***********************************************************************/


/*define the logreg_ensemble table that is going to have all the chosen models*/

data logreg_ensemble;
	attrib metric length=$20 index length=8 c length=8 cutoff length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro logreg(data=mycas.training, target=target, IntIn=V1-V28 Amount, 
		metric=precision, num_models=5);
	/*Delete various work files before running*/
	proc datasets;
		delete part: best&metric.: logreg_roc: out: valassess: data: i_:;
		run;

		/*Outer do loop over 5 'oversamples', varying the seed in each (seed = &i)*/
				
		
		%do i=1 %to &num_models;
			%bootstrap(data=&data, target=&target, i=&i.);

			/*create the table to store the metric value for each of the 25 combination of C and gamma*/
		data &metric._store;
			attrib c length=8 cutoff length=8 f1 length=8 precision length=8 recall 
				length=8;
			stop;
		run;

		/*Inner Do Loop to pass logreg parameters to the routine for each of the 5 samples created above.
		Use the parameters dataset created above (logregparms data) to pass various parameter combinations
		to the HPlogreg procedure*/ 		
				
		
		
		%do index=1 %to 25;

			/*Create a macro variable from a value in a data set*/
			data _null_;
				set logregparms;

				if index=&index.;
				call symput('c', c);
			run;

			proc logselect data=mycas.part(where=(_partind_=1)) lassorho=&c.;
				model &target.(event='1')=&IntIn.;
				code file="&outdir./logselect_score.sas" pcatall;
			run;

			/**** Scoring the data.  NOTE AND CHANGE THE PATH to location on server
			/users/eybmotmp/casuser/ ***********************/
			data mycas.out_S&i._P&index.;
				set mycas.part (where=(_partind_=0));
				%include "&outdir./logselect_score.sas";
			run;

			/*Assessment procedure for binary or interval targets run on the scored table*/
			data mycas.assess (keep=&target p_&target.1 p_&target.0 i_&target _partind_);
				set mycas.out_S&i._P&index.;
				where _partind_=0;
			run;

			ods exclude all;

			proc assess data=mycas.assess;
				input p_&target.1;
				target &target / level=nominal event='1';
				fitstat pvar=p_&target.0/ pevent='0';
				ods output rocinfo=logit_rocinfo;
			run;

			ods exclude none;

			/*Precision & Recall*/
			data logreg_roc_S&i._P&index.;
				set logit_rocinfo;
				Precision=TP/(TP+FP);
				Recall=sensitivity;
			run;

			/*Extract and store the optimal metric statistic on validation data, and the cutoff that yielded it*/
			proc sql;
				select cutoff into :cutoff from logreg_roc_S&i._P&index.
                having &metric=max(&metric);
			quit;

			data best&metric._S&i._P&index.;
				set logreg_roc_S&i._P&index.;

				if cutoff-0.001 lt &cutoff. lt cutoff+0.001;
			run;

			data _null_;
				set best&metric._S&i._P&index.;
				call symput('f1', f1);
				call symput('precision', precision);
				call symput('recall', recall);
			run;

			proc sql;
				insert into work.&metric._store set c=&c., cutoff=&cutoff., f1=&f1., 
					precision=&precision., recall=&recall.;
			quit;

			proc delete data=mycas.out_S&i._P&index.;
			run;

			proc datasets;
				delete logreg_roc_S&i._P&index.;
				run;

				/*End of outer do loop*/	
		%end;

		proc sql;
			select c, gamma, cutoff, precision, recall into :c, :gamma, :cutoff, 
				:precision, :recall from &metric._store having 
				&metric=max(&metric);
		quit;

		proc sql;
			insert into work.logreg_ensemble set metric="&metric", index=&i., c=&c., 
				gamma=&kpar., cutoff=&cutoff., f1=&f1., precision=&precision., 
				recall=&recall.;
		quit;

		/*End of outer do loop*/
	%end;

	/*throw out additional datasets you dont need*/
	proc datasets;
		delete data: best&metric.: logreg_roc;
		run;

	proc delete data=mycas.assess;
	run;

	/*Macro to be compiled ends at the %mend */
%mend logreg;

/*Call and run the macro (%logreg). I didnt change any defaults so didnt include new macro parameters*/
/*%let CatIn= ;*/
%logreg;
%logreg(data=mycas.training, target=target, IntIn=V1-V28 Amount, metric=f1);
/*

/**************************************************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/**************************************************/


/*define the gboost_ensemble table that is going to contain all the chosen models*/
data gboost_ensemble;
	attrib metric length=$20 index length=8 dpth length=8 nb_tr length=8 eta length=8 
    cutoff length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro gboost(data=mycas.training, target=target, IntIn=V1-V28 Amount, metric=precision, num_models=5);
	/*Delete various work files before running*/

	    proc datasets;
		delete part: best&metric.: gboost_roc: out: data: i_:;
		run;

		/*Outer do loop over 5 'oversamples', varying the seed in each (seed = &i)*/
				
		%do i=1 %to &num_models;
        
        bootstrap(data=&data, target=&target, i=%sysevalf(&i));

		/*create the table to store the metric value for each of the 25 combination of C and gamma*/
		data &metric._store;
			attrib dpth length=8 nb_tr length=8 eta length=8 cutoff length=8 f1 length=8 precision length=8 recall length=8;
			stop;
		run;

		/*Inner Do Loop to pass GBOOST parameters to the routine for each of the 5 samples created above.
		Use the parameters dataset created above (gboostparms data) to pass various parameter combinations
		to the GRADBOOST procedure*/ 		
				
		
		%do index=1 %to 25;

			/*Create a macro variable from a value in a data set*/
			data _null_;
				set gboostparms;
				if index=&index.;
				call symput('dpth', dpth);
				call symput('nb_tr', nb_tr);
                call symput('eta', eta);
			run;

           /* Gradboost model */

			proc gradboost data=mycas.part ntrees=&nb_tr intervalbins=20 maxdepth=&dpth learningrate=&eta;
			  input &IntIn.;
			  target &target;
			  partition rolevar=_partind_(train='1' validate='0');
              savestate rstore=mycas.gboost&i._&index.;
			run;

			/**** Scoring the data***********************/
			proc astore;
				score data=mycas.part out=mycas.out_S&i._P&index. rstore=mycas.gboost&i._&index.
					copyvars=(_ALL_);
			run;

 			/*Keep the observations in the validation set*/
			data mycas.assess (keep=&target p_&target.1 p_&target.0 i_&target);
				set mycas.out_S&i._P&index.;
                where _partind_=0;
			run;

			ods exclude all;

			proc assess data=mycas.assess maxiter=50 nbins=2 ncuts=10;
				input p_&target.1;
				target &target / level=nominal event='1';
				fitstat pvar=p_&target.0/ pevent='0';
				ods output 
				rocinfo=gboost_roc ;
			run;

			ods exclude none;

			/*Precision & Recall*/
			data gboost_roc_S&i._P&index.;
				set gboost_roc;
				Precision=TP/(TP+FP);
				Recall=sensitivity;
			run;

			/*Extract and store the optimal metric statistic on validation data, and the cutoff that yielded it*/
			proc sql;
				select cutoff into :cutoff from gboost_roc_S&i._P&index.
                having &metric=max(&metric);
			quit;

			data best&metric._S&i._P&index.;
				set gboost_roc_S&i._P&index.;
				if cutoff-0.001 lt &cutoff. lt cutoff+0.001;
			run;

			data _null_;
				set best&metric._S&i._P&index.;
				call symput('f1', f1);
                call symput('precision', precision);
                call symput('recall', recall);
			run;

			proc sql;
				insert into work.&metric._store set dpth=&dpth., nb_tr=&nb_tr., eta=&eta., cutoff=&cutoff., f1=&f1.,
                precision=&precision., recall=&recall.;
			quit;

            proc delete data=mycas.out_S&i._P&index.;
	        run;

            proc delete data=mycas.gboost&i._&index.;
	        run;

            proc datasets;
	    	delete gboost_roc_S&i._P&index.;
		    run;

     		/*End of outer do loop*/	
		%end;

		proc sql;
			select dpth, nb_tr, eta, cutoff, precision, recall into :dpth, :nb_tr, :eta, :cutoff, :precision, :recall from &metric._store having 
				&metric=max(&metric);
		quit;

		proc sql;
			insert into work.gboost_ensemble set metric="&metric", index=&i., dpth=&dpth., nb_tr=&nb_tr., eta=&eta., 
            cutoff=&cutoff., f1=&f1., precision=&precision., recall=&recall.;
		quit;

		/*End of outer do loop*/
	%end;

	/*throw out additional datasets you dont need*/
	proc datasets;
		delete data: best&metric: gboost_roc;
		run;

	proc delete data=mycas.assess;
	run;

	/*Macro to be compiled ends at the %mend */
%mend gboost;

/*Call and run the macro (%gboost). I didnt change any defaults so didnt include new macro parameters*/
/*%let CatIn= ;*/
%gboost;
%gboost(data=mycas.training, target=target, IntIn=V1-V28 Amount, metric=recall);
