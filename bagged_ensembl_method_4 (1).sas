option castimeout=max;

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/************************************************************************/
cas mySession sessopts=(caslib=casuser timeout=36000 locale="en_US" metrics=true);

 
/*DEFINE CAS ENGINE LIBREF FOR CAS IN-MEMORY TABLES*/  
libname mycas cas caslib=casuser datalimit=100G;

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

/* Bootstrap macro: We use partition here that does not allow for sampling with replacement. 
We could have used surveyselect, but it does not run on CAS */

%let ratio=3; /* unlabelled to true positive ratio */

%macro bootstrap(data=mycas.training, target=target, ul_tp = &ratio, i=1);

	data cd_ul cd_tp;
		set &data.;	
		if &target.=1 then output cd_tp;
		else output cd_ul;
	run;
	
	proc sql noprint;
		select count(*) into :N separated by ' ' from cd_tp;
	quit;
	
	proc surveyselect data=work.cd_ul method=urs seed=&i out=work.train1 outhits sampsize=%sysevalf(&ratio*&N);
	run;

	data mycas.btsp;
		set train1(drop=numberhits) cd_tp;
	run;

	/*data partition after sampling*/	
	proc partition data=mycas.btsp partition samppct=70 seed=303;
		output out=mycas.part copyvars=(_ALL_);
	run;

%mend bootstrap;


/* Macro for Recommanded number of models. We use surveyselect (does not run in CAS) in this code.
We will do an update when a CAS alternative will be found*/

%let majcov=0.8;

%macro number_model(data=mycas.training, majcov=0.8, target=target, ul_tp=&ratio.);

    %let iterator = %sysevalf(0);

	data train_data_ul_or train_data_ul train_data_tp;
		set &data.;
		if &target.=0 then output train_data_ul_or;
        if &target.=0 then output train_data_ul;
        if &target.=1 then output train_data_tp;
	run;

	proc sql noprint;
		select count(*) into :N separated by ' ' from train_data_ul;
	quit;

	proc sql noprint;
		select count(*) into :N1 separated by ' ' from train_data_tp;
	quit;

	%let M = %sysevalf(&N);

	%DO %WHILE (%sysevalf(1.0 - (&N / &M)) lt &majcov);

		proc surveyselect data=train_data_ul_or method=urs seed=&iterator out=train1 outhits sampsize=%sysevalf(&ul_tp*&N1);
		run;

		data mycas.btsp;
			set train1(drop=numberhits) train_data_tp;
		run;

		proc partition data=mycas.btsp partition samppct=70 seed=303;
			output out=mycas.btsp copyvars=(_ALL_);
		run;

		data train1(drop=_partind_);
			set mycas.btsp;
			if _partind_=1;
		run;

		proc sql;
			create table train2 as select * from train_data_ul except all select * from train1;
		quit;

		data train_data_ul;
			set train2;
		run;

		proc sql noprint;
			select count(*) into :N separated by ' ' from train_data_ul;
		quit;

		%let iterator = %sysevalf(1.0 + &iterator);

	%END;

	%if %sysfunc(mod(&iterator, 2))=0 %then
		%do;
			%let iterator = %sysevalf(1.0 + &iterator);
		%end;

    %global final_num_models;
    %let final_num_models = &iterator;

%mend number_model;

* %number_model(data=mycas.training, majcov=0.8, ultprt=5);

/*Create the c and gamma combinations in a CAS dataset.The index
is simply an ID that makes it easier to reference SVM parameter combinations */ 

data mycas.svmparms;
	retain index 0;
	Do c=0.01, 0.1, 1, 10, 100;
		Do gamma=0.01, 0.1, 1, 10, 100;
			index=index+1;
			output;
		end;
	end;
run;

proc sql noprint;
	select count(*) into :N_svm_par separated by ' ' from mycas.svmparms;
quit;

/*Create the c and penalty parameters combinations in a CAS dataset.The index
is simply an ID that makes it easier to reference logreg parameter combinations.
The c parameter here is adapted to the Logselect proc in CAS. It is not the 
same c as the one in the python code*/ 

data mycas.logregparms;
	retain index 0;
	Do c=0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9;
		index=index+1;
		output;
	end;
run;

proc sql noprint;
	select count(*) into :N_logreg_par separated by ' ' from mycas.logregparms;
quit;


/* Create the depth, number of tree and eta combinations in a SAS dataset.The index
is simply an ID that makes it easier to reference GBOOST parameter combinations */ 

data mycas.gboostparms;
	retain index 0;
	do dpth=1, 2, 3, 4;
		do nb_tr=100, 250, 500;
			do eta=0.1, 0.25, 0.5, 1;
				index=index+1;
				output;
			end;
		end;
	end;
run;

proc sql noprint;
	select count(*) into :N_gboost_par separated by ' ' from mycas.gboostparms;
quit;

/* Macro to compute the metrics */ 

%macro metric_compute(data=mycas.assess, target=target, pred_target=i_target);
	data assess1;
		set &data;
		if &target='1' and &pred_target='1' then    tp=1; else  tp=0;
		if &target='1' and &pred_target='0' then    fn=1; else	fn=0;
		if &target='0' and &pred_target='1' then	fp=1; else	fp=0;
		if &target='0' and &pred_target='0' then	tn=1; else	tn=0;
	run;

	proc summary data=assess1;
		var tp fn fp tn;
		output out=totals sum=;
	run;

	data mycas.metrics;
		set totals;
		precision=TP/(TP+FP);
		recall=TP/(TP+FN);
		f1=(2*TP)/(2*TP+FN+FP);
	run;
%mend metric_compute;

/***********************************************************************/
/**************************** SVM *************************************/
/**************************** SVM *************************************/
/**************************** SVM *************************************/
/***********************************************************************/

/*Define the svm_ensemble table that is going to contain all the chosen models*/

data svm_ensemble;
	attrib metric length=$20 index length=8 c length=8 gamma length=8
    f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro svm(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=precision, iter=1);

	/*create the table to store the metric value for each of hyperparameters combinations*/
	data &metric._store;
		attrib index length=8 c length=8 gamma length=8 f1 length=8 precision length=8 recall length=8;
		stop;
	run;

	/*Do Loop to pass svm parameters. Use the parameters dataset created above (mycas.svmparms data)
	to pass various parameter combinations to the HPSVM procedure*/ 		
	
	%do index=1 %to &N_svm_par;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.svmparms;
			if index=&index.;
			call symput('c', c);
            call symput('gamma', gamma);
		run;

		/* HPSVM model */

			proc hpsvm data=&data. method=activeset c=&c.;
				kernel rbf / k_par=&gamma.;
				/*input &CatIn./level=nominal;*/
				input &IntIn.;
				target &target;
				output out=mycas.outdata copyvars=(_partind_ &target);
                partition rolevar=_partind_(train='1' validate='0');
			run;


		/**** Scoring the data***********************/
		data mycas.valid;
			set mycas.outdata;
			where _partind_=0;
		run;

        /*Computation of the three metrics*/

		%metric_compute(data=mycas.valid, target=&target., pred_target=i_&target.);
		
        /* Store the metrics in table work.&metric._store*/
		data _null_;
			set mycas.metrics;
			call symput('f1', f1);
			call symput('precision', precision);
			call symput('recall', recall);
		run;

		proc sql;
			insert into work.&metric._store set index=&index., c=&c., gamma=&gamma., f1=&f1., precision=&precision., recall=&recall.;
		quit;

		/*End of outer do loop*/	
		%end;
    
   /* Find the best model and save it */
	proc sql;
		select index, c, gamma, f1, precision, recall into :index, :c, :gamma, :f1, :precision, :recall from &metric._store having 
				&metric=max(&metric);
	quit;

	proc sql;
		insert into work.svm_ensemble set index=&iter., metric="&metric", 
			c=&c., gamma=&gamma., f1=&f1., precision=&precision., recall=&recall.;
	quit;

	proc hpsvm data=&data. method=activeset c=&c.; /*Save the chosen model*/
		kernel rbf / k_par=&gamma.;
		input &IntIn.;
		target &target;
		savestate file="&outdir.svm_&metric._&iter..sas";
        /*savestate rstore=mycas.svm_&metric._&iter.;*/
	run;
	/*Macro to be compiled ends at the %mend */
%mend svm;


/***********************************************************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/***********************************************************************/


/*Define the logreg_ensemble table that is going to have all the chosen models*/

data logreg_ensemble;
	attrib index length=8 metric length=$20 c length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro logreg(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=precision, iter=1);
	
	/*create the table to store the metric value for each of hyperparameters combinations*/
	data &metric._store;
		attrib index length=8 c length=8 f1 length=8 precision length=8 recall length=8;
		stop;
	run;

	/*Do Loop to pass logreg parameters. Use the parameters dataset created above (mycas.logregparms data)
	to pass various parameter combinations to the LOGSELECT procedure*/ 		
			
	%do index=1 %to &N_logreg_par;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.logregparms;
			if index=&index.;
			call symput('c', c);
		run;

		/* Logselect model */

		proc logselect data=&data. lassorho=&c.;
			model &target.(event='1')=&IntIn.;
			output out=mycas.outdata copyvars=(_partind_ &target ID);
			/*code file="&outdir./logselect_score.sas" pcatall;*/
			partition rolevar=_partind_(train='1' validate='0');
		run;

		/**** Scoring the data***********************/
		data mycas.valid;
			set mycas.outdata;
			where _partind_=0;
            p_&target.1=_pred_;
            p_&target.0=1-_pred_;
            if _pred_>=0.5 then i_&target. = '1'; else i_&target. = '0';
		run;

        /*Computation of the three metrics*/

		%metric_compute(data=mycas.valid, target=&target., pred_target=i_&target.);
		
        /* Store the metrics in table work.&metric._store*/
		data _null_;
			set mycas.metrics;
			call symput('f1', f1);
			call symput('precision', precision);
			call symput('recall', recall);
		run;

		proc sql;
			insert into work.&metric._store set index=&index., c=&c., f1=&f1., precision=&precision., recall=&recall.;
		quit;

		/*End of outer do loop*/	
		%end;

    /* Find the best model and store it */
	proc sql;
		select index, c, f1, precision, recall into :index, :c, :f1, :precision, :recall from &metric._store having 
				&metric=max(&metric);
	quit;

	proc sql;
		insert into work.logreg_ensemble set index=&iter., metric="&metric", 
			c=&c., f1=&f1., precision=&precision., recall=&recall.;
	quit;

    ods output ParameterEstimates=Coeff_&metric._&iter.(keep=effect estimate rename= (estimate=estimate_&iter.)); /* save the chosen model coefficients estimates*/
	proc logselect data=&data. lassorho=&c.; /* save the chosen model*/
		model &target.(event='1')=&IntIn.;
		*output out=mycas.outdata copyvars=(_partind_ &target);
		code file="&outdir.logreg_&metric._&iter..sas" pcatall;
  		partition rolevar=_partind_(train='1' validate='0');
	run;

	proc sort data=Coeff_&metric._1; by effect;	run;

    proc sort data=Coeff_&metric._&iter.; by effect;	run;

	data Coeff_&metric._1;		/* Store logreg coefficients estimates*/
		merge Coeff_&metric._1 Coeff_&metric._&iter.;
        by effect;
	run;

	/*Macro to be compiled ends at the %mend */
%mend logreg;

/**************************************************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/**************************************************/


/*Define the gboost_ensemble table that is going to contain all the chosen models*/
data gboost_ensemble;
	attrib index length=8 metric length=$20 dpth length=8 nb_tr length=8 eta length=8 
    f1 length=8 precision length=8 recall length=8;
	stop;
run;

%macro gboost(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=precision, iter=1);
	
	/*create the table to store the metric value for each of the combination of depth, 
    number of trees and learning rate*/
	data &metric._store;
		attrib index length=8 dpth length=8 nb_tr length=8 eta length=8 f1 length=8 
			precision length=8 recall length=8;
		stop;
	run;

	/*Do Loop to pass GBOOST parameters. Use the parameters dataset created above (mycas.gboostparms data)
	to pass various parameter combinations to the GRADBOOST procedure*/ 		
		
	
	%do index=1 %to &N_gboost_par;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.gboostparms;
			if index=&index.;
			call symput('dpth', dpth);
			call symput('nb_tr', nb_tr);
			call symput('eta', eta);
		run;

		/* Gradboost model */
		proc gradboost data=&data ntrees=&nb_tr intervalbins=20 
				maxdepth=&dpth learningrate=&eta;
			output out=mycas.outdata copyvars=(_partind_ &target);
			input &IntIn.;
			target &target;
			partition rolevar=_partind_(train='1' validate='0');
			*savestate rstore=mycas.gboost_&index.;
		run;

		/**** Scoring the data***********************/
		data mycas.valid;
			set mycas.outdata;
			where _partind_=0;
		run;

        /*Computation of the three metrics*/
		%metric_compute(data=mycas.valid, target=&target., pred_target=i_&target.);

		/* Store the metrics in table work.&metric._store*/
		data _null_;
			set mycas.metrics;
			call symput('f1', f1);
			call symput('precision', precision);
			call symput('recall', recall);
		run;

		proc sql;
			insert into work.&metric._store set index=&index., dpth=&dpth., 
				nb_tr=&nb_tr., eta=&eta., f1=&f1., precision=&precision., recall=&recall.;
		quit;

		/*End of outer do loop*/	
		%end;

    /* Find the best model and store it */
	proc sql;
		select index, dpth, nb_tr, eta, f1, precision, recall into :index, :dpth, :nb_tr, 
			:eta, :f1, :precision, :recall from &metric._store having 
				&metric=max(&metric);
	quit;

	proc sql;
		insert into work.gboost_ensemble set index=&iter., metric="&metric", 
			dpth=&dpth., nb_tr=&nb_tr., eta=&eta., f1=&f1., precision=&precision., 
			recall=&recall.;
	quit;

	proc gradboost 
			data=&data ntrees=&nb_tr intervalbins=20 /* save the chosen model*/
			maxdepth=&dpth learningrate=&eta;
		input &IntIn.;
		target &target;
		partition rolevar=_partind_(train='1' validate='0');
		savestate rstore=mycas.gboost_&metric._&iter.;
	run;

	/*Macro to be compiled ends at the %mend */
%mend gboost;

/* Create tables to store chosen models parameters*/ 
data svm_ensemble;
	attrib metric length=$20 index length=8 c length=8 gamma length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

data logreg_ensemble;
	attrib index length=8 metric length=$20 c length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

data gboost_ensemble;
	attrib index length=8 metric length=$20 dpth length=8 nb_tr length=8 eta length=8 f1 length=8 precision length=8 recall length=8;
	stop;
run;

/* Create metrics table*/ 

data mycas.eval_metric;
   	retain index 0;
	Do metric='precision ', 'f1', 'recall';
		index=index+1;
		output;
	end;
run;

/* Macro creating the ensemble for all three kind of moldes */

%macro ensemble_creation(ul_tp=&ratio., metrics=mycas.eval_metric);
    %let _timer_start = %sysfunc(datetime());
	%number_model(data=mycas.training, majcov=0.8, target=target, ul_tp=&ul_tp.); /* Gives the number of recommanded models for
		majcov=0.8 and ratio=ul_tp and this number is stored in macro variable final_num_models.*/
	data _null_;
		dur=datetime() - &_timer_start;
		put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
	run;
		
    proc sql noprint;
	select count(*) into :N_metrics separated by ' ' from &metrics.;
	quit;

	%do k=1 %to &N_metrics;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set &metrics.;
			if index=&k.;
			call symput('metr', metric);
		run;

		/*%do j=1 %to 2;*/
        %do j=1 %to &final_num_models.;
            %let _timer_start = %sysfunc(datetime());
            %bootstrap(data=mycas.training, target=target, ul_tp=&ul_tp., i=%sysevalf(&j.+ &k.*&final_num_models.));/* create sample mycas.part that will
			be used to find ensemble elements */

		    %SVM(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
			%logreg(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
			%gboost(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
			data _null_;
				dur=datetime() - &_timer_start;
				put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
			run;
		%end;
	%end;
%mend ensemble_creation;

%ODSOff(); /** Turn off displays in log file so that the software won't crash **/
%ensemble_creation;



/* Model evaluation */

%macro svm_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, 
ensemble=svm_ensemble, metric=f1, ul_tp=&ratio.);
    
	data mycas.&metric._svm_models;
		set &ensemble;
		where metric="&metric.";
	run;

	proc sql noprint;
		select count(*) into :N separated by ' ' from mycas.&metric._svm_models;
	quit;

    data mycas.&metric._svm_eval; /* selct ID and target columns from the test data */
	    set &data (keep=ID target);
    run;

    data mycas.svm_ens;
	    set mycas.&metric._svm_models;
        id= _n_;
    run;

	%do j=1 %to &N;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.svm_ens;
			if id=&j.;
			call symput('i', index);
		run;

		/**** Scoring the data***********************/

		proc astore;
			score data=&data. out=out_&j. store="&outdir.svm_&metric._%sysevalf(&i.).sas" 
				copyvars=(_ALL_);
		run;

		data mycas.out_&j.;
			set out_&j.;
            keep ID p_&target.1 p_&target.0 i_&target. &target.;
			rename p_&target.0 = p&target.0_svm_&j. p_&target.1=p&target.1_svm_&j. i_&target.=i&target._svm_&j.;
		run;

		data mycas.&metric._svm_eval;
			merge mycas.&metric._svm_eval mycas.out_&j.;
			by ID;
		run;

	%end;

    data mycas.&metric._svm_eval;
		set mycas.&metric._svm_eval;
        /*p&target.1_svm_Avg=mean(of p&target.1_svm_1-p&target.1_svm_&N.);*/
        i&target._svm_Avg=mean(of i&target._svm_1-i&target._svm_&N);
        /*if p&target.1_svm_Avg>=0.5 then svm_soft_vote = '1'; else svm_hard_vote = '0';*/
        if i&target._svm_Avg>=0.5 then svm_hard_vote = '1'; else svm_hard_vote = '0';
        keep ID &target svm_hard_vote;
	run;

   %metric_compute(data=mycas.&metric._svm_eval, target=target, pred_target=svm_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results1;
		set mycas.metrics;
		ensemble="svm_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Stor evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results1;
	run;
   
%mend svm_eval;


%macro logreg_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, 
ensemble=logreg_ensemble, metric=f1, ul_tp=&ratio.);
    
	data mycas.&metric._logreg_models;
		set &ensemble;
		where metric="&metric.";
	run;

	proc sql noprint;
		select count(*) into :N separated by ' ' from mycas.&metric._logreg_models;
	quit;

    data mycas.&metric._logreg_eval; /* selct ID and target columns from the test data */
	    set &data (keep=ID target);
    run;

    data mycas.logreg_ens;
	    set mycas.&metric._logreg_models;
        id= _n_;
    run;

	%do j=1 %to &N;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.logreg_ens;
			if id=&j.;
			call symput('i', index);
		run;

		/**** Scoring the data***********************/
		data mycas.out_&j.;
			set &data;
			%include "&outdir.logreg_&metric._%sysevalf(&i.).sas";
            keep ID p_&target.1 p_&target.0 i_&target;
			rename p_&target.0 = p&target.0_logreg_&j. p_&target.1=p&target.1_logreg_&j. i_&target.=i&target._logreg_&j.;
		run;

        data mycas.&metric._logreg_eval;
			merge mycas.&metric._logreg_eval mycas.out_&j.;
            by ID;
		run;
	%end;

    /* Hard vote and Soft vote */
    data mycas.&metric._logreg_eval;
		set  mycas.&metric._logreg_eval;
        p&target.1_logreg_Avg=mean(of p&target.1_logreg_1-p&target.1_logreg_&N.);
        i&target._logreg_Avg=mean(of i&target._logreg_1-i&target._logreg_&N);
        if p&target.1_logreg_Avg>=0.5 then logreg_soft_vote = '1'; else logreg_soft_vote = '0';
        if i&target._logreg_Avg>=0.5 then logreg_hard_vote = '1'; else logreg_hard_vote = '0';
        keep ID &target logreg_hard_vote logreg_soft_vote;
	run;
     
    /* Mean coefficient */
	data mycas.Coeff_&metric.(keep=effect mean_coef); /* Store logreg coefficients estimates*/
		set Coeff_&metric._1; mean_coef=mean(of estimate_1-estimate_&N.);
	run;

	Data B(keep=Intercept Amount V1-V28);
		set mycas.test;	intercept=1;
	run;
	
	proc sql;
		select effect into :my_column_order separated by " " from mycas.coeff_&metric. order by effect;
	quit;
	
	data B;
		retain &my_column_order.;set B;
	run;
	
	proc iml;
		use mycas.coeff_&metric.; read all var {mean_coef} into x; close;
		use B; read all var _all_ into y; close;
		z=y*x;
		create pred1 from z; append from z;		close;
	quit;

	data mycas.pred;
        set mycas.test(keep=id target);
        set pred1;pred=1-logistic(col1);
        mean_coef_vote=(pred>=0.5);
	run;

   %metric_compute(data=mycas.pred, target=target, pred_target=mean_coef_vote); /** Gives mycas.metrics that 
   contain the performance evaluation for this ensemble for mean_coeff***/

	data mycas.results;
		set mycas.metrics;
		ensemble="logreg_mean_coef_&metric._ul_&ul_tp.";
	run;

	data mycas.evaluations;	/* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;

   %metric_compute(data=mycas.&metric._logreg_eval, target=target, pred_target=logreg_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results1;
		set mycas.metrics;
		ensemble="logreg_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Stor evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results1;
	run;
   %metric_compute(data=mycas.&metric._logreg_eval, target=target, pred_target=logreg_soft_vote);/** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for soft_vote***/

	data mycas.results2;
		set mycas.metrics;
		ensemble="logreg_soft_vote_&metric._ul_&ul_tp.";
	run;

    data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results2;
	run;    
%mend logreg_eval;



%macro gboost_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, 
ensemble=gboost_ensemble, metric=f1, ul_tp=&ratio.);
    
	data mycas.&metric._gboost_models;
		set &ensemble;
		where metric="&metric.";
	run;

	proc sql noprint;
		select count(*) into :N separated by ' ' from mycas.&metric._gboost_models;
	quit;

    data mycas.&metric._gboost_eval; /* selct ID and target columns from the test data */
	    set &data (keep=ID target);
    run;

    data mycas.gb_ens;
	    set mycas.&metric._gboost_models;
        id= _n_;
    run;

	%do j=1 %to &N;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set mycas.gb_ens;
			if id=&j.;
			call symput('i', index);
		run;

		/**** Scoring the data***********************/
		proc astore;
			score data=&data out=mycas.out_&j. rstore=mycas.gboost_&metric._%sysevalf(&i.)
					copyvars=(ID);
		run;

       /*** Rename predicted variable and Join with the observed target**/
		data mycas.assess;
			set mycas.out_&j.;
			keep ID p_&target.1 p_&target.0 i_&target;
			rename p_&target.0 = p&target.0_gboost_&j. p_&target.1=p&target.1_gboost_&j. i_&target.=i&target._gboost_&j.;
		run;

        data mycas.&metric._gboost_eval;
			merge mycas.&metric._gboost_eval mycas.assess;
            by ID;
		run;
	%end;

    data mycas.&metric._gboost_eval;
		set  mycas.&metric._gboost_eval;
        p&target.1_gboost_Avg=mean(of p&target.1_gboost_1-p&target.1_gboost_&N.);
        i&target._gboost_Avg=mean(of i&target._gboost_1-i&target._gboost_&N);
        if p&target.1_gboost_Avg>=0.5 then gboost_soft_vote = '1'; else gboost_soft_vote = '0';
        if i&target._gboost_Avg>=0.5 then gboost_hard_vote = '1'; else gboost_hard_vote = '0';
        keep ID &target gboost_hard_vote gboost_soft_vote;
	run;

   %metric_compute(data=mycas.&metric._gboost_eval, target=target, pred_target=gboost_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results1;
		set mycas.metrics;
		ensemble="gboost_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results1;
	run;
   %metric_compute(data=mycas.&metric._gboost_eval, target=target, pred_target=gboost_soft_vote);/** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for soft_vote***/

	data mycas.results2;
		set mycas.metrics;
		ensemble="gboost_soft_vote_&metric._ul_&ul_tp.";
	run;

    data mycas.evaluations; /* Stor evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results2;
	run;    
%mend gboost_eval;

/* Table to store performances */
data mycas.evaluations;
	attrib ensemble length=$50;
	stop;
run;

/*  Macro the uses %svm_eval, %gboost_eval, and %logreg_eval to create the final evaluation table */
%macro performance_eval(metrics=mycas.eval_metric);
	proc sql noprint;
		select count(*) into :N_metrics separated by ' ' from &metrics.;
	quit;

	%do k=1 %to &N_metrics;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set &metrics.;
			if index=&k.;
			call symput('metr', metric);
		run;

		%svm_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, ensemble=svm_ensemble, metric=&metr., ul_tp=&ratio.);
		%gboost_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, ensemble=gboost_ensemble, metric=&metr., ul_tp=&ratio.);
		%logreg_eval(data=mycas.test, target=target, IntIn=V1-V28 Amount, ensemble=logreg_ensemble, metric=&metr., ul_tp=&ratio.);
	%end;
%mend performance_eval;

%performance_eval;

proc export data=mycas.evaluations
    outfile="&outdir.evaluations1.csv"
    dbms=csv replace;
run;

/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;

/*****************************************************************************/
/*  Terminate the specified CAS session (mySession). No reconnect is possible*/
/*****************************************************************************/
cas mySession terminate;

