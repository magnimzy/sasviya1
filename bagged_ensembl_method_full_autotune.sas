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
	attrib metric length=$20 model length=$20;
	stop;
run;

data mycas.f1_svm_eval(keep=ID target) mycas.ase_svm_eval(keep=ID target) 
		mycas.auc_svm_eval(keep=ID target);
	set mycas.test;
	output mycas.f1_svm_eval;
	output mycas.ase_svm_eval;
	output mycas.auc_svm_eval;
run;

%macro svm(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=f1, iter=1);

		/* SVMACHINE model */

		proc svmachine data=&data.;
			partition rolevar=_partind_(train='1' validate='0');
			autotune searchmethod=grid objective=&metric. popsize=10 maxiter=5 maxevals=50 ;
			input &IntIn./level=interval;
			target &target.;
			savestate rstore=mycas.svm_&metric._&iter.;
			ods output BestConfiguration=bestconfig;
		run;

		data bestconfig;
			set bestconfig; model="svm_model_&iter"; metric="&metric.";
		run;

		data svm_ensemble;
			set svm_ensemble bestconfig;
		run;
		
		proc astore;
			score data=mycas.test out=mycas.svm_pred_&metric._&iter.
		        rstore=mycas.svm_&metric._&iter. copyvars=(_ALL_);
		run;

        data mycas.out;
			set mycas.svm_pred_&metric._&iter.;
            keep ID p_&target.1 p_&target.0 i_&target. &target.;
			rename p_&target.0 = p&target.0_svm_&j. p_&target.1=p&target.1_svm_&j. i_&target.=i&target._svm_&j.;
		run;

		data mycas.&metric._svm_eval;
			merge mycas.&metric._svm_eval mycas.out;
			by ID;
		run;
    
%mend svm;

/***********************************************************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/********************LOGISTIC REGRESSION ********************************/
/***********************************************************************/


data logreg_ensemble;
	attrib metric length=$20 model length=$20;
	stop;
run;

data mycas.f1_logreg_eval(keep=ID target) mycas.ase_logreg_eval(keep=ID target) 
		mycas.auc_logreg_eval(keep=ID target);
	set mycas.test;
	output mycas.f1_logreg_eval;
	output mycas.ase_logreg_eval;
	output mycas.auc_logreg_eval;
run;

%macro logreg(data=part, target=target, IntIn=V1-V28 Amount, metric=f1, iter=1);

		/* LOGREG AUTOTUNE model */

		proc cas noqueue;
			autotune.tuneLogistic / 
		        scoreoptions={table={name="part", caslib="casuser", where="_partind_=0"}}, 
		        trainOptions={table={name="part", caslib="casuser", where="_partind_=1"}, 
				model={depVars={{name="&target."}}, 
				effects={{vars=${&Intin.}}}}, savestate={name="logreg_&metric._&iter."} } 
				useParameters="CUSTOM",
		        tunerOptions={seed=12345, objective="&metric.", userdefinedpartition=TRUE} 
		        tuningParameters={{name='LASSORHO'}};
				ods output ParameterEstimates=Coeff_&metric._&iter.(keep=effect estimate rename= (estimate=estimate_&iter.));
			ods output bestconfiguration=bestconfig;
			run;
		quit;

		data bestconfig;
			set bestconfig; model="logreg_model_&iter"; metric="&metric.";
		run;

		data logreg_ensemble;
			set logreg_ensemble bestconfig;
		run;
		
		proc astore;
			score data=mycas.test out=mycas.logreg_pred_&metric._&iter.
		        rstore=mycas.logreg_&metric._&iter. copyvars=(_ALL_);
		run;

        data mycas.out;
			set mycas.logreg_pred_&metric._&iter.;
            keep ID p_&target.1 p_&target.0 i_&target. &target.;
			rename p_&target.0 = p&target.0_logreg_&j. p_&target.1=p&target.1_logreg_&j. i_&target.=i&target._logreg_&j.;
		run;

		data mycas.&metric._logreg_eval;
			merge mycas.&metric._logreg_eval mycas.out;
			by ID;
		run;

        /* Storing coefficients for later mean coefficient vote */
		proc sort data=Coeff_&metric._1;
			by effect;
		run;
		
		proc sort data=Coeff_&metric._&iter.;
			by effect;
		run;
		
		data Coeff_&metric._1; /* Store logreg coefficients estimates*/
			merge Coeff_&metric._1 Coeff_&metric._&iter.;
			by effect;
		run;
    
%mend logreg;

/**************************************************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/******************* BOOSTED TREE *****************/
/**************************************************/

data gboost_ensemble;
	attrib metric length=$20 model length=$20;
	stop;
run;

data mycas.f1_gboost_eval(keep=ID target) mycas.ase_gboost_eval(keep=ID target) 
		mycas.auc_gboost_eval(keep=ID target);
	set mycas.test;
	output mycas.f1_gboost_eval;
	output mycas.ase_gboost_eval;
	output mycas.auc_gboost_eval;
run;

%macro gboost(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=f1, iter=1);

		/* GRADBOOST model */
        
		proc gradboost data=&data.;
			partition rolevar=_partind_(train='1' validate='0');
			autotune tuningparameters=(ntrees maxdepth learningrate samplingrate(exclude) 
				vars_to_try(exclude) lasso(exclude) ridge(exclude) numbin(exclude) ) 
				objective=&metric.  maxevals=50 maxiters=5 popsize=10 targetevent='1';
			input &IntIn./level=interval;
			target &target.;
			savestate rstore=mycas.gboost_&metric._&iter.;
			ods output BestConfiguration=bestconfig;
		run;

		data bestconfig;
			set bestconfig; model="gboost_model_&iter"; metric="&metric.";
		run;

		data gboost_ensemble;
			set gboost_ensemble bestconfig;
		run;
		
		proc astore;
			score data=mycas.test out=mycas.gboost_pred_&metric._&iter.
		        rstore=mycas.gboost_&metric._&iter. copyvars=(_ALL_);
		run;

        data mycas.out;
			set mycas.gboost_pred_&metric._&iter.;
            keep ID p_&target.1 p_&target.0 i_&target. &target.;
			rename p_&target.0 = p&target.0_gboost_&j. p_&target.1=p&target.1_gboost_&j. i_&target.=i&target._gboost_&j.;
		run;

		data mycas.&metric._gboost_eval;
			merge mycas.&metric._gboost_eval mycas.out;
			by ID;
		run;
    
%mend gboost;

/* Metrics table */
data mycas.eval_metric;
   	retain index 0;
	Do metric='ase', 'f1', 'auc';
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

		%do j=1 %to 100;
        /*%do j=1 %to &final_num_models.;*/
            %let _timer_start = %sysfunc(datetime());
            %bootstrap(data=mycas.training, target=target, ul_tp=&ul_tp., i=%sysevalf(&j.+ &k.*&final_num_models.));/* create sample mycas.part that will
			be used to find ensemble elements */
		    %svm(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
            %logreg(data=part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
            %gboost(data=mycas.part, target=target, IntIn=V1-V28 Amount, metric=&metr., iter=&j.);
			data _null_;
				dur=datetime() - &_timer_start;
				put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
			run;
		%end;
	%end;
%mend ensemble_creation;

%ODSOff();
%ensemble_creation;

/*******************/
/* Model evaluation */
/*******************/

%let cutoff=0.50;

/* SVM eval */

%macro svm_eval(metric=misc, ul_tp=&ratio., target=target);
  
	data mycas.&metric._svm_eval1;
		set mycas.&metric._svm_eval;
        i&target._svm_Avg=mean(of i&target._svm_1-i&target._svm_&final_num_models.);
        svm_hard_vote = (i&target._svm_Avg>=&cutoff.);
        keep ID &target i&target._svm_Avg svm_hard_vote;
	run;

   %metric_compute(data=mycas.&metric._svm_eval1, target=target, pred_target=svm_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results1;
		set mycas.metrics;
		ensemble="svm_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Stor evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results1;
	run;
   
%mend svm_eval;

/* logreg eval */

%macro logreg_eval(metric=misc, ul_tp=&ratio., target=target);
    /* hard vote and soft vote */
	data mycas.&metric._logreg_eval1;
		set mycas.&metric._logreg_eval;
        p&target.1_logreg_Avg=mean(of p&target.1_logreg_1-p&target.1_logreg_&final_num_models.);
        i&target._logreg_Avg=mean(of i&target._logreg_1-i&target._logreg_&final_num_models.);
        logreg_soft_vote = (p&target.1_logreg_Avg>=&cutoff.);
        logreg_hard_vote = (i&target._logreg_Avg>=&cutoff.);
        keep ID &target i&target._logreg_Avg logreg_hard_vote p&target.1_logreg_Avg logreg_soft_vote;
	run;

   %metric_compute(data=mycas.&metric._logreg_eval1, target=target, pred_target=logreg_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results;
		set mycas.metrics;
		ensemble="logreg_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;

   %metric_compute(data=mycas.&metric._logreg_eval1, target=target, pred_target=logreg_soft_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results;
		set mycas.metrics;
		ensemble="logreg_soft_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;
   
    /* mean coefficient vote */

	data mycas.Coeff_&metric.(keep=effect mean_coef);
		set Coeff_&metric._1;
		mean_coef=mean(of estimate_1-estimate_&final_num_models.);
	run;
	
	Data B(keep=Intercept Amount V1-V28); set mycas.test; intercept=1; /* Get the covariates */
	run;
	
	proc sql; /* Order the columns to have a coherent matrix multiplication. Each parameter much be multiply by the variable it represents */
		select effect into :my_column_order separated by " " from 
			mycas.coeff_&metric. order by effect;
	quit;
	
	data B;
		retain &my_column_order.; set B;
	run;
	
	proc iml; /* Matrix multiplication using proc iml */
		use mycas.coeff_&metric.; read all var {mean_coef} into x; close;
		use B; read all var _all_ into y; close;
		z=y*x;
		create pred1 from z;
		append from z;
		close;
	quit;
	
	data mycas.pred;
		set mycas.test(keep=id target);
		set pred1;
		pred=1-logistic(col1);
        mean_coef_vote=(pred>=&cutoff.);
	run;
	
	%metric_compute(data=mycas.pred, target=target, pred_target=mean_coef_vote);/** Gives mycas.metrics that
	contain the performance evaluation for this ensemble for mean_coeff***/

	data mycas.results;
		set mycas.metrics;
		ensemble="logreg_mean_coef_&metric._ul_&ul_tp.";
	run;
	
	data mycas.evaluations;
		/* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;
   
%mend logreg_eval;

/* gboost eval */

%macro gboost_eval(metric=misc, ul_tp=&ratio.,target=target);
  
	data mycas.&metric._gboost_eval1;
		set mycas.&metric._gboost_eval;
        p&target.1_gboost_Avg=mean(of p&target.1_gboost_1-p&target.1_gboost_&final_num_models.);
        i&target._gboost_Avg=mean(of i&target._gboost_1-i&target._gboost_&final_num_models.);
        gboost_soft_vote =(p&target.1_gboost_Avg>=&cutoff.);  
        gboost_hard_vote =(i&target._gboost_Avg>=&cutoff.);
        keep ID &target i&target._gboost_Avg gboost_hard_vote  p&target.1_gboost_Avg gboost_soft_vote;
	run;

   %metric_compute(data=mycas.&metric._gboost_eval1, target=target, pred_target=gboost_hard_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results;
		set mycas.metrics;
		ensemble="gboost_hard_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;

   %metric_compute(data=mycas.&metric._gboost_eval1, target=target, pred_target=gboost_soft_vote); /** Gives 
   mycas.metrics that contain the performance evaluation for this ensemble for hard_vote***/

	data mycas.results;
		set mycas.metrics;
		ensemble="gboost_soft_vote_&metric._ul_&ul_tp.";
	run;
    
	data mycas.evaluations; /* Store evaluation performance in table mycas.evaluations*/
		set mycas.evaluations mycas.results;
	run;
   
%mend gboost_eval;

data mycas.evaluations;
	attrib ensemble length=$50;
	stop;
run;

/* Macro the uses %svm_eval, %gboost_eval, and %logreg_eval to create the final evaluation table */

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

		%svm_eval(metric=&metr., ul_tp=&ratio.,target=target);
        %logreg_eval(metric=&metr., ul_tp=&ratio.,target=target);
		%gboost_eval(metric=&metr., ul_tp=&ratio.,target=target);
    %end;
%mend performance_eval;

%performance_eval;

proc export data=mycas.evaluations
    outfile="&outdir.evaluations2.csv"
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



