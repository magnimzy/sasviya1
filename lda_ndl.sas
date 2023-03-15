option castimeout=max;

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/************************************************************************/
cas mySession sessopts=(caslib=casuser timeout=36000 locale="en_US" metrics=true);

 
/*DEFINE CAS ENGINE LIBREF FOR CAS IN-MEMORY TABLES*/  
libname mycas cas caslib=casuser;

/*DEFINE LIBREF to LOCAL TABLES*/
libname locallib '/global/home/tmp_stemp56/sasuser.viya/';

/*SPECIFY FOLDER PATH FOR score code OUTPUT FILES*/
%let outdir = /global/home/tmp_stemp56/sasuser.viya/;

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
proc import datafile='/global/home/tmp_stemp56/sasuser.viya/lda_parameter.csv'
	dbms=CSV
	out=mycas.lda_parameter;
	getnames=YES;
run;


/*lda non default loss computation*/



%macro ndl(data=mycas.lda_parameter, N=1000);

	data mycas.results;
		do i=1 to &N;
			output;
		end;
	run;

    proc sql noprint;
		select count(*) into :M separated by ' ' from &data;
	quit;

	%do j=1 %to &M;

		/*Create a macro variable from a value in a data set*/
		data _null_;
			set &data;
			if scenario=&j.;
			call symput('pois1', poisson);
			call symput('lnorm1', lognorm1);
			call symput('lnorm2', lognorm2);
		run;

		data mycas.poissons;
			do i=1 to &N;
				nb_occ=rand("poisson",&pois1);
				output;
			end;
		run;

		data mycas.losses (keep=i loss&j.);
			do i=1 to &N;
				nb_occ=rand("poisson",&pois1);
                loss&j. = 0;
                do z=1 to nb_occ;
                   log_loss=rand("Normal",&lnorm1, &lnorm2);
                   loss1=exp(log_loss); 
                   loss&j.=loss&j.+loss1;                
                end;                 
				output;
			end;
		run;

		data mycas.results;
			merge mycas.results mycas.losses;
			by i;
		run;
	%end;

	data mycas.results;
		set mycas.results;
		agg_loss = sum(of loss1 - loss&M.);
	run;
	proc means data=mycas.results n mean max min range std  p1 p5 q1 median q3  p90 p95 p99;
		Var loss1 - loss&M. agg_loss;
	run;

%mend ndl;

%ndl;

%put loss&M;

/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;

/*****************************************************************************/
/*  Terminate the specified CAS session (mySession). No reconnect is possible*/
/*****************************************************************************/
cas mySession terminate;

