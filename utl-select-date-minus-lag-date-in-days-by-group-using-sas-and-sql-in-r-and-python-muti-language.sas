%let pgm=utl-select-date-minus-lag-date-in-days-by-group-using-sas-and-sql-in-r-and-python-muti-language;

%stop_submission;

Select date minus lag date in days by group using sas and sql in r and python muti language

OP says it is slow because he/she has a big dataset.
With out knowing the size of the data it is hard measure what slow means.
I suspect the dataset is not big data (ie single table 1tb)

        SOLUTIONS  (slighly different input. Data is sorted by type date)

           0 elegant hash solution no rqire a sort
             Keintz, Mark
             mkeintz@outlook.com
           1 sas datastep
           2 r sql
           3 python sql
           4 r tidyverse (not in base -> group_by %>% lag arrange)

github
https://tinyurl.com/5des9c3v
https://github.com/rogerjdeangelis/utl-select-date-minus-lag-date-in-days-by-group-using-sas-and-sql-in-r-and-python-muti-language

stackoverflow
https://tinyurl.com/2yu98j28
https://stackoverflow.com/questions/79176217/find-number-of-days-in-between-dates-of-observations-belonging-to-the-same-group

related repo
https://tinyurl.com/5azsfab2
https://github.com/rogerjdeangelis/utl-lags-in-proc-sql-monotonic-datastep-is-preferred

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                        |                                         |                                     */
/*                   INPUT                |                PROCESS                  |           OUTPUT                    */
/*           DATE is DAYS SINCW 1/1/60    |                =======                  |           ======                    */
/*           =========================    |                                         |                                     */
/*                                        |                                         |                                     */
/*  ID    TYPE       DATEC        DATE    | SAS DATASTEP                            | ID TYPE    DATEC     DATE DIFDATE   */
/*                                        | ============                            |                                     */
/*   1    type1    01/01/2014    19724    |                                         |  1 type1 01/01/2014 19724      .    */
/*   2    type2    01/06/2015    20094    | data want;                              |  3 type1 01/06/2015 20094    370    */
/*   3    type1    01/06/2015    20094    |                                         |  4 type1 07/04/2017 21004    910    */
/*   4    type1    07/04/2017    21004    |    %dosubl('                            |  2 type2 01/06/2015 20094      .    */
/*   5    type2    05/10/2018    21314    |     proc sort data=sd1.have             |  5 type2 05/10/2018 21314   1220    */
/*   6    type3    05/01/2026    24227    |          out=havSrt;                    |  6 type3 05/01/2026 24227      .    */
/*                                        |        by type date;                    |                                     */
/*                                        |     run;quit;                           |                                     */
/*                                        |     ');                                 |                                     */
/*                                        |                                         |                                     */
/*                                        |  set havSrt;                            |                                     */
/*                                        |   by type;                              |                                     */
/*                                        |                                         |                                     */
/*                                        |   difdate=dif(date);                    |                                     */
/*                                        |   if first.type then difdate=.;         |                                     */
/*                                        |                                         |                                     */
/*                                        | run;quit;                               |                                     */
/*                                        |                                         |                                     */
/*                                        | --------------------------------------  |                                     */
/*                                        |                                         |                                     */
/*                                        | R AND PYTHON SQL                        |                                     */
/*                                        | ===============                         |                                     */
/*                                        |                                         |                                     */
/*                                        | select                                  |                                     */
/*                                        |      date                               |                                     */
/*                                        |     ,type                               |                                     */
/*                                        |     ,datec                              |                                     */
/*                                        |     ,date - lag(date)                   |                                     */
/*                                        | over                                    |                                     */
/*                                        |     (partition                          |                                     */
/*                                        |         by type                         |                                     */
/*                                        |     order                               |                                     */
/*                                        |         by date) as difference          |                                     */
/*                                        | from                                    |                                     */
/*                                        |     have                                |                                     */
/*                                        | order by                                |                                     */
/*                                        |     type, date                          |                                     */
/*                                        |                                         |                                     */
/*                                        |-----------------------------------------|                                     */
/*                                        |                                         |                                     */
/*                                        | R tidyverse language                    |                                     */
/*                                        |                                         |                                     */
/*                                        | want<-have %>%                          |                                     */
/*                                        |    group_by(TYPE ) %>%                  |                                     */
/*                                        |    arrange(TYPE, DATE) %>%              |                                     */
/*                                        |    mutate(diff=DATE-lag(DATE))          |                                     */
/*                                        |                                         |                                     */
/***************************************************************************************|**********************************/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input ID  type$  datec $10.;
date=input(datec,mmddyy10.);
cards4;
1 type1 01/01/2014
2 type2 01/06/2015
3 type1 01/06/2015
4 type1 07/04/2017
5 type2 05/10/2018
6 type3 05/01/2026
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  ID    TYPE       DATEC        DATE                                                                                    */
/*                                                                                                                        */
/*   1    type1    01/01/2014    19724                                                                                    */
/*   2    type2    01/06/2015    20094                                                                                    */
/*   3    type1    01/06/2015    20094                                                                                    */
/*   4    type1    07/04/2017    21004                                                                                    */
/*   5    type2    05/10/2018    21314                                                                                    */
/*   6    type3    05/01/2026    24227                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*___                    _               _
 / _ \   ___  __ _ ___  | |__   __ _ ___| |__
| | | | / __|/ _` / __| | `_ \ / _` / __| `_ \
| |_| | \__ \ (_| \__ \ | | | | (_| \__ \ | | |
 \___/  |___/\__,_|___/ |_| |_|\__,_|___/_| |_|

*/

data want (drop=_:);
  set have;
  call missing(_prior_date);
  if _n_=1 then do;
   declare hash h ();
     h.definekey('type');
     h.definedata('type','_prior_date');
     h.definedone();
  end;

  if h.find()=0 then difdate=date-_prior_date;
  h.replace(key:type,data:type,data:date);  /*DATE value gets inserted into _PRIOR_DATE in the hash*/
run;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*ID TYPE    DATEC     DATE DIFDATE                                                                                       */
/*                                                                                                                        */
/* 1 type1 01/01/2014 19724      .                                                                                        */
/* 3 type1 01/06/2015 20094    370                                                                                        */
/* 4 type1 07/04/2017 21004    910                                                                                        */
/* 2 type2 01/06/2015 20094      .                                                                                        */
/* 5 type2 05/10/2018 21314   1220                                                                                        */
/* 6 type3 05/01/2026 24227      .                                                                                        */
/*                                                                                                                        */
/*************************************************************************************************************************

/*                       _       _            _
/ |  ___  __ _ ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
| | / __|/ _` / __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
| | \__ \ (_| \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_| |___/\__,_|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                                                     |_|
*/

data want;

   %dosubl('
    proc sort data=sd1.have out=havSrt;
       by type date;
    run;quit;
    ');

 set havSrt;
  by type;

  difdate=dif(date);
  if first.type then difdate=.;

  keep id datec date difdate;

run;quit;

proc print data=want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  WANT total obs=6                                                                                                      */
/*                                                                                                                        */
/*   ID      DATEC        DATE     DIF                                                                                    */
/*                                                                                                                        */
/*    1    01/01/2014    19724       .                                                                                    */
/*    3    01/06/2015    20094     370                                                                                    */
/*    4    07/04/2017    21004     910                                                                                    */
/*    2    01/06/2015    20094       .                                                                                    */
/*    5    05/10/2018    21314    1220                                                                                    */
/*    6    05/01/2026    24227       .                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
have<-read_sas("d:/sd1/have.sas7bdat")
str(have)
want<-sqldf('
  select
       date
      ,type
      ,datec
      ,date - lag(date)
  over
      (partition
          by type
      order
          by date) as difference
  from
      have
  order by
      type, date
  ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                        |                                                                               */
/*  > want                                | SAS                                                                           */
/*                                        |                                                                               */
/*     DATE  TYPE      DATEC difference   | ROWNAMES     DATE    TYPE       DATEC       DIFFERENCE                        */
/*                                        |                                                                               */
/*  1 19724 type1 01/01/2014         NA   |     1       19724    type1    01/01/2014          .                           */
/*  2 20094 type1 01/06/2015        370   |     2       20094    type1    01/06/2015        370                           */
/*  3 21004 type1 07/04/2017        910   |     3       21004    type1    07/04/2017        910                           */
/*  4 20094 type2 01/06/2015         NA   |     4       20094    type2    01/06/2015          .                           */
/*  5 21314 type2 05/10/2018       1220   |     5       21314    type2    05/10/2018       1220                           */
/*  6 24227 type3 05/01/2026         NA   |     6       24227    type3    05/01/2026          .                           */
/*                                         |                                                                               */
/**************************************************************************************************************************/

/*____               _   _                             _
|___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;
 delete pywant;
run;quit;

%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_python.py').read())
have,meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat')
have
want=pdsql('''                     \
    select                         \
         date                      \
        ,type                      \
        ,datec                     \
        ,date - lag(date)          \
    over                           \
        (partition                 \
            by type                \
        order                      \
            by date) as difference \
    from                           \
        have                       \
    order by                       \
        type, date                 \
    ''')
print(want);
fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3);
;;;;
%utl_pyendx(resolve=Y);

proc print data=sd1.pywant;
run;quit;


/**************************************************************************************************************************/
/*                                              |                                                                         */
/* PYTHON                                       |                                                                         */
/*                                              |                                                                         */
/*        DATE   TYPE       DATEC  difference   |    DATE    TYPE       DATEC       DIFFERENCE                            */
/*                                              |                                                                         */
/*  0  19724.0  type1  01/01/2014         NaN   |   19724    type1    01/01/2014          .                               */
/*  1  20094.0  type1  01/06/2015       370.0   |   20094    type1    01/06/2015        370                               */
/*  2  21004.0  type1  07/04/2017       910.0   |   21004    type1    07/04/2017        910                               */
/*  3  20094.0  type2  01/06/2015         NaN   |   20094    type2    01/06/2015          .                               */
/*  4  21314.0  type2  05/10/2018      1220.0   |   21314    type2    05/10/2018       1220                               */
/*  5  24227.0  type3  05/01/2026         NaN   |   24227    type3    05/01/2026          .                               */
/*                                              |                                                                         */
/**************************************************************************************************************************/

/*  _            _   _     _
| || |    _ __  | |_(_) __| |_   ___   _____ _ __ ___ ___  ___
| || |_  | `__| | __| |/ _` | | | \ \ / / _ \ `__/ __/ __|/ _ \
|__   _| | |    | |_| | (_| | |_| |\ V /  __/ |  \__ \__ \  __/
   |_|   |_|     \__|_|\__,_|\__, | \_/ \___|_|  |___/___/\___|
                             |___/
*/
%utl_rbeginx;
parmcards4;
library(haven)
library(tidyverse)
source("c:/oto/fn_tosas9x.R")
have<-read_sas("d:/sd1/have.sas7bdat")
want<-have %>%
   group_by(TYPE ) %>%
   arrange(TYPE, DATE) %>%
   mutate(diff=DATE-lag(DATE))
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                        |                                                                               */
/*  > want                                | SAS                                                                           */
/*                                        |                                                                               */
/*     DATE  TYPE      DATEC difference   | ROWNAMES     DATE    TYPE       DATEC       DIFFERENCE                        */
/*                                        |                                                                               */
/*  1 19724 type1 01/01/2014         NA   |     1       19724    type1    01/01/2014          .                           */
/*  2 20094 type1 01/06/2015        370   |     2       20094    type1    01/06/2015        370                           */
/*  3 21004 type1 07/04/2017        910   |     3       21004    type1    07/04/2017        910                           */
/*  4 20094 type2 01/06/2015         NA   |     4       20094    type2    01/06/2015          .                           */
/*  5 21314 type2 05/10/2018       1220   |     5       21314    type2    05/10/2018       1220                           */
/*  6 24227 type3 05/01/2026         NA   |     6       24227    type3    05/01/2026          .                           */
/*                                        |                                                                               */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
