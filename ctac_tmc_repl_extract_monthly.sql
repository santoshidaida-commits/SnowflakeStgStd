
--/**************************************************************************************************************/
--/* FileName:*/--/*ctac_tmc_repl_vs_extract_monthly.sql															    */
--/* Description: Loading Montly Marriott's Request For Pricing Weekly load from S3 into stage table 			*/
--/* Purpose:  To Fetch latest run id files from S3 SZ and extract records and load it into stage table			*/
--/* This step is executed through SNOWSQL																		*/
--/*																											*/
--/* Change history:																							*/
--/*																											*/
--/* Name                Date         Modification																*/
--/* ===============     ==========   ============																*/
--/* TCS	             06/22/2022     Initial Version															*/
--/*																											*/
--/**************************************************************************************************************/

!set variable_substitution=true;
USE role sysadmin;
/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL
(CD_BOOKING_IATA,	
DT_START,
DT_END,	
CD_PARENT_IATA,	
CD_STATUS)
SELECT CD_BOOKING_IATA,	
DT_START,
MAX(DT_END),	
CD_PARENT_IATA,	
CD_STATUS
FROM &{RES_DB}.&{STG_SCHEMA}.TMC_BIATA_REL_STG
  GROUP BY CD_BOOKING_IATA,	
DT_START,	
CD_PARENT_IATA,	
CD_STATUS;

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_REL as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL
(CD_BOOKING_IATA, 
	TX_TA_NAME, 
	CD_TA_COUNTRY)
SELECT CD_BOOKING_IATA, 
	TX_TA_NAME, 
	CD_TA_COUNTRY
FROM &{RES_DB}.&{STG_SCHEMA}.TMC_IATA_MASTER_STG;

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER;

create or replace view &{RES_DB}.PROXY.RES_DIM_IATA_MASTER as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_IATA_MASTER;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL
(CD_PARENT_IATA,
 TX_TMC_NAME)
SELECT CD_PARENT_IATA,
 rtrim(TX_TMC_NAME)
FROM &{RES_DB}.&{STG_SCHEMA}.TMC_PARENT_MASTER_STG;

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER;

create or replace view &{RES_DB}.PROXY.RES_DIM_PARENT_MASTER as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PARENT_MASTER;



/**Delta refresh**/
INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS_REPL
( 
	CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	CD_TMC_PIATA,
	DT_DATE_ARRIVE,
	CD_MARKET_SEGMENT,
	QY_ROOMNIGHTS,
	CD_CTAC_CURRENCY,
	CU_TM6_REVENUE_USD,
	CU_TM6_PREF_AMT_USD,
	CU_TM6_CNTRY_TAX_USD,
	CU_TM6_PRVN_TAX_USD,
	CU_TM6_CITY_TAX_USD,
	CU_TM6_REVENUE_CTAC,
	CU_TM6_PREF_AMT_CTAC,
	CU_TM6_CNTRY_TAX_CTAC,
	CU_TM6_PRVN_TAX_CTAC,
	CU_TM6_CITY_TAX_CTAC,
	CD_TM6_REASON_CODE,
	CU_TM9_FEE_AMT_USD,
	CU_TM9_CNTRY_TAX_USD,
	CU_TM9_PRVN_TAX_USD,
	CU_TM9_CITY_TAX_USD,
	CU_TM9_FEE_AMT_CTAC,
	CU_TM9_CNTRY_TAX_CTAC,
	CU_TM9_PRVN_TAX_CTAC,
	CU_TM9_CITY_TAX_CTAC,
	ID_PROPERTY,
	CD_CONFO_NUM
)
select 
	CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	CD_TMC_PIATA,
	DT_DATE_ARRIVE,
	CD_MARKET_SEGMENT,
	QY_ROOMNIGHTS,
	CD_CTAC_CURRENCY,
	ROUND(CU_TM6_REVENUE_USD,3),
	ROUND(CU_TM6_PREF_AMT_USD,3),
	ROUND(CU_TM6_CNTRY_TAX_USD,3),
	ROUND(CU_TM6_PRVN_TAX_USD,3),
	ROUND(CU_TM6_CITY_TAX_USD,3),
	ROUND(CU_TM6_REVENUE_CTAC,3),
	ROUND(CU_TM6_PREF_AMT_CTAC,3),
	ROUND(CU_TM6_CNTRY_TAX_CTAC,3),
	ROUND(CU_TM6_PRVN_TAX_CTAC,3),
	ROUND(CU_TM6_CITY_TAX_CTAC,3),
	CD_TM6_REASON_CODE,
	ROUND(CU_TM9_FEE_AMT_USD,3),
	ROUND(CU_TM9_CNTRY_TAX_USD,3),
	ROUND(CU_TM9_PRVN_TAX_USD,3),
	ROUND(CU_TM9_CITY_TAX_USD,3),
	ROUND(CU_TM9_FEE_AMT_CTAC,3),
	ROUND(CU_TM9_CNTRY_TAX_CTAC,3),
	ROUND(CU_TM9_PRVN_TAX_CTAC,3),
	ROUND(CU_TM9_CITY_TAX_CTAC,3),
	ID_PROPERTY,
	CD_CONFO_NUM
FROM &{RES_DB}.&{STG_SCHEMA}.TMC_TMC_STAYS_STG;


create temp table res.main.temp_stays_date as
SELECT D1.year_acctg, D1.period_acctg_id
FROM res.main.mrdw_dim_date D1, res.main.mrdw_dim_date D2
WHERE D2.date_dt =  current_date
AND D1.date_pars_id = (D2.date_pars_id - 28);


update RES.MAIN.RES_DIM_TMC_STAYS_REPL t
set cd_week_acctg =
(select max(cd_week_acctg)
 from RES.MAIN.RES_FACT_COMMISSION_STAYS c
 where t.id_property = c.id_property
 and t.id_seq_number = c.id_seq_number
 and t.id_year_acctg = c.id_year_acctg
 and t.id_period_acctg = c.id_period_acctg
 and t.id_year_acctg = (select year_acctg from res.main.temp_stays_date)
 and t.id_period_acctg = (select period_acctg_id from res.main.temp_stays_date))
where t.id_year_acctg = (select year_acctg from res.main.temp_stays_date)
and t.id_period_acctg = (select period_acctg_id from res.main.temp_stays_date);


create temp table res.main.temp_stays_del_date as
select year_acctg, period_acctg_id
from res.main.mrdw_dim_date
where date_key = (
select (max(date_key) - 1204)
from res.main.mrdw_dim_date
where (period_acctg_id, year_acctg) in
(select d2.period_acctg_id, d2.year_acctg
from res.main.mrdw_dim_date d1, res.main.mrdw_dim_date d2
WHERE d1.date_dt = CURRENT_DATE
AND d2.date_pars_id = (d1.date_pars_id - 28)));


delete from RES.MAIN.RES_DIM_TMC_STAYS_REPL
where id_year_acctg = (select year_acctg from res.main.temp_stays_del_date)
and id_period_acctg <  (select period_acctg_id from res.main.temp_stays_del_date);



USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS;

create or replace view &{RES_DB}.PROXY.RES_DIM_TMC_STAYS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN_REPL;

INSERT INTO RES.MAIN.RES_DIM_PROP_OPTN_REPL 
(
CD_PROPERTY, 
	DT_START, 
	DT_END, 
	CD_STATUS
)
SELECT CD_PROPERTY, 
	DT_START, 
	DT_END, 
	CD_STATUS
FROM &{RES_DB}.&{STG_SCHEMA}.TMC_PROP_OPTN_STG;	

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN;

create or replace view &{RES_DB}.PROXY.RES_DIM_PROP_OPTN as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN;



----------One time script--------------------------------------------------------------------------------------------
/* TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES;


Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('1A','C1','GDS Amadeus',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('1G','C1','GDS Galileo',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('AA','C1','GDS Sabre',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('AARJ','C1','GDS Omaha reject Q',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('TW','C1','GDS Worldspan',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('UA','C1','GDS Apollo',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WWWMC','C3','MARRIOTT.COM',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('OMAIC','C2','INTERNET Internet Customer Care - Omaha',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WB','C2','INTERNET Pegasus',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WWWWS','C2','INTERNET WorldRes',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WWWRL','C2','INTERNET e-Rooming List',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WWWEX','C2','INTERNET Expedia',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('WWWHT','C2','INTERNET Hotel.com',CURRENT_DATE);
Insert into &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_CODE_C_SRC_TYPES (CD_BOOK_OFFICE,CD_BOOK_SOURCE,TX_BOOK_SOURCE,DW_LOAD_TS) values ('HD','C2','INTERNET Travelweb (through Pegasus)',CURRENT_DATE);  */

-----------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE RES.MAIN.RES_DIM_RPT_DATES_PAY_MONTH;


INSERT INTO RES.MAIN.RES_DIM_RPT_DATES_PAY_MONTH(ID_YEAR_CAL, ID_MONTH_CAL, TX_MONTH)
SELECT DISTINCT DT.year_cal, DT.month_cal_id, DT.month_txt
FROM RES.MAIN.mrdw_dim_date DT
WHERE DT.date_key BETWEEN (SELECT MIN(date_key)
                                    FROM RES.MAIN.mrdw_dim_date DT
                                        WHERE (dt.year_cal, dt.month_cal_id) IN
                                        (SELECT DECODE(DT.month_cal_id, 12, DT.year_cal - 1, DT.year_cal - 2) , DECODE(DT.month_cal_id, 12, 1, dt.month_cal_id + 1)
                                        FROM RES.MAIN.mrdw_dim_date DT
                                        WHERE DT.date_key = (SELECT MAX(LTRIM(END_QUERY.date_key))
                                                 FROM RES.MAIN.mrdw_dim_date END_QUERY
                                                 WHERE END_QUERY.year_acctg = (SELECT LTRIM(RDWD_DATE1.year_acctg)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END_QUERY.period_acctg_id = (SELECT  LTRIM (RDWD_DATE1.period_acctg_id)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))																		  
																		  )
																		  )
																		  )
AND
(SELECT MAX(LTRIM(END_QUERY1.date_key))
                                                 FROM RES.MAIN.mrdw_dim_date END_QUERY1
                                                 WHERE END_QUERY1.year_acctg = (SELECT LTRIM(RDWD_DATE1.year_acctg)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												 AND END_QUERY1.period_acctg_id= (SELECT LTRIM (RDWD_DATE1.period_acctg_id)                                                                        
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))						  
																		  );
																		  

create or replace view &{RES_DB}.PROXY.RES_DIM_RPT_DATES_PAY_MONTH as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_RPT_DATES_PAY_MONTH;

TRUNCATE TABLE RES.MAIN.RES_DIM_RPT_DATES_PAY_PERIOD;


INSERT INTO RES.MAIN.RES_DIM_RPT_DATES_PAY_PERIOD(ID_YEAR_ACCTG, ID_PERIOD_ACCTG)																		  
SELECT DISTINCT DT.year_acctg, DT.period_acctg_id
FROM RES.MAIN.mrdw_dim_date DT
WHERE DT.date_key BETWEEN ( SELECT MIN(LTRIM(STRT.date_key))
                                                   FROM RES.MAIN.mrdw_dim_date STRT
                                                   WHERE STRT.period_acctg_id = (SELECT LTRIM(RDWD_DATE2.period_acctg_id)
                                                                                 FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                         WHERE RDWD_DATE2.date_dt = current_date
                                                         AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
                                                   AND (STRT.year_acctg)= (SELECT LTRIM(RDWD_DATE1.year_acctg) -  2
                                                                                  FROM RES.MAIN.mrdw_dim_date RDWD_DATE1
                                                                                  WHERE RDWD_DATE1.date_dt = current_date))
                                AND
                                        (SELECT MAX(LTRIM(END_QUERY.date_key))
                                                 FROM RES.MAIN.mrdw_dim_date END_QUERY
                                                 WHERE END_QUERY.year_acctg = (SELECT LTRIM(RDWD_DATE1.year_acctg)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												 AND END_QUERY.period_acctg_id = (SELECT  LTRIM (RDWD_DATE1.period_acctg_id)                                                                  
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28)) 						  
																		  );	


create or replace view &{RES_DB}.PROXY.RES_DIM_RPT_DATES_PAY_PERIOD as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_RPT_DATES_PAY_PERIOD;


/* TRUNCATE TABLE RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH;


INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,7,'JULY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,8,'AUGUST');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,9,'SEPTEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,10,'OCTOBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,11,'NOVEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2020,12,'DECEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,1,'JANUARY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,2,'FEBRUARY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,3,'MARCH');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,4,'APRIL');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,5,'MAY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,6,'JUNE');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,7,'JULY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,8,'AUGUST');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,9,'SEPTEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,10,'OCTOBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,11,'NOVEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2021,12,'DECEMBER');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,1,'JANUARY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,2,'FEBRUARY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,3,'MARCH');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,4,'APRIL');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,5,'MAY');
INSERT INTo RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL,ID_MONTH_CAL,TX_MONTH) values(2022,6,'JUNE');  */

create or replace view &{RES_DB}.PROXY.RES_DIM_RPT_DATES_STAY_MONTH as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_RPT_DATES_STAY_MONTH;


INSERT INTO RES.MAIN.RES_DIM_RPT_DATES_STAY_MONTH(ID_YEAR_CAL, ID_MONTH_CAL, TX_MONTH)
SELECT DISTINCT DT.year_cal, DT.month_cal_id, DT.month_txt
FROM RES.MAIN.mrdw_dim_date DT
WHERE DT.date_key BETWEEN
(SELECT MIN(date_key)
 FROM RES.MAIN.mrdw_dim_date DT
 WHERE dt.year_cal IN
                (SELECT DT.year_cal - 2
                 FROM RES.MAIN.mrdw_dim_date DT
                 WHERE DT.date_key = (SELECT MAX(END1.date_key)
                                              FROM RES.MAIN.mrdw_dim_date END1
                                              WHERE END1.year_acctg IN 
                                                     (SELECT LTRIM(DECODE(RDWD_DATE1.period_acctg_id, 1, (RDWD_DATE1.year_acctg - 1), RDWD_DATE1.year_acctg))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END1.period_acctg_id IN (SELECT LTRIM (DECODE(RDWD_DATE1.period_acctg_id, 1, 13, (RDWD_DATE1.period_acctg_id - 1)))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))	   
													   )
													   )
													   )
AND dt.month_cal_id IN
                (SELECT DT.month_cal_id
                 FROM RES.MAIN.mrdw_dim_date DT
                 WHERE DT.date_key = (SELECT MAX(END1.date_key)
                                              FROM RES.MAIN.mrdw_dim_date END1
                                              WHERE END1.year_acctg IN 
                                                     (SELECT LTRIM(DECODE(RDWD_DATE1.period_acctg_id, 1, (RDWD_DATE1.year_acctg - 1), RDWD_DATE1.year_acctg))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END1.period_acctg_id IN (SELECT LTRIM (DECODE(RDWD_DATE1.period_acctg_id, 1, 13, (RDWD_DATE1.period_acctg_id - 1)))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))	   
													   )
													   )
AND
(SELECT MAX(LTRIM(END_QUERY.date_key))
FROM RES.MAIN.mrdw_dim_date END_QUERY
WHERE END_QUERY.month_cal_id IN
                (SELECT DECODE(DT.month_cal_id, 1, DT.year_cal - 1, DT.year_cal)
                 FROM RES.MAIN.mrdw_dim_date DT
                 WHERE DT.date_key = (SELECT MAX(END1.date_key)
                                              FROM RES.MAIN.mrdw_dim_date END1
                                              WHERE END1.year_acctg IN
                                                     (SELECT LTRIM(DECODE(RDWD_DATE1.period_acctg_id, 1, (RDWD_DATE1.year_acctg - 1), RDWD_DATE1.year_acctg))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END1.period_acctg_id IN (SELECT LTRIM (DECODE(RDWD_DATE1.period_acctg_id, 1, 13, (RDWD_DATE1.period_acctg_id - 1)))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))	   							 	   
													   )
													   )
AND END_QUERY.month_cal_id IN
                (SELECT DECODE(DT.month_cal_id, 1, 12, dt.month_cal_id - 1)
                 FROM RES.MAIN.mrdw_dim_date DT
                 WHERE DT.date_key = (SELECT MAX(END1.date_key)
                                              FROM RES.MAIN.mrdw_dim_date END1
                                              WHERE END1.year_acctg IN(
                                                     (SELECT LTRIM(DECODE(RDWD_DATE1.period_acctg_id, 1, (RDWD_DATE1.year_acctg - 1), RDWD_DATE1.year_acctg))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END1.period_acctg_id IN (SELECT LTRIM (DECODE(RDWD_DATE1.period_acctg_id, 1, 13, (RDWD_DATE1.period_acctg_id - 1)))
                                                      FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                      WHERE RDWD_DATE2.date_dt = current_date
                                                       AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))	   
													   )									 	   
													   )
													   )													   
													   );													   


TRUNCATE TABLE RES.MAIN.RES_DIM_RPT_DATES_STAY_PERIOD;


INSERT INTO RES.MAIN.RES_DIM_RPT_DATES_STAY_PERIOD(ID_YEAR_ACCTG, ID_PERIOD_ACCTG)
SELECT DISTINCT DT.year_acctg, DT.period_acctg_id
FROM RES.MAIN.mrdw_dim_date DT
WHERE DT.date_key BETWEEN         (SELECT MAX(LTRIM(STRT.date_key))
                                                 FROM RES.MAIN.mrdw_dim_date STRT
                                                 WHERE STRT.year_acctg IN (SELECT LTRIM(RDWD_DATE1.year_acctg - 2)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND STRT.period_acctg_id IN (SELECT LTRIM (RDWD_DATE1.period_acctg_id)
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))						  
																		  )
                                AND
                                        (SELECT MAX(LTRIM(END_QUERY.date_key))
                                                 FROM RES.MAIN.mrdw_dim_date END_QUERY
                                                 WHERE END_QUERY.year_acctg = (SELECT LTRIM(DECODE(RDWD_DATE1.period_acctg_id, 1, (RDWD_DATE1.year_acctg - 1), RDWD_DATE1.year_acctg))
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))
												AND END_QUERY.period_acctg_id = (SELECT 
                                                                          LTRIM (DECODE(RDWD_DATE1.period_acctg_id, 1, 13, (RDWD_DATE1.period_acctg_id - 1)))
                                                                          FROM RES.MAIN.mrdw_dim_date RDWD_DATE1, RES.MAIN.mrdw_dim_date RDWD_DATE2
                                                                          WHERE RDWD_DATE2.date_dt = current_date
                                                                          AND RDWD_DATE1.date_pars_id = (RDWD_DATE2.date_pars_id - 28))						  
																		  );

create or replace view &{RES_DB}.PROXY.RES_DIM_RPT_DATES_STAY_PERIOD as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_RPT_DATES_STAY_PERIOD;

----------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RES.MAIN.RES_TMC_MRDW_CONFO_V (CD_PROPERTY, CD_CONFO_NUM_CUR, DT_DATE_ARRIVE, NM_PFIX_SFIX) AS 
select tmc.cd_property, tmc.cd_confo_num_cur, tmc.dt_date_arrive,
         decode (mc.sort_mkt_ctgy_id ,2,'GROUP',3,'CONTRACT',4,'COMP' , mpc.pfix_sfix_cd || ' ' || mpc.pfix_sfix_nm ) nm_pfix_sfix
from  res.main.RES_FACT_MRDW_CONFO tmc,
      res.main.mrdw_dim_mkt_pfix_sfix_cntl mpc,
      res.main.MRDW_DIM_MARKET_SEGMENT_SAT ms,
      res.main.MRDW_DIM_MARKET_CATEGORY_SAT mc
where tmc.id_mkt_pfix_sfix = mpc.mkt_pfix_sfix_key(+)
and   mpc.market_seg_key = ms.market_seg_key(+)
and   ms.market_ctgy_key = mc.market_ctgy_key(+);

---------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_A AS
SELECT
	/*+ FIRST_ROWS */
	T.CD_PROPERTY AS CD_PROPERTY,
	T.ID_YEAR_ACCTG AS ID_YEAR_ACCTG,
	T.ID_PERIOD_ACCTG AS ID_PERIOD_ACCTG,
	T.CD_TMC_PIATA AS CD_TMC_PIATA,
	T.CD_BOOKING_IATA AS CD_BOOKING_IATA,
	T.CD_TM6_REASON_CODE AS CD_TMC_REASON,
	SUBSTR(T.CD_MARKET_SEGMENT,1,2) AS CD_MARKET_SEGMENT,
	T.CD_CTAC_CURRENCY AS CD_CTAC_CURRENCY,
	SUM(T.QY_ROOMNIGHTS) AS QY_ROOMNIGHTS,
	SUM(T.CU_TM6_REVENUE_CTAC) AS CU_REVENUE_CTAC,
	SUM(T.CU_TM6_REVENUE_USD) AS CU_REVENUE_USD,
	0 AS CU_PHI_COMMISSION_CTAC,
	0 AS CU_PH_COMMISSION_USD,
	0 AS CU_TH_COMM_TAX_CTAC,
	0 AS CU_TH_COMM_TAX_USD,
	SUM(T.CU_TM6_PREF_AMT_CTAC) AS CU_PREF_PYMT_CTAC,
	SUM(T.CU_TM6_PREF_AMT_USD) AS CU_PREF_PYMT_USD,
	SUM(T.CU_TM6_CNTRY_TAX_CTAC + T.CU_TM6_PRVN_TAX_CTAC + T.CU_TM6_CITY_TAX_CTAC) AS CU_PREF_PYMT_TAX_CTAC,
	SUM(T.CU_TM6_CNTRY_TAX_USD + T.CU_TM6_PRVN_TAX_USD + T.CU_TM6_CITY_TAX_USD) AS CU_PREF_PYMT_TAX_USD,
	SUM(T.CU_TM9_FEE_AMT_CTAC) AS CU_TMC_FEE_CTAC,
	SUM(T.CU_TM9_FEE_AMT_USD) AS CU_TMC_FEE_USD,
	SUM(T.CU_TM9_CNTRY_TAX_CTAC + T.CU_TM9_PRVN_TAX_CTAC + T.CU_TM9_CITY_TAX_CTAC) AS CU_TMC_FEE_TAX_CTAC,
	SUM(T.CU_TM9_CNTRY_TAX_USD + T.CU_TM9_PRVN_TAX_USD + T.CU_TM9_CITY_TAX_USD) AS CU_TMC_FEE_TAX_USD,
	case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end as NM_PFIX_SFIX
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS T,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN O,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_TMC_MRDW_CONFO_MV CON_V
	WHERE T.CD_TM6_REASON_CODE != 'JRVS'
	AND T.CD_PROPERTY = O.CD_PROPERTY
	AND O.CD_STATUS = 'P'
	AND T.DT_DATE_ARRIVE BETWEEN O.DT_START AND O.DT_END
	AND T.DT_DATE_ARRIVE = CON_V.DT_DATE_ARRIVE(+)
	AND T.CD_PROPERTY = CON_V.CD_PROPERTY(+)
	AND T.CD_CONFO_NUM = CON_V.CD_CONFO_NUM_CUR(+)
GROUP BY
	T.CD_PROPERTY,
	T.ID_YEAR_ACCTG,
	T.ID_PERIOD_ACCTG,
	T.CD_TMC_PIATA,
	T.CD_BOOKING_IATA,
	T.CD_TM6_REASON_CODE,
	SUBSTR(T.CD_MARKET_SEGMENT,1,2),
	T.CD_CTAC_CURRENCY,
	CU_PHI_COMMISSION_CTAC,
	CU_PH_COMMISSION_USD,
	CU_TH_COMM_TAX_CTAC,
	CU_TH_COMM_TAX_USD,
	case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_1 AS
SELECT
	/*+ FIRST_ROWS */
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2) AS CD_MARKET_SEGMENT,
	C.CD_PHI_CTAC_CURRENCY,
	SUM(C.QY_ROOMNIGHTS) AS QY_ROOMNIGHTS,
	SUM(C.CU_PHI_REVENUE_CTAC) CU_PHI_REVENUE_CTAC,
	SUM(C.CU_PH_REVENUE_USD) CU_PH_REVENUE_USD,
	SUM(C.CU_PHI_COMMISSION_CTAC) CU_PHI_COMMISSION_CTAC,
	SUM(C.CU_PH_COMMISSION_USD) CU_PH_COMMISSION_USD,
	SUM(C.CU_TH_COMM_TAX_CTAC) CU_TH_COMM_TAX_CTAC,
	SUM(C.CU_TH_COMM_TAX_USD)CU_TH_COMM_TAX_USD,
	0 AS CU_PREF_PYMT_CTAC,
	0 AS CU_PREF_PYMT_USD,
	0 AS CU_PREF_PYMT_TAX_CTAC,
	0 AS CU_PREF_PYMT_TAX_USD,
	0 AS TMC_FEE_PYMT_CTAC,
	0 AS TMC_FEE_PYMT_USD,
	0 AS TMC_FEE_PYMT_TAX_CTAC,
	0 AS TMC_FEE_PYMT_TAX_USD,
	case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end as NM_PFIX_SFIX
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS C,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN O,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL B,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_TMC_MRDW_CONFO_MV CON_V
WHERE
	NOT EXISTS (
	SELECT
		'x'
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS T
	WHERE
		T.ID_PROPERTY = C.ID_PROPERTY
		AND T.ID_SEQ_NUMBER = C.ID_SEQ_NUMBER)
	AND C.CD_REASON_CODE != 'JRVS'
	AND C.CD_PROPERTY = O.CD_PROPERTY
	AND O.CD_STATUS = 'P'
	AND C.DT_DATE_ARRIVE BETWEEN O.DT_START AND O.DT_END
	AND C.CD_BOOKING_IATA = B.CD_BOOKING_IATA
	AND C.DT_DATE_ARRIVE BETWEEN B.DT_START AND B.DT_END
	AND C.DT_DATE_ARRIVE = CON_V.DT_DATE_ARRIVE(+)
	AND C.CD_PROPERTY = CON_V.CD_PROPERTY(+)
	AND C.CD_CONFO_NUM = CON_V.CD_CONFO_NUM_CUR(+)
GROUP BY
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,
	1,
	2),
	C.CD_PHI_CTAC_CURRENCY,
	CU_PREF_PYMT_CTAC,
	CU_PREF_PYMT_USD,
	CU_PREF_PYMT_TAX_CTAC,
	CU_PREF_PYMT_TAX_USD,
	TMC_FEE_PYMT_CTAC,
	TMC_FEE_PYMT_USD,
	TMC_FEE_PYMT_TAX_CTAC,
	TMC_FEE_PYMT_TAX_USD,
	case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_2 AS
SELECT
	/*+ FIRST_ROWS */
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2)CD_MARKET_SEGMENT,
	C.CD_PHI_CTAC_CURRENCY,
	SUM(C.QY_ROOMNIGHTS)QY_ROOMNIGHTS,
	SUM(C.CU_PHI_REVENUE_CTAC)CU_PHI_REVENUE_CTAC,
	SUM(C.CU_PH_REVENUE_USD)CU_PH_REVENUE_USD,
	SUM(C.CU_PHI_COMMISSION_CTAC)CU_PHI_COMMISSION_CTAC,
	SUM(C.CU_PH_COMMISSION_USD)CU_PH_COMMISSION_USD,
	SUM(C.CU_TH_COMM_TAX_CTAC)CU_TH_COMM_TAX_CTAC,
	SUM(C.CU_TH_COMM_TAX_USD)CU_TH_COMM_TAX_USD,
	0 AS CU_PREF_PYMT_CTAC,
	0 AS CU_PREF_PYMT_USD,
	0 AS CU_PREF_PYMT_TAX_CTAC,
	0 AS CU_PREF_PYMT_TAX_USD,
	0 AS TMC_FEE_PYMT_CTAC,
	0 AS TMC_FEE_PYMT_USD,
	0 AS TMC_FEE_PYMT_TAX_CTAC,
	0 AS TMC_FEE_PYMT_TAX_USD,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end as NM_PFIX_SFIX
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS C,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL B,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_TMC_MRDW_CONFO_MV CON_V
WHERE
	NOT EXISTS (
	SELECT
		'x'
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS T
	WHERE
		T.ID_PROPERTY = C.ID_PROPERTY
		AND T.ID_SEQ_NUMBER = C.ID_SEQ_NUMBER)
	AND C.CD_REASON_CODE != 'JRVS'
	AND TO_NUMBER(TO_CHAR(C.DT_DATE_ARRIVE,
	'YYYY')) < 2004
	AND C.CD_BOOKING_IATA = B.CD_BOOKING_IATA
	AND C.DT_DATE_ARRIVE BETWEEN B.DT_START AND B.DT_END
	AND C.DT_DATE_ARRIVE = CON_V.DT_DATE_ARRIVE(+)
	AND C.CD_PROPERTY = CON_V.CD_PROPERTY(+)
	AND C.CD_CONFO_NUM = CON_V.CD_CONFO_NUM_CUR(+)
GROUP BY
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2),
	C.CD_PHI_CTAC_CURRENCY,
	CU_PREF_PYMT_CTAC,
	CU_PREF_PYMT_USD,
	CU_PREF_PYMT_TAX_CTAC,
	CU_PREF_PYMT_TAX_USD,
	TMC_FEE_PYMT_CTAC,
	TMC_FEE_PYMT_USD,
	TMC_FEE_PYMT_TAX_CTAC,
	TMC_FEE_PYMT_TAX_USD,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_3 AS
SELECT
	/*+ FIRST_ROWS */
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2)CD_MARKET_SEGMENT,
	C.CD_PHI_CTAC_CURRENCY,
	0 AS QY_ROOMNIGHTS,
	0 AS CU_PHI_REVENUE_CTAC,
	0 AS CU_PH_REVENUE_USD,
	SUM(C.CU_PHI_COMMISSION_CTAC)CU_PHI_COMMISSION_CTAC,
	SUM(C.CU_PH_COMMISSION_USD)CU_PH_COMMISSION_USD,
	SUM(C.CU_TH_COMM_TAX_CTAC)CU_TH_COMM_TAX_CTAC,
	SUM(C.CU_TH_COMM_TAX_USD)CU_TH_COMM_TAX_USD,
	0 AS CU_PREF_PYMT_CTAC,
	0 AS CU_PREF_PYMT_USD,
	0 AS CU_PREF_PYMT_TAX_CTAC,
	0 AS CU_PREF_PYMT_TAX_USD,
	0 AS TMC_FEE_PYMT_CTAC,
	0 AS TMC_FEE_PYMT_USD,
	0 AS TMC_FEE_PYMT_TAX_CTAC,
	0 AS TMC_FEE_PYMT_TAX_USD,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end as NM_PFIX_SFIX
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS C,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_OPTN O,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL B,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_TMC_MRDW_CONFO_MV CON_V
WHERE
	EXISTS (
	SELECT
		'x'
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS T
	WHERE
		T.ID_PROPERTY = C.ID_PROPERTY
		AND T.ID_SEQ_NUMBER = C.ID_SEQ_NUMBER)
	AND C.CD_REASON_CODE != 'JRVS'
	AND C.CD_PROPERTY = O.CD_PROPERTY
	AND O.CD_STATUS = 'P'
	AND C.DT_DATE_ARRIVE BETWEEN O.DT_START AND O.DT_END
	AND C.CD_BOOKING_IATA = B.CD_BOOKING_IATA
	AND C.DT_DATE_ARRIVE BETWEEN B.DT_START AND B.DT_END
	AND C.DT_DATE_ARRIVE = CON_V.DT_DATE_ARRIVE(+)
	AND C.CD_PROPERTY = CON_V.CD_PROPERTY(+)
	AND C.CD_CONFO_NUM = CON_V.CD_CONFO_NUM_CUR(+)
GROUP BY
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2),
	C.CD_PHI_CTAC_CURRENCY,
	CU_PREF_PYMT_CTAC,
	CU_PREF_PYMT_USD,
	CU_PREF_PYMT_TAX_CTAC,
	CU_PREF_PYMT_TAX_USD,
	TMC_FEE_PYMT_CTAC,
	TMC_FEE_PYMT_USD,
	TMC_FEE_PYMT_TAX_CTAC,
	TMC_FEE_PYMT_TAX_USD,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_4 AS
SELECT
	/*+ FIRST_ROWS */
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2)CD_MARKET_SEGMENT,
	C.CD_PHI_CTAC_CURRENCY,
	0 AS QY_ROOMNIGHTS,
	0 AS CU_PHI_REVENUE_CTAC,
	0 AS CU_PH_REVENUE_USD,
	SUM(C.CU_PHI_COMMISSION_CTAC)CU_PHI_COMMISSION_CTAC,
	SUM(C.CU_PH_COMMISSION_USD)CU_PH_COMMISSION_USD,
	SUM(C.CU_TH_COMM_TAX_CTAC)CU_TH_COMM_TAX_CTAC,
	SUM(C.CU_TH_COMM_TAX_USD)CU_TH_COMM_TAX_USD,
	0 AS CU_PREF_PYMT_CTAC,
	0 AS CU_PREF_PYMT_USD,
	0 AS CU_PREF_PYMT_TAX_CTAC,
	0 AS CU_PREF_PYMT_TAX_USD,
	0 AS TMC_FEE_PYMT_CTAC,
	0 AS TMC_FEE_PYMT_USD,
	0 AS TMC_FEE_PYMT_TAX_CTAC,
	0 AS TMC_FEE_PYMT_TAX_USD,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end as NM_PFIX_SFIX
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS C,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL B,
	&{RES_DB}.&{MAIN_SCHEMA}.RES_TMC_MRDW_CONFO_MV CON_V
WHERE
	EXISTS (
	SELECT
		'x'
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_STAYS T
	WHERE
		T.ID_PROPERTY = C.ID_PROPERTY
		AND T.ID_SEQ_NUMBER = C.ID_SEQ_NUMBER)
	AND C.CD_REASON_CODE != 'JRVS'
	AND TO_NUMBER(TO_CHAR(C.DT_DATE_ARRIVE,
	'YYYY')) < 2004
	AND C.CD_BOOKING_IATA = B.CD_BOOKING_IATA
	AND C.DT_DATE_ARRIVE BETWEEN B.DT_START AND B.DT_END
	AND C.DT_DATE_ARRIVE = CON_V.DT_DATE_ARRIVE(+)
	AND C.CD_PROPERTY = CON_V.CD_PROPERTY(+)
	AND C.CD_CONFO_NUM = CON_V.CD_CONFO_NUM_CUR(+)
GROUP BY
	C.CD_PROPERTY,
	C.ID_YEAR_ACCTG,
	C.ID_PERIOD_ACCTG,
	B.CD_PARENT_IATA,
	C.CD_BOOKING_IATA,
	C.CD_REASON_CODE,
	SUBSTR(C.CD_MARKET_SEGMENT,	1,	2),
	C.CD_PHI_CTAC_CURRENCY,
	CU_PREF_PYMT_CTAC,
	CU_PREF_PYMT_USD,
	CU_PREF_PYMT_TAX_CTAC,
	CU_PREF_PYMT_TAX_USD,
	TMC_FEE_PYMT_CTAC,
	TMC_FEE_PYMT_USD,
	TMC_FEE_PYMT_TAX_CTAC,
	TMC_FEE_PYMT_TAX_USD ,
		case when con_v.NM_PFIX_SFIX = 'O Group Other' then 'GROUP' else  con_v.NM_PFIX_SFIX end;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_B AS
select * from ( select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_1 union all 
select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_2 union all 
select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_3 union all 
select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_4  )x
;

    
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENT_INTERMEDIATE_TEMP as (
select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_A
union 
select * from &{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENTS_TEMP_B
);


TRUNCATE TABLE RES.MAIN.RES_AGG_PAYMENTS_SUMMARY_REPL; 

INSERT
	INTO
	&{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY_REPL 
	(CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_QTR_ACCTG,
	ID_YEAR_CAL,
	ID_QTR_CAL,
	ID_MONTH_CAL,
	CD_TMC_PIATA,
	CD_TMC_REASON,
	CD_CTAC_CURRENCY,
	QY_ROOMNIGHTS,
	CU_REVENUE_CTAC,
	CU_REVENUE_USD,
	CU_COMMISSION_CTAC,
	CU_COMMISSION_USD,
	CU_COMM_TAX_CTAC,
	CU_COMM_TAX_USD,
	CU_PREF_PYMT_CTAC,
	CU_PREF_PYMT_USD,
	CU_PREF_PYMT_TAX_CTAC,
	CU_PREF_PYMT_TAX_USD,
	CU_TMC_FEE_CTAC,
	CU_TMC_FEE_USD,
	CU_TMC_FEE_TAX_CTAC,
	CU_TMC_FEE_TAX_USD,
	NM_PFIX_SFIX,
	CD_MARKET_SEGMENT)
SELECT
	CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	DT.QTR_ACCTG_ID,
	DT.YEAR_CAL ,
	DT.QTR_CAL_ID,
	DT.MONTH_CAL_ID ,
	CD_TMC_PIATA,
	CD_TMC_REASON,
	CD_CTAC_CURRENCY,
	sum(QY_ROOMNIGHTS)QY_ROOMNIGHTS,
	sum(CU_REVENUE_CTAC)CU_REVENUE_CTAC,
	sum(CU_REVENUE_USD)CU_REVENUE_USD,
	sum(CU_PHI_COMMISSION_CTAC)CU_PHI_COMMISSION_CTAC,
	sum(CU_PH_COMMISSION_USD)CU_PH_COMMISSION_USD,
	sum(CU_TH_COMM_TAX_CTAC)CU_TH_COMM_TAX_CTAC,
	sum(CU_TH_COMM_TAX_USD)CU_TH_COMM_TAX_USD,
	sum(CU_PREF_PYMT_CTAC)CU_PREF_PYMT_CTAC,
	sum(CU_PREF_PYMT_USD)CU_PREF_PYMT_USD,
	sum(CU_PREF_PYMT_TAX_CTAC)CU_PREF_PYMT_TAX_CTAC,
	sum(CU_PREF_PYMT_TAX_USD)CU_PREF_PYMT_TAX_USD,
	sum(CU_TMC_FEE_CTAC)CU_TMC_FEE_CTAC,
	sum(CU_TMC_FEE_USD)CU_TMC_FEE_USD,
	sum(CU_TMC_FEE_TAX_CTAC)CU_TMC_FEE_TAX_CTAC,
	sum(CU_TMC_FEE_TAX_USD)CU_TMC_FEE_TAX_USD,
	NM_PFIX_SFIX,
	(CASE
		WHEN ltrim(rtrim(CD_MARKET_SEGMENT))= '' THEN 'GR'
		WHEN try_to_numeric(CD_MARKET_SEGMENT) is not null
		AND CAST(CD_MARKET_SEGMENT AS integer) < 10 THEN 'XX'
		WHEN try_to_numeric(CD_MARKET_SEGMENT) is not null THEN CD_MARKET_SEGMENT
		WHEN ((ASCII(SUBSTR(CD_MARKET_SEGMENT, 1, 1)) >= 65
		AND ASCII(SUBSTR(CD_MARKET_SEGMENT, 1, 1)) <= 90)
		AND (ASCII(SUBSTR(CD_MARKET_SEGMENT, 2, 1)) >= 65
		AND ASCII(SUBSTR(CD_MARKET_SEGMENT, 2, 1)) <= 90)) THEN 'GR'
		ELSE 'XX'
	END) CD_MARKET_SEGMENT
FROM
	&{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENT_INTERMEDIATE_TEMP TP
LEFT OUTER JOIN (
	SELECT
		D1.QTR_ACCTG_ID ,
		D1.YEAR_CAL ,
		D1.QTR_CAL_ID,
		D1.MONTH_CAL_ID ,
		D1.YEAR_ACCTG ,
		D1.PERIOD_ACCTG_ID
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE D1
	WHERE
		(D1.DATE_KEY,
		D1.YEAR_ACCTG,
		D1.PERIOD_ACCTG_ID) IN (
		SELECT
			MAX(D2.DATE_KEY),
			D2.YEAR_ACCTG,
			D2.PERIOD_ACCTG_ID
		FROM
			&{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE D2
		GROUP BY
			D2.YEAR_ACCTG,
			D2.PERIOD_ACCTG_ID) )DT ON
	TP.ID_YEAR_ACCTG = DT.YEAR_ACCTG
	AND TP.ID_PERIOD_ACCTG = DT.PERIOD_ACCTG_ID
GROUP BY
	CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	DT.QTR_ACCTG_ID,
	DT.YEAR_CAL ,
	DT.QTR_CAL_ID,
	DT.MONTH_CAL_ID ,
	CD_TMC_PIATA,
	CD_TMC_REASON,
	CD_CTAC_CURRENCY,
	NM_PFIX_SFIX ,
	CD_MARKET_SEGMENT;
   
   
TRUNCATE TABLE RES.MAIN.RES_AGG_PAYMENTS_BIATA_REPL;    
   
INSERT
	INTO
	&{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA_REPL 
	(CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_QTR_ACCTG,
	ID_YEAR_CAL,
	ID_QTR_CAL,
	ID_MONTH_CAL,
	CD_TMC_PIATA,
	CD_BOOKING_IATA,
	CD_CTAC_CURRENCY,
	QY_ROOMNIGHTS,
	CU_REVENUE_CTAC,
	CU_REVENUE_USD,
	CD_BRAND,
	CD_MGMT_TYPE,
	FL_COMPARABLE,
	ID_DIVISION,
	ID_REGION)
SELECT
	AGG.CD_PROPERTY,
	AGG.ID_YEAR_ACCTG,
	AGG.ID_PERIOD_ACCTG,
	AGG.QTR_ACCTG_ID,
	AGG.YEAR_CAL ,
	AGG.QTR_CAL_ID,
	AGG.MONTH_CAL_ID ,
	AGG.CD_TMC_PIATA,
	AGG.CD_BOOKING_IATA,
	AGG.CD_CTAC_CURRENCY,
	AGG.QY_ROOMNIGHTS,
	AGG.CU_REVENUE_CTAC,
	AGG.CU_REVENUE_USD,
	PT.BRAND_CD,
	PT.MGMT_TYPE_CD,
	PT.COMPARABLE_IND,
	PT.DIVISION_ID,
	PT.REGION_ID
FROM
	(
	SELECT
		CD_PROPERTY,
		ID_YEAR_ACCTG,
		ID_PERIOD_ACCTG,
		DT.QTR_ACCTG_ID,
		DT.YEAR_CAL ,
		DT.QTR_CAL_ID,
		DT.MONTH_CAL_ID ,
		CD_TMC_PIATA,
		CD_BOOKING_IATA,
		CD_CTAC_CURRENCY,
		sum(QY_ROOMNIGHTS)QY_ROOMNIGHTS,
		sum(CU_REVENUE_CTAC)CU_REVENUE_CTAC,
		sum(CU_REVENUE_USD)CU_REVENUE_USD
	FROM
		&{RES_DB}.&{MAIN_SCHEMA}.TMC_PAYMENT_INTERMEDIATE_TEMP TP
	LEFT OUTER JOIN (
		SELECT
			D1.QTR_ACCTG_ID ,
			D1.YEAR_CAL ,
			D1.QTR_CAL_ID,
			D1.MONTH_CAL_ID ,
			D1.YEAR_ACCTG ,
			D1.PERIOD_ACCTG_ID
		FROM
			&{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE D1
		WHERE
			(D1.DATE_KEY,
			D1.YEAR_ACCTG,
			D1.PERIOD_ACCTG_ID) IN (
			SELECT
				MAX(D2.DATE_KEY),
				D2.YEAR_ACCTG,
				D2.PERIOD_ACCTG_ID
			FROM
				&{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE D2
			GROUP BY
				D2.YEAR_ACCTG,
				D2.PERIOD_ACCTG_ID) )DT ON
		TP.ID_YEAR_ACCTG = DT.YEAR_ACCTG
		AND TP.ID_PERIOD_ACCTG = DT.PERIOD_ACCTG_ID
	GROUP BY
		CD_PROPERTY,
		ID_YEAR_ACCTG,
		ID_PERIOD_ACCTG,
		DT.QTR_ACCTG_ID,
		DT.YEAR_CAL ,
		DT.QTR_CAL_ID,
		DT.MONTH_CAL_ID ,
		CD_TMC_PIATA,
		CD_BOOKING_IATA,
		CD_CTAC_CURRENCY )AGG
LEFT OUTER JOIN &{PTY_DB}.&{MAIN_SCHEMA}.PTY_DIM_PROPERTY PT ON
	AGG.CD_PROPERTY = PT.PROPERTY_CD
WHERE
	PT.BRAND_CD != 'NR' ;
	
	
USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY;

create or replace view &{RES_DB}.PROXY.RES_AGG_PAYMENTS_SUMMARY as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_SUMMARY;	


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA;

create or replace view &{RES_DB}.PROXY.RES_AGG_PAYMENTS_BIATA as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BIATA;	

---------------------------------------------
TRUNCATE TABLE RES.MAIN.RES_AGG_PAYMENTS_BRSVP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP_REPL (CD_BRAND,CD_MGMT_TYPE,FL_COMPARABLE,ID_YEAR_ACCTG,ID_QTR_ACCTG,ID_PERIOD_ACCTG,ID_YEAR_CAL,ID_QTR_CAL,ID_MONTH_CAL,CD_TMC_PIATA,CD_TMC_REASON,CD_MARKET_SEGMENT,CD_CTAC_CURRENCY,ID_DIVISION,ID_REGION,NM_PFIX_SFIX,QY_ROOMNIGHTS,CU_REVENUE_CTAC,CU_REVENUE_USD,CU_COMMISSION_CTAC,CU_COMMISSION_USD,CU_COMM_TAX_CTAC,CU_COMM_TAX_USD,CU_PREF_PYMT_CTAC,CU_PREF_PYMT_USD,CU_PREF_PYMT_TAX_CTAC,CU_PREF_PYMT_TAX_USD,CU_TMC_FEE_CTAC,CU_TMC_FEE_USD,CU_TMC_FEE_TAX_CTAC,CU_TMC_FEE_TAX_USD)
SELECT 
B.BRAND_CD, 
B.MGMT_TYPE_CD, 
B.COMPARABLE_IND,
A.ID_YEAR_ACCTG, 
A.ID_QTR_ACCTG, 
A.ID_PERIOD_ACCTG,
A.ID_YEAR_CAL, 
A.ID_QTR_CAL, 
A.ID_MONTH_CAL, 
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY, 
B.DIVISION_ID,
B.REGION_ID,
A.NM_PFIX_SFIX,
sum(A.QY_ROOMNIGHTS)QY_ROOMNIGHTS, 
sum(A.CU_REVENUE_CTAC)CU_REVENUE_CTAC, 
sum(A.CU_REVENUE_USD)CU_REVENUE_USD, 
sum(A.CU_COMMISSION_CTAC)CU_COMMISSION_CTAC, 
sum(A.CU_COMMISSION_USD)CU_COMMISSION_USD, 
sum(A.CU_COMM_TAX_CTAC)CU_COMM_TAX_CTAC, 
sum(A.CU_COMM_TAX_USD)CU_COMM_TAX_USD, 
sum(A.CU_PREF_PYMT_CTAC)CU_PREF_PYMT_CTAC, 
sum(A.CU_PREF_PYMT_USD)CU_PREF_PYMT_USD, 
sum(A.CU_PREF_PYMT_TAX_CTAC)CU_PREF_PYMT_TAX_CTAC, 
sum(A.CU_PREF_PYMT_TAX_USD)CU_PREF_PYMT_TAX_USD, 
sum(A.CU_TMC_FEE_CTAC)CU_TMC_FEE_CTAC, 
sum(A.CU_TMC_FEE_USD)CU_TMC_FEE_USD, 
sum(A.CU_TMC_FEE_TAX_CTAC)CU_TMC_FEE_TAX_CTAC, 
sum(A.CU_TMC_FEE_TAX_USD)CU_TMC_FEE_TAX_USD
FROM RES.MAIN.RES_AGG_PAYMENTS_SUMMARY A, 
             PTY.MAIN.PTY_DIM_PROPERTY B
WHERE A.CD_PROPERTY = B.PROPERTY_CD
AND B.BRAND_CD != 'NR'
group by 
B.BRAND_CD, 
B.MGMT_TYPE_CD, 
B.COMPARABLE_IND, 
A.ID_YEAR_ACCTG, 
A.ID_QTR_ACCTG, 
A.ID_PERIOD_ACCTG,
A.ID_YEAR_CAL, 
A.ID_QTR_CAL, 
A.ID_MONTH_CAL, 
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY,
B.DIVISION_ID,
B.REGION_ID,
A.NM_PFIX_SFIX
;


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP_REPL;



ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP;



create or replace view &{RES_DB}.PROXY.RES_AGG_PAYMENTS_BRSVP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_PAYMENTS_BRSVP;

--------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE RES.MAIN.RES_AGG_STAY_MONTH_BRSVP_REPL;

INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP_REPL(CD_BRAND,CD_MGMT_TYPE,FL_COMPARABLE,ID_YEAR_CAL,ID_QTR_CAL,ID_MONTH_CAL,CD_TMC_PIATA,CD_TMC_REASON,CD_MARKET_SEGMENT,CD_CTAC_CURRENCY,ID_DIVISION,ID_REGION,NM_PFIX_SFIX,QY_ROOMNIGHTS,CU_REVENUE_CTAC,CU_REVENUE_USD,CU_COMMISSION_CTAC,CU_COMMISSION_USD,CU_COMM_TAX_CTAC,CU_COMM_TAX_USD,CU_PREF_PYMT_CTAC,CU_PREF_PYMT_USD,CU_PREF_PYMT_TAX_CTAC,CU_PREF_PYMT_TAX_USD,CU_TMC_FEE_CTAC,CU_TMC_FEE_USD,CU_TMC_FEE_TAX_CTAC,CU_TMC_FEE_TAX_USD)
SELECT 
B.BRAND_CD, 
B.MGMT_TYPE_CD, 
B.COMPARABLE_IND,
A.ID_YEAR_CAL, 
A.ID_QTR_CAL, 
A.ID_MONTH_CAL,
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY, 
B.DIVISION_ID,
B.REGION_ID,
A.NM_PFIX_SFIX,
SUM(A.QY_ROOMNIGHTS)QY_ROOMNIGHTS, 
SUM(A.CU_REVENUE_CTAC)CU_REVENUE_CTAC, 
SUM(A.CU_REVENUE_USD)CU_REVENUE_USD, 
SUM(A.CU_COMMISSION_CTAC)CU_COMMISSION_CTAC, 
SUM(A.CU_COMMISSION_USD)CU_COMMISSION_USD, 
SUM(A.CU_COMM_TAX_CTAC)CU_COMM_TAX_CTAC, 
SUM(A.CU_COMM_TAX_USD)CU_COMM_TAX_USD, 
SUM(A.CU_PREF_PYMT_CTAC)CU_PREF_PYMT_CTAC, 
SUM(A.CU_PREF_PYMT_USD)CU_PREF_PYMT_USD, 
SUM(A.CU_PREF_PYMT_TAX_CTAC)CU_PREF_PYMT_TAX_CTAC, 
SUM(A.CU_PREF_PYMT_TAX_USD)CU_PREF_PYMT_TAX_USD, 
SUM(A.CU_TMC_FEE_CTAC)CU_TMC_FEE_CTAC, 
SUM(A.CU_TMC_FEE_USD)CU_TMC_FEE_USD, 
SUM(A.CU_TMC_FEE_TAX_CTAC)CU_TMC_FEE_TAX_CTAC, 
SUM(A.CU_TMC_FEE_TAX_USD)CU_TMC_FEE_TAX_USD 
FROM RES.MAIN.RES_AGG_STAY_MONTH_SUMMARY A, 
 PTY.MAIN.PTY_DIM_PROPERTY B
WHERE A.CD_PROPERTY = B.PROPERTY_CD
AND B.BRAND_CD != 'NR' 
GROUP BY 
B.BRAND_CD, 
B.MGMT_TYPE_CD, 
B.COMPARABLE_IND, 
A.ID_YEAR_CAL, 
A.ID_QTR_CAL, 
A.ID_MONTH_CAL,
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY,
B.DIVISION_ID,
B.REGION_ID,
A.NM_PFIX_SFIX ;


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP_REPL;



ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP;



create or replace view &{RES_DB}.PROXY.RES_AGG_STAY_MONTH_BRSVP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_MONTH_BRSVP;


----------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE RES.MAIN.RES_AGG_STAY_PERIOD_BRSVP_REPL;

INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP_REPL(CD_BRAND,CD_MGMT_TYPE,FL_COMPARABLE,ID_YEAR_ACCTG,ID_QTR_ACCTG,ID_PERIOD_ACCTG,CD_TMC_PIATA,CD_TMC_REASON,CD_MARKET_SEGMENT,CD_CTAC_CURRENCY,ID_DIVISION,ID_REGION,NM_PFIX_SFIX,QY_ROOMNIGHTS,CU_REVENUE_CTAC,CU_REVENUE_USD,CU_COMMISSION_CTAC,CU_COMMISSION_USD,CU_COMM_TAX_CTAC,CU_COMM_TAX_USD,CU_PREF_PYMT_CTAC,CU_PREF_PYMT_USD,CU_PREF_PYMT_TAX_CTAC,CU_PREF_PYMT_TAX_USD,CU_TMC_FEE_CTAC,CU_TMC_FEE_USD,CU_TMC_FEE_TAX_CTAC,CU_TMC_FEE_TAX_USD)
SELECT 
B.CD_BRAND,  
B.CD_MGMT_TYPE, 
B.FL_COMPARABLE,  
A.ID_YEAR_ACCTG, 
A.ID_QTR_ACCTG, 
A.ID_PERIOD_ACCTG,
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY, 
B.ID_DIVISION,
B.ID_REGION,
A.NM_PFIX_SFIX,
sum(A.QY_ROOMNIGHTS)QY_ROOMNIGHTS, 
sum(A.CU_REVENUE_CTAC)CU_REVENUE_CTAC, 
sum(A.CU_REVENUE_USD)CU_REVENUE_USD, 
sum(A.CU_COMMISSION_CTAC)CU_COMMISSION_CTAC, 
sum(A.CU_COMMISSION_USD)CU_COMMISSION_USD, 
sum(A.CU_COMM_TAX_CTAC)CU_COMM_TAX_CTAC, 
sum(A.CU_COMM_TAX_USD)CU_COMM_TAX_USD, 
sum(A.CU_PREF_PYMT_CTAC)CU_PREF_PYMT_CTAC, 
sum(A.CU_PREF_PYMT_USD)CU_PREF_PYMT_USD, 
sum(A.CU_PREF_PYMT_TAX_CTAC)CU_PREF_PYMT_TAX_CTAC, 
sum(A.CU_PREF_PYMT_TAX_USD)CU_PREF_PYMT_TAX_USD, 
sum(A.CU_TMC_FEE_CTAC)CU_TMC_FEE_CTAC, 
sum(A.CU_TMC_FEE_USD)CU_TMC_FEE_USD, 
sum(A.CU_TMC_FEE_TAX_CTAC)CU_TMC_FEE_TAX_CTAC, 
sum(A.CU_TMC_FEE_TAX_USD)CU_TMC_FEE_TAX_USD
FROM RES.MAIN.RES_AGG_STAY_PERIOD_SUMMARY A, 
  PTY.MAIN.PTY_DIM_PROPERTY B
WHERE A.CD_PROPERTY = B.PROPERTY_CD
AND B.BRAND_CD != 'NR' 
GROUP BY
B.CD_BRAND,  
B.CD_MGMT_TYPE, 
B.FL_COMPARABLE, 
A.ID_YEAR_ACCTG, 
A.ID_QTR_ACCTG, 
A.ID_PERIOD_ACCTG,
A.CD_TMC_PIATA, 
A.CD_TMC_REASON, 
A.CD_MARKET_SEGMENT, 
A.CD_CTAC_CURRENCY, 
B.ID_DIVISION,
B.ID_REGION,
A.NM_PFIX_SFIX
;


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP_REPL;



ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP;



create or replace view &{RES_DB}.PROXY.RES_AGG_STAY_PERIOD_BRSVP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_AGG_STAY_PERIOD_BRSVP;

--------------------------------------------------------------------------------------------------------------------------
