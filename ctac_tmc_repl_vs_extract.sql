--/**************************************************************************************************************/
--/* FileName:*/--/*ctac_tmc_repl_vs_extract.sql															    */
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
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL;

INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL
(
   CD_GROUP_IATA, 
	CD_PROPERTY, 
	ID_MINI_HOTEL_SEQ, 
	CD_MINI_HOTEL, 
	NM_EVENT, 
	DT_EVENT_START, 
	DT_EVENT_END, 
	CD_OPPORTY_NUM, 
	CD_QUOTE_NUM, 
	CD_GRP_UNPAID_RSN, 
	CD_DATA_SOURCE, 
	DT_GROUP_RELEASE, 
	CD_DELETE_IND, 
	CD_FAST_PAY_IND, 
	CD_MH_STATUS, 
	CD_STAYS_STATUS, 
	CD_CREATE_USER, 
	DT_CREATE, 
	TM_CREATE, 
	CD_UPDATE_USER, 
	TS_UPDATE, 
	DT_ORIG_RCVD)
    SELECT CD_GROUP_IATA, 
	CD_PROPERTY, 
	ID_MINI_HOTEL_SEQ, 
	CD_MINI_HOTEL, 
	NM_EVENT, 
	DT_EVENT_START, 
	DT_EVENT_END, 
	CD_OPPORTY_NUM, 
	CD_QUOTE_NUM, 
	CD_GRP_UNPAID_RSN, 
	CD_DATA_SOURCE, 
	DT_GROUP_RELEASE, 
	CD_DELETE_IND, 
	CD_FAST_PAY_IND, 
	CD_MH_STATUS, 
	CD_STAYS_STATUS, 
	CD_CREATE_USER, 
	DT_CREATE, 
	TM_CREATE, 
	CD_UPDATE_USER, 
	TS_UPDATE, 
	DT_ORIG_RCVD
    FROM &{RES_DB}.&{STG_SCHEMA}.TMC_GI_MINI_HOTEL_MASTER_STG;


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER;

create or replace view &{RES_DB}.PROXY.RES_DIM_GI_MINI_HOTEL_MASTER as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_MINI_HOTEL_MASTER;		


TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL;

INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL
(CD_BOOKING_IATA, 
	TX_TA_NAME, 
	CD_TA_COUNTRY, 
	CD_STATUS_IND, 
	DT_STATUS_DATE, 
	CD_BIATA_LOC_TYPE, 
	DT_BIATA_CREATE)
SELECT CD_BOOKING_IATA, 
	TX_TA_NAME, 
	CD_TA_COUNTRY, 
	CD_STATUS_IND, 
	DT_STATUS_DATE, 
	CD_BIATA_LOC_TYPE, 
	DT_BIATA_CREATE
    FROM &{RES_DB}.&{STG_SCHEMA}.TMC_VS_IATA_MASTER_STG;


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL_CLONE;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER;

create or replace view &{RES_DB}.PROXY.RES_DIM_VS_IATA_MASTER as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_IATA_MASTER;		


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
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL
(CD_BOOKING_IATA,	
DT_START,
DT_END,	
CD_PARENT_IATA,	
CD_STATUS,	
CD_BIATA_STATUS,
DT_INACTIVE)
SELECT
BR.CD_BOOKING_IATA, 
BR.DT_START,
BR.DT_END, 
BR.CD_PARENT_IATA, 
BR.CD_STATUS, 
decode(IM.CD_STATUS_IND, 'X', 'X', ' '),
decode(IM.cd_status_ind, 'X', IM.DT_STATUS_DATE, to_date('12/31/9999', 'MM/DD/YYYY')) as dt_inactive
FROM
&{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_REL_REPL  BR, 
&{RES_DB}.&{STG_SCHEMA}.TMC_VS_IATA_MASTER_STG IM
WHERE
BR.cd_booking_iata = IM.cd_booking_iata (+);


USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL;

create or replace view &{RES_DB}.PROXY.RES_DIM_VS_BIATA_REL as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL;

INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL
(CD_PROPERTY, 
	CD_BRAND, 
	CD_PROP_OWNER_GRP)
SELECT CD_PROPERTY, 
	CD_BRAND, 
	CD_PROP_OWNER_GRP
    FROM &{RES_DB}.&{STG_SCHEMA}.TMC_PROP_PROFILE_STG; 



USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE;

create or replace view &{RES_DB}.PROXY.RES_DIM_PROP_PROFILE as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_PROP_PROFILE;

--truncate table RES.MAIN.RES_DIM_TMC_COMP_DATES;
--
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('1P',to_date('18-JUN-22','DD-MON-YY'),to_date('15-JUL-22','DD-MON-YY'),to_date('19-JUN-21','DD-MON-YY'),to_date('16-JUL-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('3P',to_date('23-APR-22','DD-MON-YY'),to_date('15-JUL-22','DD-MON-YY'),to_date('24-APR-21','DD-MON-YY'),to_date('16-JUL-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('YTP',to_date('01-JAN-22','DD-MON-YY'),to_date('15-JUL-22','DD-MON-YY'),to_date('02-JAN-21','DD-MON-YY'),to_date('16-JUL-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('FYP',to_date('02-JAN-21','DD-MON-YY'),to_date('31-DEC-21','DD-MON-YY'),to_date('04-JAN-20','DD-MON-YY'),to_date('01-JAN-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('1M',to_date('01-JUN-22','DD-MON-YY'),to_date('30-JUN-22','DD-MON-YY'),to_date('01-JUN-21','DD-MON-YY'),to_date('30-JUN-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('3M',to_date('01-APR-22','DD-MON-YY'),to_date('30-JUN-22','DD-MON-YY'),to_date('01-APR-21','DD-MON-YY'),to_date('30-JUN-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('YTM',to_date('01-JAN-22','DD-MON-YY'),to_date('30-JUN-22','DD-MON-YY'),to_date('01-JAN-21','DD-MON-YY'),to_date('30-JUN-21','DD-MON-YY'));
--Insert into RES.MAIN.RES_DIM_TMC_COMP_DATES (TX_DATE_CODE,DT_START_TY,DT_END_TY,DT_START_LY,DT_END_LY) values ('FYM',to_date('01-JAN-21','DD-MON-YY'),to_date('31-DEC-21','DD-MON-YY'),to_date('01-JAN-20','DD-MON-YY'),to_date('31-DEC-20','DD-MON-YY'));
--COMMIT;


/**RES_DIM_TMC_COMP_DATES**/
update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt) 
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28))),
 dt_end_ty = (SELECT max(DT.date_dt) 
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28)))
where tx_date_code = '1P';

update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt) 
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg , DT.period_acctg_id) in
(SELECT distinct d1.year_acctg - 1, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28))),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg , DT.period_acctg_id) in
(SELECT distinct d1.year_acctg - 1, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28)))
where tx_date_code = '1P';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE (d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 84))
 OR(d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28)))),
 dt_end_ty = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE (d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 84))
 OR(d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28))))
where tx_date_code = '3P';

update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg - 1, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE (d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 84))
 OR(d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28)))),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_acctg, DT.period_acctg_id) in
(SELECT distinct d1.year_acctg - 1, d1.period_acctg_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
 WHERE (d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 84))
 OR(d2.date_dt = CURRENT_DATE()
 AND d1.date_pars_id = (d2.date_pars_id - 28))))
where tx_date_code = '3P';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT  min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE dt.year_acctg = (select d3.year_acctg
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))
AND dt.period_acctg_id <= (select d1.period_acctg_id
                           from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
                           where d2.date_dt = CURRENT_DATE()
                           AND d1.date_pars_id = (d2.date_pars_id - 28))),
dt_end_ty = (SELECT   max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE dt.year_acctg = (select d3.year_acctg
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))
AND dt.period_acctg_id <= (select d1.period_acctg_id
                           from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
                           where d2.date_dt = CURRENT_DATE()
                           AND d1.date_pars_id = (d2.date_pars_id - 28)))                        
where tx_date_code = 'YTP';



update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT  min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE dt.year_acctg = (select d3.year_acctg - 1
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))
AND dt.period_acctg_id <= (select d1.period_acctg_id
                           from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
                           where d2.date_dt = CURRENT_DATE()
                           AND d1.date_pars_id = (d2.date_pars_id - 28))),
dt_end_ly = (SELECT  max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE dt.year_acctg = (select d3.year_acctg - 1
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))
AND dt.period_acctg_id <= (select d1.period_acctg_id
                           from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d2
                           where d2.date_dt = CURRENT_DATE()
                           AND d1.date_pars_id = (d2.date_pars_id - 28)))                           
where tx_date_code = 'YTP';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE DT.year_acctg = (select d3.year_acctg - 1
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))),
dt_end_ty = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE DT.year_acctg = (select d3.year_acctg - 1
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28)))                          
where tx_date_code = 'FYP';

update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE DT.year_acctg = (select d3.year_acctg - 2
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28))),
dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT
WHERE DT.year_acctg = (select d3.year_acctg - 2
                          from &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d3, &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d4
                          where d3.date_dt = CURRENT_DATE()
                          AND d4.date_pars_id = (d3.date_pars_id - 28)))
where tx_date_code = 'FYP';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.Month_cal_Id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), -1))),
dt_end_ty =  (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.Month_cal_Id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), -1)))    
where tx_date_code = '1M';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal - 1,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), -1))),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal - 1,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), -1)))
where tx_date_code = '1M';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 3)
 OR d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 1))),
 dt_end_ty = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 3)
 OR d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 1)))
where tx_date_code = '3M';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 15)
 OR d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 13))),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 15)
 OR d1.date_dt = ADD_MONTHS(CURRENT_DATE(), - 13)))
 where tx_date_code = '3M';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT distinct d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYY')
 AND d1.month_cal_id <= to_char(ADD_MONTHS(CURRENT_DATE(), - 1), 'MM'))),
 dt_end_ty = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT distinct d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYY')
 AND d1.month_cal_id <= to_char(ADD_MONTHS(CURRENT_DATE(), - 1), 'MM')))
where tx_date_code = 'YTM';

update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT distinct d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYY') - 1
 AND d1.month_cal_id <= to_char(ADD_MONTHS(CURRENT_DATE(), - 1), 'MM'))),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal, DT.month_cal_id) in
(SELECT distinct d1.year_cal,  d1.month_cal_id
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYY') - 1
 AND d1.month_cal_id <= to_char(ADD_MONTHS(CURRENT_DATE(), - 1), 'MM')))
 where tx_date_code = 'YTM';

update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ty =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal) in
(SELECT distinct d1.year_cal
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(CURRENT_DATE(), 'YYYY') - 1)),
 dt_end_ty = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal) in
(SELECT distinct d1.year_cal
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(CURRENT_DATE(), 'YYYY') - 1))
 where tx_date_code = 'FYM';


update &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES
set dt_start_ly =
(SELECT min(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal) in
(SELECT distinct d1.year_cal
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(CURRENT_DATE(), 'YYYY') - 2)),
 dt_end_ly = (SELECT max(DT.date_dt)
FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE DT WHERE (DT.year_cal) in
(SELECT distinct d1.year_cal
 FROM &{RES_DB}.&{MAIN_SCHEMA}.MRDW_DIM_DATE d1
 WHERE d1.year_cal = to_char(CURRENT_DATE(), 'YYYY') - 2))
where tx_date_code = 'FYM';

COMMIT;

create or replace view &{RES_DB}.PROXY.RES_DIM_TMC_COMP_DATES as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES;



/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '1M'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '1M'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_1M_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '1P'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '1P'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_1P_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '3M'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '3M'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_3M_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP;



/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '3P'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = '3P'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_3P_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP;



/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'FYM'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'FYM'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_FYM_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP;



/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'FYP'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'FYP'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL;

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP;


create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_FYP_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'YTM'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'YTM'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL;

--DQ CHECK

ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_YTM_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL
(CD_BOOKING_IATA,		
CD_PARENT_IATA,	
CD_COMP_FLAG)
(select cd_booking_iata, cd_parent_iata, decode(count(*), 2, 'Y', 'N') as cd_comp_flag  from (
select distinct br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'YTP'
and br.dt_start <= cd1.dt_start_ty
and cd1.dt_start_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ty
and cd1.dt_end_ty <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end 
UNION ALL
select br.cd_booking_iata, br.cd_parent_iata, br.dt_start, br.dt_end
from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br, &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_COMP_DATES cd1
where cd1.tx_date_code = 'YTP'
and br.dt_start <= cd1.dt_start_ly
and cd1.dt_start_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end
and br.dt_start <= cd1.dt_end_ly
and cd1.dt_end_ly <= case when br.dt_end <= br.dt_inactive then br.dt_end
                        else br.dt_inactive end )
group by cd_booking_iata, cd_parent_iata);

USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP;

create or replace view &{RES_DB}.PROXY.RES_DIM_BIATA_PIATA_YTP_COMP as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP;



delete from res.main.RES_FACT_COMMISSION_STAYS_REPL repl
where  cd_stay_source_table in ( 'H','0');
					 



INSERT INTO RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL
   (CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	DT_DATE_ARRIVE,
	CD_MARKET_SEGMENT,
	QY_ROOMNIGHTS,
	CU_PH_REVENUE_USD,
	CU_PH_COMMISSION_USD,
	CD_PHI_CTAC_CURRENCY,
	CU_PHI_REVENUE_CTAC,
	CU_PHI_COMMISSION_CTAC,
	CU_TH_COMM_TAX_USD,
	CU_TH_COMM_TAX_CTAC,
	CD_REASON_CODE,
	ID_PROPERTY,
	CD_WEEK_ACCTG,
	CD_CONFO_NUM,
	QY_COMM_RATE_PCT,
	CD_GROUP_IATA,
	ID_MINI_HOTEL_SEQ,
	ID_GROUP_CONNECT,
	CU_GI_COMMISSION_USD,
	CU_GI_COMMISSION_CTAC,
	CU_GI_COMM_TAX_USD,
	CU_GI_COMM_TAX_CTAC,
	CD_STAY_SOURCE_TABLE,
	CD_MINI_HOTEL,
	ID_GI_YEAR_ACCTG,
	ID_GI_PERIOD_ACCTG,
	CD_GI_WEEK_ACCTG,
	CD_GI_REASON_CODE,
	QY_ATTR_PCT,
	QY_ROOMNIGHTS_ORIG
   )
   SELECT CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_SEQ_NUMBER,
	trim(CD_BOOKING_IATA),
	DT_DATE_ARRIVE,
	CD_MARKET_SEGMENT,
	QY_ROOMNIGHTS,
	CU_PH_REVENUE_USD,
	CU_PH_COMMISSION_USD,
	CD_PHI_CTAC_CURRENCY,
	CU_PHI_REVENUE_CTAC,
	CU_PHI_COMMISSION_CTAC,
	CU_TH_COMM_TAX_USD,
	CU_TH_COMM_TAX_CTAC,
	CD_REASON_CODE,
	ID_PROPERTY,
	CD_WEEK_ACCTG,
	CD_CONFO_NUM,
	QY_COMM_RATE_PCT,
	CD_GROUP_IATA,
	ID_MINI_HOTEL_SEQ,
	ID_GROUP_CONNECT,
	CU_GI_COMMISSION_USD,
	CU_GI_COMMISSION_CTAC,
	CU_GI_COMM_TAX_USD,
	CU_GI_COMM_TAX_CTAC,
	CD_STAY_SOURCE_TABLE,
	CD_MINI_HOTEL,
	ID_GI_YEAR_ACCTG,
	ID_GI_PERIOD_ACCTG,
	CD_GI_WEEK_ACCTG,
	CD_GI_REASON_CODE,
	QY_ATTR_PCT,
	QY_ROOMNIGHTS_ORIG
	FROM RES.STAGE.TMC_COMMISSION_STAYS_STG;
	
	
--DROP TABLE RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD;
use role sysadmin;
CREATE OR REPLACE TABLE RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD as
SELECT T.* FROM RES.STAGE.RDWT_TMC_COMMISSION_STAYS_STG T,
                     res.main.RES_FACT_COMMISSION_STAYS_REPL S
                     WHERE T.ID_GROUP_CONNECT = S.ID_GROUP_CONNECT 
                     AND T.ID_GROUP_CONNECT > 0;
					 
CREATE OR REPLACE TABLE RES.STAGE.T_TMC_COMMISSION_STAYS_STG as
SELECT * FROM RES.STAGE.TMC_COMMISSION_STAYS_STG;	


INSERT INTO RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL
(CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	DT_DATE_ARRIVE,
	CD_MARKET_SEGMENT,
	QY_ROOMNIGHTS,
	CU_PH_REVENUE_USD,
	CU_PH_COMMISSION_USD,
	CD_PHI_CTAC_CURRENCY,
	CU_PHI_REVENUE_CTAC,
	CU_PHI_COMMISSION_CTAC,
	CU_TH_COMM_TAX_USD,
	CU_TH_COMM_TAX_CTAC,
	CD_REASON_CODE,
	ID_PROPERTY,
	CD_WEEK_ACCTG,
	CD_CONFO_NUM,
	QY_COMM_RATE_PCT,
	CD_GROUP_IATA,
	ID_MINI_HOTEL_SEQ,
	ID_GROUP_CONNECT,
	CU_GI_COMMISSION_USD,
	CU_GI_COMMISSION_CTAC,
	CU_GI_COMM_TAX_USD,
	CU_GI_COMM_TAX_CTAC,
	CD_STAY_SOURCE_TABLE,
	CD_MINI_HOTEL,
	ID_GI_YEAR_ACCTG,
	ID_GI_PERIOD_ACCTG,
	CD_GI_WEEK_ACCTG,
	CD_GI_REASON_CODE,
	QY_ATTR_PCT,
	QY_ROOMNIGHTS_ORIG
   )
SELECT CD_PROPERTY, ID_YEAR_ACCTG, ID_PERIOD_ACCTG, ID_SEQ_NUMBER, CD_BOOKING_IATA, DT_DATE_ARRIVE, CD_MARKET_SEGMENT, QY_ROOMNIGHTS,
	CU_PH_REVENUE_USD, CU_PH_COMMISSION_USD, CD_PHI_CTAC_CURRENCY, CU_PHI_REVENUE_CTAC, CU_PHI_COMMISSION_CTAC,
	CU_TH_COMM_TAX_USD, CU_TH_COMM_TAX_CTAC, CD_REASON_CODE, ID_PROPERTY, CD_WEEK_ACCTG, CD_CONFO_NUM,
	QY_COMM_RATE_PCT, CD_GROUP_IATA, ID_MINI_HOTEL_SEQ, ID_GROUP_CONNECT, CU_GI_COMMISSION_USD,
	CU_GI_COMMISSION_CTAC, CU_GI_COMM_TAX_USD, CU_GI_COMM_TAX_CTAC, CD_STAY_SOURCE_TABLE, CD_MINI_HOTEL,
	ID_GI_YEAR_ACCTG, ID_GI_PERIOD_ACCTG, CD_GI_WEEK_ACCTG, CD_GI_REASON_CODE,100,QY_ROOMNIGHTS
    FROM RES.STAGE.RDWT_TMC_COMMISSION_STAYS_STG
	MINUS
	SELECT CD_PROPERTY, ID_YEAR_ACCTG, ID_PERIOD_ACCTG, ID_SEQ_NUMBER, CD_BOOKING_IATA, DT_DATE_ARRIVE, CD_MARKET_SEGMENT, QY_ROOMNIGHTS,
	CU_PH_REVENUE_USD, CU_PH_COMMISSION_USD, CD_PHI_CTAC_CURRENCY, CU_PHI_REVENUE_CTAC, CU_PHI_COMMISSION_CTAC,
	CU_TH_COMM_TAX_USD, CU_TH_COMM_TAX_CTAC, CD_REASON_CODE, ID_PROPERTY, CD_WEEK_ACCTG, CD_CONFO_NUM,
	QY_COMM_RATE_PCT, CD_GROUP_IATA, ID_MINI_HOTEL_SEQ, ID_GROUP_CONNECT, CU_GI_COMMISSION_USD,
	CU_GI_COMMISSION_CTAC, CU_GI_COMM_TAX_USD, CU_GI_COMM_TAX_CTAC, CD_STAY_SOURCE_TABLE, CD_MINI_HOTEL,
	ID_GI_YEAR_ACCTG, ID_GI_PERIOD_ACCTG, CD_GI_WEEK_ACCTG, CD_GI_REASON_CODE,100,QY_ROOMNIGHTS
    FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD;

	
	
	
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.QY_COMM_RATE_PCT = U.QY_COMM_RATE_PCT
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMMISSION_USD = U.CU_GI_COMMISSION_USD
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMMISSION_CTAC = U.CU_GI_COMMISSION_CTAC
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMM_TAX_USD = U.CU_GI_COMM_TAX_USD
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMM_TAX_CTAC = U.CU_GI_COMM_TAX_CTAC
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.ID_GI_YEAR_ACCTG = U.ID_GI_YEAR_ACCTG
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.ID_GI_PERIOD_ACCTG = U.ID_GI_PERIOD_ACCTG
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CD_GI_WEEK_ACCTG = U.CD_GI_WEEK_ACCTG
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'G';

---------------------------------------------------------------------------------------	
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMMISSION_USD = U.CU_GI_COMMISSION_USD
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'T';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMMISSION_CTAC = U.CU_GI_COMMISSION_CTAC
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'T';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMM_TAX_USD = U.CU_GI_COMM_TAX_USD
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'T';
	
	UPDATE RES.MAIN.RES_FACT_COMMISSION_STAYS_REPL s SET s.CU_GI_COMM_TAX_CTAC = U.CU_GI_COMM_TAX_CTAC
	FROM RES.STAGE.T_RDWS_COMMISSION_STAYS_UPD u
	WHERE s.ID_GROUP_CONNECT = U.ID_GROUP_CONNECT
	AND U.PH_STAY_TYPE = 'T';
	
	
	
	UPDATE res.main.RES_FACT_COMMISSION_STAYS_repl SET DT_DATE_ARRIVE = '0001-01-01'
 WHERE extract(year from dt_date_arrive) = '2001';



USE role sysadmin;
Truncate table IF EXISTS &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS_REPL_CLONE;
CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS;

create or replace view &{RES_DB}.PROXY.RES_FACT_COMMISSION_STAYS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS;


/**Complete refresh**/
TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS_REPL 
   (	CD_BOOKING_IATA, 
	DT_DATE_ARRIVE, 
	CD_PIATA_HISTORY, 
	CD_PIATA_CURRENT
)
(select distinct
trim(cs.cd_booking_iata),
cs.dt_date_arrive,
br_history.cd_parent_iata,
br_current.cd_parent_iata
from &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS cs,
     &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br_history,
     &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_VS_BIATA_REL br_current
where cs.cd_booking_iata = br_history.cd_booking_iata(+)
and case when (to_char(cs.dt_date_arrive, 'YYYY')) <  '2004' then to_date('01/01/2004', 'MM/DD/YYYY') else cs.dt_date_arrive end
    between br_history.dt_start(+) and br_history.dt_end(+)
and cs.cd_booking_iata = br_current.cd_booking_iata(+)  
and current_date() between br_current.dt_start(+) and br_current.dt_end(+));

-- update &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS bp
-- set cd_1p_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1P_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_3p_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3P_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_ytp_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTP_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_fyp_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYP_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_1m_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_1M_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_3m_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_3M_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_ytm_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_YTM_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history),
--     cd_fym_comp = (select cd_comp_flag
--                   from &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_BIATA_PIATA_FYM_COMP comp
--                   where comp.cd_booking_iata = bp.cd_booking_iata
--                   and comp.cd_parent_iata = bp.cd_piata_history);
-- COMMIT;
-- 1
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_3p_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_3P_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;

--2
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_ytp_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_YTP_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;

--3
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_1P_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_1P_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;
                                                              
--4
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_fyp_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_fyp_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;
                                                                                                                          
--5                                                                                                    
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_1m_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_1m_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;
                                                                                                                       
--6
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_3m_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_3m_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;

--7
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_ytm_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_ytm_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;
                                                              
--8                                          
update RES.MAIN.RES_FACT_BIATA_PIATAS_REPL bp 
set bp.cd_fym_comp = comp.cd_comp_flag 
from RES.MAIN.RES_DIM_BIATA_PIATA_fym_COMP comp
where comp.cd_booking_iata = bp.cd_booking_iata
                  and comp.cd_parent_iata = bp.cd_piata_history;

COMMIT;
USE role sysadmin;

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS;

create or replace view &{RES_DB}.PROXY.RES_FACT_BIATA_PIATAS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_BIATA_PIATAS;



TRUNCATE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS_REPL;


INSERT INTO &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS_REPL
      (CD_BIATA)
      SELECT DISTINCT CD_GROUP_IATA
      FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_STAYS
	    where CD_GROUP_IATA is not null;

 --1     
     UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_1P = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '1P'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_1P = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '1P'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;	
								  
								
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_1P = 'N'
                                            WHERE FL_GI_COM_1P IS NULL;                           
      
 --2     
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_1M = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '1M'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_1M = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '1M'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;	
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_1M = 'N'
                                            WHERE FL_GI_COM_1M IS NULL;    
                                            
--3
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_3P = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '3P'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_3P = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '3P'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_3P = 'N'
                                            WHERE FL_GI_COM_3P IS NULL;   
                                            
--4
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_3M = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '3M'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_3M = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = '3M'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_3M = 'N'
                                            WHERE FL_GI_COM_3M IS NULL;   
                                            
--5
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_YTP = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'YTP'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_YTP = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'YTP'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_YTP = 'N'
                                            WHERE FL_GI_COM_YTP IS NULL; 
                                            
--6
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_YTM = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'YTM'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_YTM = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'YTM'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_YTM = 'N'
                                            WHERE FL_GI_COM_YTM IS NULL;   
                                            
--7
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_FYP = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'FYP'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_FYP = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'FYP'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_FYP = 'N'
                                            WHERE FL_GI_COM_FYP IS NULL;   
                                            
--8
         UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_FYM = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'FYM'
	                             AND I.CD_STATUS_IND <> 'X';
	
UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL R SET FL_GI_COM_FYM = 'Y'
                                   FROM RES.MAIN.RES_DIM_VS_IATA_MASTER I,
                                   RES.MAIN.RES_DIM_TMC_COMP_DATES C 
                                   WHERE R.CD_BIATA = I.CD_BOOKING_IATA
                                   AND I.DT_BIATA_CREATE <= C.DT_START_LY
								   AND C.TX_DATE_CODE = 'FYM'
	                             AND I.CD_STATUS_IND = 'X' 
		                        AND I.DT_STATUS_DATE >= C.DT_END_TY;
                                
      UPDATE RES.MAIN.RES_DIM_GI_COMPS_REPL SET FL_GI_COM_FYM = 'N'
                                            WHERE FL_GI_COM_FYM IS NULL;                                               
      
	  
	  CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS;

create or replace view &{RES_DB}.PROXY.RES_DIM_GI_COMPS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_GI_COMPS;

-------------------------------------------------------------------------------------------------------------------------
--EAGENT tables

create temp table res.main.get_run_params_temp as SELECT
       CASE
       WHEN extract(dayofweek from current_date) = 0
             THEN current_date - 4
       WHEN extract(dayofweek from current_date) = 6
             THEN current_date - 3
       WHEN extract(dayofweek from current_date) = 2
             THEN current_date - 6
       WHEN extract(dayofweek from current_date) = 3
             THEN current_date - 7   
          WHEN extract(dayofweek from current_date) = 1
             THEN current_date - 5        
       END  Last_Wednesday
       ;
       

create temp table res.main.get_run_min_date_temp as 
SELECT  date_key as min_date
FROM res.main.mrdw_dim_date dt, res.main.get_run_params_temp run_date
WHERE dt.date_dt = last_wednesday - 6;


create temp table res.main.get_run_max_date_temp as 
SELECT  date_key as max_date
FROM res.main.mrdw_dim_date dt, res.main.get_run_params_temp run_date
WHERE dt.date_dt = last_wednesday;


create temp table res.main.temp_eagent_date as
SELECT max(dt2.date_dt) date_dt,dr2.confo_num_orig_id,dr2.date_create_key,dr2.confo_num_curr_id 
                        FROM res.main.mrdw_fact_daily_revenue dr2
                          INNER JOIN res.main.mrdw_dim_currency_conversion cc2
                             ON dr2.date_currency_depart_key = cc2.date_currency_key
                          INNER JOIN res.main.mrdw_dim_date dt2
                             ON cc2.date_key = dt2.date_key
                          WHERE  dt2.date_key >= (select min_date from res.main.get_run_min_date_temp)
                          AND dt2.date_key <= (select max_date from res.main.get_run_max_date_temp)
                          group by dr2.confo_num_orig_id,dr2.date_create_key,dr2.confo_num_curr_id;
						  
						  
create temp table res.main.distinct_eagent_temp_date as
select distinct date_dt,confo_num_orig_id,date_create_key,confo_num_curr_id  from res.main.temp_eagent_date; 						  
						  
						  
						  
CREATE TEMP TABLE RES.MAIN.mrdw_stg_prev_depart_daily_rev_key AS
  SELECT DISTINCT
   dr.confo_num_curr_id,
   dr.confo_num_orig_id,
   dr.date_create_key
  FROM res.main.mrdw_fact_daily_revenue dr
  INNER JOIN res.main.mrdw_dim_currency_conversion cc1
     ON dr.date_currency_depart_key = cc1.date_currency_key
  INNER JOIN res.main.mrdw_dim_date dt1
     ON cc1.date_key = dt1.date_key
  INNER JOIN res.main.mrdw_dim_status_accom sa
     ON dr.status_accom_key = sa.status_accom_key
  INNER JOIN res.main.MRDW_BRG_INTERMEDIARY BRG
          ON ( DR.CONFO_NUM_CURR_ID = BRG.CONFO_NUM_CURR_ID and
               DR.CONFO_NUM_ORIG_ID = BRG.CONFO_NUM_ORIG_ID and
               DR.DATE_CREATE_KEY = BRG.DATE_CREATE_KEY
             )
  INNER JOIN res.main.MRDW_DIM_INTERMEDIARY IM
          ON ( BRG.INTERMEDIARY_KEY = IM.INTERMEDIARY_KEY and
               BRG.INTERMEDIARY_TYPE_RESV_CD = IM.INTERMEDIARY_TYPE_CD
              )
  INNER JOIN res.main.mrdw_dim_eagent_lookup lk
     ON (IM.intermediary_id = lk.martan_iata_number)
  WHERE dr.year_cal_stay >= 2020
     AND dt1.date_dt = (SELECT max(t.date_dt) from  res.main.distinct_eagent_temp_date t
                          WHERE t.date_create_key = dr.date_create_key
                          AND t.confo_num_curr_id = dr.confo_num_curr_id
                          AND t.confo_num_orig_id = dr.confo_num_orig_id
                        )
     AND dt1.date_key >= (select min_date from res.main.get_run_min_date_temp)
     AND dt1.date_key <= (select max_date from res.main.get_run_max_date_temp)
     AND sa.action_cd <> 'NR';
	 
	 
	 DELETE FROM  RES.MAIN.mrdw_stg_prev_depart_daily_rev_key
WHERE (confo_num_orig_id||date_create_key)
IN (SELECT distinct dr.confo_num_orig_id||dr.date_create_key
 FROM res.main.mrdw_fact_daily_revenue dr
      INNER JOIN res.main.mrdw_dim_currency_conversion cc1
        ON dr.date_currency_depart_key = cc1.date_currency_key
      INNER JOIN res.main.mrdw_dim_date dt1
        ON cc1.date_key = dt1.date_key
 WHERE  dr.year_cal_stay >= 2020
       AND  dt1.date_key > (select max_date from res.main.get_run_max_date_temp)
       AND ((dr.confo_num_orig_id||dr.date_create_key)
           IN (SELECT (pdk.confo_num_orig_id||pdk.date_create_key)
               FROM RES.MAIN.mrdw_stg_prev_depart_daily_rev_key pdk)));
	 

truncate table res.main.mrdw_stg_depart_daily_rev_key ;

insert into RES.MAIN.mrdw_stg_depart_daily_rev_key (
select distinct confo_num_curr_id ,      confo_num_orig_id,       date_create_key
from RES.MAIN.mrdw_stg_prev_depart_daily_rev_key );



---Eagent header

--delete
DELETE FROM RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL
where DW_LOAD_TS = current_date;
 

 
--update 
 create temp table res.main.eagent_upd_temp as
 select distinct confo_num_orig_id,property_cd, create_dt,dw_load_ts from 
(SELECT  DISTINCT comp.confo_num_orig_id, dt.date_dt create_dt,  pr.property_cd,'N' Paid_flag, current_date UPDATE_DT,
current_date dw_load_ts,
SUM(atd.rooms_qty * atd.days_stay_qty)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) room_nights,
SUM(ao.proj_net_local_rev_amt)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) orig_net_rev_amt,
MAX(dt2.date_dt)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) departure_dt
from RES.MAIN.res_fact_accom_to_date_EADS atd
  LEFT OUTER JOIN  res.main.mrdw_fact_accom_orig ao on (  ----res.main.mrdw_fact_accom_orig
               atd.confo_num_curr_id = ao.confo_num_curr_id
           AND atd.confo_num_orig_id = ao.confo_num_orig_id
           AND atd.date_create_key   = ao.date_create_key
           AND atd.room_pool_key     = ao.room_pool_key
           AND atd.date_arrival_key  = ao.date_arrival_key )
  INNER JOIN  res.main.mrdw_dim_currency_conversion cc1 on
          ( atd.date_currency_depart_key = cc1.date_currency_key)
  INNER JOIN res.main.mrdw_stg_depart_daily_rev_key comp on
      (atd.confo_num_orig_id = comp.confo_num_orig_id
       AND atd.confo_num_curr_id = comp.confo_num_curr_id
       AND atd.date_create_key = comp.date_create_key) 
  INNER JOIN res.main.mrdw_dim_room_pool rp on
      (atd.room_pool_key = rp.room_pool_key)
  INNER JOIN pty.main.mrdw_dim_property pr on
      (rp.property_id = pr.property_id)
  INNER JOIN res.main.mrdw_dim_date dt on
      (atd.date_create_key = dt.date_key)
  INNER JOIN res.main.mrdw_dim_date dt2 on
      (cc1.date_key = dt2.date_key)
  LEFT OUTER JOIN res.main.mrdw_dim_status_accom sa on
      (ao.status_accom_key = sa.status_accom_key and
        sa.status_accom_key = ao.status_accom_key and
        (sa.action_cd <> 'NX' and sa.action_cd <> 'WL' and
     ((sa.action_cd <> 'NR') or
      (sa.action_cd = 'NR' and sa.day_room_ind = 1)
       )
      ))
  LEFT OUTER JOIN res.main.mrdw_dim_source_booking sb on
      (ao.book_source_orig_key = sb.book_source_key and
        not ((ao.confo_num_orig_id <> ao.confo_num_orig_id) and
         (sb.agent_initials_txt = '01') and
           (sb.duty_cd = 'PR')
        )))
		  INTERSECT
        (select distinct confo_num_orig_id,property_cd, create_dt,dw_load_ts
 from RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL);
 
 
 --Update
 /* UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	CONFO_NUM_ORIG_ID = CONFO_NUM_ORIG_ID from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	CREATE_DT = CREATE_DT from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	PROPERTY_CD = PROPERTY_CD from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	PAID_FLAG = PAID_FLAG from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	UPDATE_DT = UPDATE_DT from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;		

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	DW_LOAD_TS = DW_LOAD_TS from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;		

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	ROOM_NIGHTS = ROOM_NIGHTS from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;		

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	ORIGINAL_NET_REV_AMT = ORIGINAL_NET_REV_AMT from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;	

 UPDATE RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL r
set
	DEPARTURE_DT = DEPARTURE_DT from res.main.eagent_upd_temp t
	                       where r.confo_num_orig_id = t.confo_num_orig_id
						   and r.property_cd = t.property_cd
						   and r.create_dt = t.create_dt
						   and r.dw_load_ts = t.dw_load_ts;		*/						   

--Insert
INSERT INTO RES.MAIN.RES_DIM_TMC_EAGENT_HEADER_REPL
(
	CONFO_NUM_ORIG_ID,
	CREATE_DT,
	PROPERTY_CD,
	PAID_FLAG,
	UPDATE_DT,
	DW_LOAD_TS,
	ROOM_NIGHTS,
	ORIGINAL_NET_REV_AMT,
	DEPARTURE_DT
)
SELECT  DISTINCT comp.confo_num_orig_id, dt.date_dt create_dt,  pr.property_cd,'N' Paid_flag, current_date UPDATE_DT,
current_date dw_load_ts,
SUM(atd.rooms_qty * atd.days_stay_qty)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) room_nights,
SUM(ao.proj_net_local_rev_amt)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) orig_net_rev_amt,
MAX(dt2.date_dt)
       OVER ( PARTITION BY  
                           atd.confo_num_orig_id,
                           atd.date_create_key ) departure_dt
from RES.MAIN.res_fact_accom_to_date_EADS atd
  LEFT OUTER JOIN  res.main.mrdw_fact_accom_orig ao on (
               atd.confo_num_curr_id = ao.confo_num_curr_id
           AND atd.confo_num_orig_id = ao.confo_num_orig_id
           AND atd.date_create_key   = ao.date_create_key
           AND atd.room_pool_key     = ao.room_pool_key
           AND atd.date_arrival_key  = ao.date_arrival_key )
  INNER JOIN  res.main.mrdw_dim_currency_conversion cc1 on
          ( atd.date_currency_depart_key = cc1.date_currency_key)
  INNER JOIN res.main.mrdw_stg_depart_daily_rev_key comp on
      (atd.confo_num_orig_id = comp.confo_num_orig_id
       AND atd.confo_num_curr_id = comp.confo_num_curr_id
       AND atd.date_create_key = comp.date_create_key) 
  INNER JOIN res.main.mrdw_dim_room_pool rp on
      (atd.room_pool_key = rp.room_pool_key)
  INNER JOIN pty.main.mrdw_dim_property pr on
      (rp.property_id = pr.property_id)
  INNER JOIN res.main.mrdw_dim_date dt on
      (atd.date_create_key = dt.date_key)
  INNER JOIN res.main.mrdw_dim_date dt2 on
      (cc1.date_key = dt2.date_key);
/*  LEFT OUTER JOIN res.main.mrdw_dim_status_accom sa on
      (ao.status_accom_key = sa.status_accom_key and
        (sa.action_cd <> 'NX' and sa.action_cd <> 'WL' and
     ((sa.action_cd <> 'NR') or
      (sa.action_cd = 'NR' and sa.day_room_ind = 1)
       )
      ))
  LEFT OUTER JOIN res.main.mrdw_dim_source_booking sb on
      (ao.book_source_orig_key = sb.book_source_key and
        not ((ao.confo_num_orig_id <> ao.confo_num_orig_id) and
         (sb.agent_initials_txt = '01') and
           (sb.duty_cd = 'PR')
        ))
		;*/
		--50000

CREATE OR REPLACE TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_EAGENT_HEADER_REPL_CLONE clone &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_EAGENT_HEADER_REPL;


ALTER TABLE &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_EAGENT_HEADER_REPL_CLONE swap with &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_EAGENT_HEADER;

create or replace view &{RES_DB}.PROXY.RES_DIM_TMC_EAGENT_HEADER as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_TMC_EAGENT_HEADER;



---Eagent details
INSERT INTO RES.MAIN.RES_DIM_EAGENT_DETAIL_REPL
(CONFO_NUM_CURR_ID,   
CONFO_NUM_ORIG_ID,   
CREATE_DT,         
ROOM_POOL_CD, 
ARRIVAL_DT,         
PROPERTY_CD, 
PAID_FLAG,               
DW_LOAD_TS)
SELECT comp.confo_num_curr_id, comp.confo_num_orig_id, dt_cr.date_dt create_dt, rp.room_pool_cd,
       dt_ar.date_dt arrival_dt, pr.property_cd,'N' Paid_flag, current_date dw_load_ts
FROM RES.MAIN.res_fact_accom_to_date_EADS atd,
         res.main.mrdw_fact_accom_orig ao,
     res.main.mrdw_dim_status_accom sa,
     res.main.mrdw_stg_depart_daily_rev_key comp,
     res.main.mrdw_dim_room_pool rp,
     pty.main.mrdw_dim_property pr,
     res.main.mrdw_dim_date dt_cr,
     res.main.mrdw_dim_date dt_ar
WHERE atd.confo_num_curr_id = ao.confo_num_curr_id(+)
           AND atd.confo_num_orig_id = ao.confo_num_orig_id(+)
           AND atd.date_create_key   = ao.date_create_key(+)
           AND atd.room_pool_key     = ao.room_pool_key(+)
           AND atd.date_arrival_key  = ao.date_arrival_key(+)
       AND   ao.status_accom_key = sa.status_accom_key(+)
       and atd.room_pool_key = rp.room_pool_key
AND   rp.property_id = pr.property_id
AND   atd.confo_num_curr_id = comp.confo_num_curr_id
AND   atd.confo_num_orig_id = comp.confo_num_orig_id
AND   atd.date_create_key = comp.date_create_key
AND   atd.date_create_key = dt_cr.date_key
AND   atd.date_arrival_key = dt_ar.date_key
AND   sa.action_cd(+) <> 'NR'
AND   sa.status_cd(+) <> 'X';


CREATE OR REPLACE TABLE RES.MAIN.RES_DIM_EAGENT_DETAIL_REPL_CLONE clone RES.MAIN.RES_DIM_EAGENT_DETAIL_REPL;




ALTER TABLE RES.MAIN.RES_DIM_EAGENT_DETAIL_REPL_CLONE swap with RES.MAIN.RES_DIM_EAGENT_DETAIL;



create or replace view RES.PROXY.RES_DIM_EAGENT_DETAIL as select * FROM RES.MAIN.RES_DIM_EAGENT_DETAIL;
 
--Eagent Hist
----------------------------------------------------------------------------------------------------------------------------
--FACT_MRDW_CONFO
--change here for repl
truncate table RES.MAIN.RES_FACT_COMMISSION_CONFOS;

 INSERT INTO RES.MAIN.RES_FACT_COMMISSION_CONFOS
(CD_PROPERTY, 
	CD_CONFO_NUM, 
	DT_DATE_ARRIVE)
select distinct cs.cd_property,
       cs.cd_confo_num,
       cs.dt_date_arrive
from RES.MAIN.RES_FACT_COMMISSION_STAYS CS
where LENGTH(TRIM(TRANSLATE(cd_confo_num, '0123456789','          '))) = 0
and  cs.cd_confo_num between '70000000' and '99999999'
and  not exists (select 'x'
                   from RES.MAIN.RES_FACT_MRDW_CONFO mc
                   where mc.cd_property = cs.cd_property
                   and mc.cd_confo_num_cur = cs.cd_confo_num
                   and mc.dt_date_arrive = cs.dt_date_arrive); 

create or replace view &{RES_DB}.PROXY.RES_FACT_COMMISSION_CONFOS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_COMMISSION_CONFOS;


truncate table RES.MAIN.RES_DIM_COMMISSION_CONFOS;

INSERT INTO RES.MAIN.RES_DIM_COMMISSION_CONFOS
SELECT   CD_PROPERTY,  CD_CONFO_NUM ,  DT_DATE_ARRIVE 
   FROM RES.MAIN.RES_FACT_COMMISSION_CONFOS;

create or replace view &{RES_DB}.PROXY.RES_DIM_COMMISSION_CONFOS as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_COMMISSION_CONFOS;


insert into res.main.RES_FACT_MRDW_CONFO_REPL
(
CD_PROPERTY,
	CD_CONFO_NUM_CUR,
	DT_DATE_ARRIVE,
	CD_MARKET,
	CD_RATE_PGM,
	CD_BOOK_SOURCE,
	CD_COMPANY,
	NM_COMPANY,
	ID_DERIVED_ACCOUNT,
	NM_COMPANY_CLEANSED,
	DT_DATE_CREATE,
	CD_CONFO_NUM_ORIG,
	CD_BOOK_OFFICE,
	ID_MKT_PFIX_SFIX)
select  property_cd,
        confo_num_cd,
		date_arrival_dt,
		market_cd,
		rate_pgm_cd ,
		orig_book_source_cd,
		company_cd ,
		company_nm,
		derived_account_id , 
		company_cleansed_nm,
		dt_date_create,
		confo_num_orig_id,
		orig_book_office_cd,
		mkt_pfix_sfix_key
		from (
select rtrim(cc.cd_property) as property_cd,
       rtrim(cc.cd_confo_num) as  confo_num_cd,
       cc.dt_date_arrive as date_arrival_dt,
       row_number() over( partition by cc.dt_date_arrive, property_cd, confo_num_cd order by rp.room_pool_key asc) rn,
       rtrim(ac.market_cd) as market_cd,
       rtrim(ac.rate_pgm_cd) as rate_pgm_cd ,
       ac.orig_book_source_cd,
       rtrim(ac.company_cd) as company_cd ,
       rtrim(ac.company_nm) as company_nm,
       ac.derived_account_id , 
	   Substr(dc.company_cleansed_nm,1,40) as company_cleansed_nm,
       ac.confo_create_dt dt_date_create,
	   ac.confo_num_orig_id,
       ac.orig_book_office_cd,
       ac.mkt_pfix_sfix_key
from res.main.RES_dim_COMMISSION_CONFOS cc
inner join
  RES.MAIN.res_fact_accom_to_date_EADS  ac on (cc.dt_date_arrive = ac.accom_arrival_dt
  and  to_number(replace(cc.cd_confo_num,' ','')) =  ac.confo_num_curr_id 
                                              and cc.cd_property = ac.property_cd
                                        )
inner join
  res.main.mrdw_dim_room_pool rp on  (ac.room_pool_key = rp.room_pool_key) 
left outer join  res.main.mrdw_dim_unmatched_company dc on ( dc.unmatched_company_key = ac.unmatched_company_key )
where ac.rooms_qty > 0
and ac.res_status_cd != 'X'
and ac.accom_action_cd  in ('HF', 'HK', 'HS')
order by 1,2,3,4,5,6,7,8,9,10,11,12)
where rn=1;



CREATE OR REPLACE TABLE RES.MAIN.RES_FACT_MRDW_CONFO_REPL_CLONE clone RES.MAIN.RES_FACT_MRDW_CONFO_REPL;

ALTER TABLE RES.MAIN.RES_FACT_MRDW_CONFO_REPL_CLONE swap with RES.MAIN.RES_FACT_MRDW_CONFO;

create or replace view &{RES_DB}.PROXY.RES_FACT_MRDW_CONFO as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_FACT_MRDW_CONFO;


truncate table RES.MAIN.RES_DIM_DERIVED_ACCOUNT;

INSERT INTO RES.MAIN.RES_DIM_DERIVED_ACCOUNT
(DERIVED_ACCOUNT_ID)
SELECT distinct ID_DERIVED_ACCOUNT FROM RES.MAIN.RES_FACT_MRDW_CONFO;


create or replace view &{RES_DB}.PROXY.RES_DIM_DERIVED_ACCOUNT as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_DERIVED_ACCOUNT;



truncate table RES.MAIN.RES_DIM_HIGHEST_LEVEL_ACCOUNT;

INSERT INTO RES.MAIN.RES_DIM_HIGHEST_LEVEL_ACCOUNT
(ID_DERIVED_ACCOUNT,
ID_HIGHEST_LEVEL_BUSINESS,
NM_HIGHEST_LEVEL_ACCOUNT)
select  da.derived_account_id,eha.highest_level_business_id,substr(eha.highest_level_account_name,1,40)
from RES.MAIN.RES_DIM_DERIVED_ACCOUNT da,
      RES.MAIN.edw_dim_highest_level_account_v eha
where eha.account_nat_key =  da.derived_account_id;

create or replace view &{RES_DB}.PROXY.RES_DIM_HIGHEST_LEVEL_ACCOUNT as select * FROM &{RES_DB}.&{MAIN_SCHEMA}.RES_DIM_HIGHEST_LEVEL_ACCOUNT;


-------------------vs_extract--------------------------------------------------------

/* CREATE OR REPLACE VIEW RES.MAIN.RES_TMC_MRDW_CONFO_V (CD_PROPERTY, CD_CONFO_NUM_CUR, DT_DATE_ARRIVE, NM_PFIX_SFIX) AS 
select tmc.cd_property, tmc.cd_confo_num_cur, tmc.dt_date_arrive,
         decode (mc.sort_mkt_ctgy_id ,2,'GROUP',3,'CONTRACT',4,'COMP' , mpc.pfix_sfix_cd || ' ' || mpc.pfix_sfix_nm ) nm_pfix_sfix
from  res.main.RES_FACT_MRDW_CONFO tmc,
      res.main.mrdw_dim_mkt_pfix_sfix_cntl mpc,
      res.main.MRDW_DIM_MARKET_SEGMENT_SAT ms,
      res.main.MRDW_DIM_MARKET_CATEGORY_SAT mc
where tmc.id_mkt_pfix_sfix = mpc.mkt_pfix_sfix_key(+)
and   mpc.market_seg_key = ms.market_seg_key(+)
and   ms.market_ctgy_key = mc.market_ctgy_key(+); */



create or replace table res.main.tmc_vs_extract_temp1 as
select
       cs.cd_property,
       cs.id_year_acctg,
       cs.id_period_acctg,
       cs.cd_week_acctg,
       cs.id_seq_number,
       cs.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       cs.cd_reason_code,
       cs.cd_confo_num,
       cs.dt_date_arrive,
       cs.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      cs.qy_roomnights_orig,
       decode (cs.qy_roomnights_orig, 0, 1, cs.qy_roomnights_orig) as qy_roomnights,
       decode (cs.qy_roomnights_orig, 0, 0, (cs.qy_roomnights/cs.qy_roomnights_orig)) as stay_nights,
       cs.cu_ph_revenue_usd,
      (cu_ph_commission_usd + cu_th_comm_tax_usd) as cu_ph_commission_usd,
       0 as cu_bd_fee_taxes_usd,
       0 as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp) as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       qy_comm_rate_pct,
       cd_group_iata,
       id_mini_hotel_seq,
       id_group_connect,
       cu_gi_commission_usd,
       cu_gi_commission_ctac,
       cu_gi_comm_tax_usd,
       cu_gi_comm_tax_ctac,
       cd_stay_source_table,
       cd_phi_ctac_currency,
       cu_phi_revenue_ctac,
       cu_phi_commission_ctac,
       cd_mini_hotel,
       id_gi_year_acctg,
       id_gi_period_acctg,
       cd_gi_week_acctg,
       id_derived_account,
       nm_company_cleansed
from res.main.RES_FACT_COMMISSION_STAYS cs,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO mc,
     res.main.mrdw_dim_date dt,
     pty.main.mrdw_dim_property pr
where pr.brand_cd != 'NR'
and cs.id_property = pr.property_id
AND NOT EXISTS
(SELECT 'x' FROM res.main.RES_DIM_TMC_STAYS TS 
WHERE TS.ID_PROPERTY = CS.ID_PROPERTY AND TS.ID_SEQ_NUMBER = CS.ID_SEQ_NUMBER 
AND (CS.CD_STAY_SOURCE_TABLE = 'P' OR CS.CD_STAY_SOURCE_TABLE = 'R' OR CS.CD_STAY_SOURCE_TABLE IS NULL)) 
AND (cs.id_year_acctg >=2012 or cs.id_gi_year_acctg >=2012) 
and dt.date_dt between cs.dt_date_arrive and (cs.dt_date_arrive + (decode(cs.qy_roomnights_orig, 0, 0, cs.qy_roomnights_orig - 1))) 
and bp.cd_booking_iata(+) = cs.cd_booking_iata 
and bp.dt_date_arrive(+) = cs.dt_date_arrive 
--and (cs.qy_roomnights != 0 OR cs.cu_ph_revenue_usd != 0)
and (cs.qy_roomnights + cs.cu_ph_revenue_usd + cs.cu_gi_commission_ctac) != 0
--     OR (cu_ph_commission_usd + cu_th_comm_tax_usd) != 0
--     OR (cu_gi_commission_usd + cu_gi_comm_tax_usd) != 0)and cs.cd_property = mc.cd_property (+)
and mc.cd_property (+) = cs.cd_property
and mc.cd_confo_num_cur (+) = cs.cd_confo_num and mc.dt_date_arrive (+) = cs.dt_date_arrive; 



create or replace table res.main.tmc_vs_extract_temp2 as
select 
       cs.cd_property,
       cs.id_year_acctg,
      cs.id_period_acctg,
      cs.cd_week_acctg,
       cs.id_seq_number,
        cs.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       cs.cd_reason_code,
       cs.cd_confo_num,
       cs.dt_date_arrive,
       cs.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      cs.qy_roomnights_orig,
       0 as qy_roomnights,
       decode (cs.qy_roomnights, 0, 0, 1) as stay_nights,
       0 as cu_ph_revenue_usd,
      (cu_ph_commission_usd + cu_th_comm_tax_usd) as cu_ph_commission_usd,
       0 as cu_bd_fee_taxes_usd,
       0 as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp) as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       qy_comm_rate_pct,
       cd_group_iata,
       id_mini_hotel_seq,
       id_group_connect,
       cu_gi_commission_usd,
       cu_gi_commission_ctac,
       cu_gi_comm_tax_usd,
       cu_gi_comm_tax_ctac,
       cd_stay_source_table,
       cd_phi_ctac_currency,
       0 as cu_phi_revenue_ctac,
       cu_phi_commission_ctac,
       cd_mini_hotel,
       id_gi_year_acctg,
       id_gi_period_acctg,
       cd_gi_week_acctg,
       mc.id_derived_account,
       mc.nm_company_cleansed
from res.main.RES_FACT_COMMISSION_STAYS cs,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO mc,
     res.main.mrdw_dim_date dt,
     pty.main.mrdw_dim_property pr
where pr.brand_cd != 'NR'
and cs.id_property=pr.property_id
and EXISTS
(SELECT 'x' FROM RES.MAIN.RES_DIM_TMC_STAYS TS 
WHERE TS.ID_PROPERTY = CS.ID_PROPERTY AND TS.ID_SEQ_NUMBER = CS.ID_SEQ_NUMBER 
AND (CS.CD_STAY_SOURCE_TABLE = 'P' OR CS.CD_STAY_SOURCE_TABLE = 'R' OR CS.CD_STAY_SOURCE_TABLE IS NULL)) 
and (cs.id_year_acctg >=2012 or cs.id_gi_year_acctg >=2012) 
and dt.date_dt between cs.dt_date_arrive and (cs.dt_date_arrive + (decode(cs.qy_roomnights_orig, 0, 0, cs.qy_roomnights_orig - 1))) 
and bp.cd_booking_iata(+) = cs.cd_booking_iata 
and bp.dt_date_arrive(+) = cs.dt_date_arrive 
and mc.cd_property (+) = cs.cd_property 
and mc.cd_confo_num_cur (+) = cs.cd_confo_num 
and mc.dt_date_arrive (+) = cs.dt_date_arrive;



create or replace table res.main.tmc_vs_extract_temp3 as
select 
       ts.cd_property,
       ts.id_year_acctg,
       ts.id_period_acctg,
       ts.cd_week_acctg,
       ts.id_seq_number,
       ts.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       ts.cd_tm6_reason_code cd_reason_code,
       ts.cd_confo_num,
       ts.dt_date_arrive,
       ts.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      ts.qy_roomnights as qy_roomnights_orig,
       decode(ts.qy_roomnights, 0, 1, ts.qy_roomnights) as qy_roomnights,
       decode(ts.qy_roomnights, 0, 0, 1) as stay_nights,
       ts.cu_tm6_revenue_usd cu_ph_revenue_usd,
       0 as cu_ph_commission_usd,
      (cu_tm9_fee_amt_usd + cu_tm9_city_tax_usd + cu_tm9_cntry_tax_usd + cu_tm9_prvn_tax_usd)  as cu_bd_fee_taxes_usd,
      (cu_tm6_pref_amt_usd + cu_tm6_city_tax_usd + cu_tm6_cntry_tax_usd + cu_tm6_prvn_tax_usd) as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp)  as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       0 qy_comm_rate_pct,
       NULL cd_group_iata,
       0 id_mini_hotel_seq,
       0 id_group_connect,
       0 cu_gi_commission_usd,
       0 cu_gi_commission_ctac,
       0 cu_gi_comm_tax_usd,
       0 cu_gi_comm_tax_ctac,
       NULL cd_stay_source_table,
       NULL cd_phi_ctac_currency,
       cu_tm6_revenue_ctac as cu_phi_revenue_ctac,
       0 cu_phi_commission_ctac,
       NULL cd_mini_hotel,
       0 id_gi_year_acctg,
       0 id_gi_period_acctg,
       NULL cd_gi_week_acctg,
       mc.id_derived_account,
       mc.nm_company_cleansed
from res.main.RES_DIM_TMC_STAYS ts,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO  mc,
     res.main.mrdw_dim_date dt,
     pty.main.mrdw_dim_property pr
  where pr.brand_cd != 'NR'
and ts.id_property = pr.property_id
and ts.id_year_acctg >=2012
and bp.cd_booking_iata = ts.cd_booking_iata 
and bp.dt_date_arrive = ts.dt_date_arrive 
and (ts.qy_roomnights != 0 OR ts.cu_tm6_revenue_usd != 0
     OR (cu_tm9_fee_amt_usd + cu_tm9_city_tax_usd + cu_tm9_cntry_tax_usd + cu_tm9_prvn_tax_usd) != 0
     OR (cu_tm6_pref_amt_usd + cu_tm6_city_tax_usd + cu_tm6_cntry_tax_usd + cu_tm6_prvn_tax_usd) != 0) 
and mc.cd_property(+) = ts.cd_property 
and mc.cd_confo_num_cur(+) = ts.cd_confo_num 
and mc.dt_date_arrive(+) = ts.dt_date_arrive 
and dt.date_dt between ts.dt_date_arrive and (ts.dt_date_arrive + decode(ts.qy_roomnights, 0, 0, (abs(ts.qy_roomnights) - 1)));


create or replace table res.main.tmc_vs_extract_temp as
select 
       cd_property,
       id_year_acctg,
       id_period_acctg,
       min(cd_week_acctg) as cd_week_acctg,
       id_seq_number,
       cd_booking_iata,
       cd_piata_current,
       cd_piata_history,
       cd_reason_code,
       cd_confo_num,
       dt_date_arrive,
       cd_ctac_market_segment,
       cd_mrdw_market_segment,
       cd_mrdw_rate_pgm,
       cd_mrdw_book_source,
       cd_company,
       nm_company,
       date_dt dt_date,
       qy_roomnights_orig,
       sum(qy_roomnights) as qy_roomnights,
       stay_nights,
       sum(cu_ph_revenue_usd) as cu_ph_revenue_usd,
       sum(cu_ph_commission_usd) as cu_ph_commission_usd,
       sum(cu_bd_fee_taxes_usd) as cu_bd_fee_taxes_usd,
       sum(cu_pref_pay_taxes_usd) as cu_pref_pay_taxes_usd,
       cd_1p_comp,
       cd_3p_comp,
       cd_ytp_comp,
       cd_fyp_comp,
       cd_1m_comp,
       cd_3m_comp,
       cd_ytm_comp,
       cd_fym_comp,
       max(qy_comm_rate_pct) as qy_comm_rate_pct,
       max(cd_group_iata) as cd_group_iata,
       max(id_mini_hotel_seq) as id_mini_hotel_seq,
       max(id_group_connect) as id_group_connect,
       sum(cu_gi_commission_usd) as cu_gi_commission_usd,
       sum(cu_gi_commission_ctac) as cu_gi_commission_ctac,
       sum(cu_gi_comm_tax_usd) as cu_gi_comm_tax_usd,
       sum(cu_gi_comm_tax_ctac) as cu_gi_comm_tax_ctac,
       max(cd_stay_source_table) as cd_stay_source_table,
       max(cd_phi_ctac_currency) as cd_phi_ctac_currency,
       sum(cu_phi_revenue_ctac) as cu_phi_revenue_ctac,
       sum(cu_phi_commission_ctac) as cu_phi_commission_ctac,
       max(cd_mini_hotel) as cd_mini_hotel,
       max(id_gi_year_acctg) as id_gi_year_acctg,
       max(id_gi_period_acctg) as id_gi_period_acctg,
       max(cd_gi_week_acctg) cd_gi_week_acctg,
       max(id_derived_account) as id_derived_account,
       max(nm_company_cleansed) as nm_company_cleansed
       from (
select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed from res.main.tmc_vs_extract_temp1
       union all
select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed from res.main.tmc_vs_extract_temp2
       union all
select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed from res.main.tmc_vs_extract_temp3)
       group by
     cd_property,
       id_year_acctg,
       id_period_acctg,
       id_seq_number,
       cd_booking_iata,
       cd_piata_current,
       cd_piata_history,
       cd_reason_code,
       cd_confo_num,
       dt_date_arrive,
       cd_ctac_market_segment,
       cd_mrdw_market_segment,
       cd_mrdw_rate_pgm,
       cd_mrdw_book_source,
       cd_company,
       nm_company,
       dt_date,
       qy_roomnights_orig,
       stay_nights,
       cd_1p_comp,
       cd_3p_comp,
       cd_ytp_comp,
       cd_fyp_comp,
       cd_1m_comp,
       cd_3m_comp,
       cd_ytm_comp,
       cd_fym_comp;       
	   
	   
	   CREATE or replace TABLE RES.MAIN.TMC_VS_EXTRACT_STAYNIGHT_REVENUE_AND_PAYMENT AS
SELECT CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	CD_WEEK_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	CD_PIATA_CURRENT,
	CD_PIATA_HISTORY,
    CD_CONFO_NUM,
	DT_DATE_ARRIVE,
	CD_CTAC_MARKET_SEGMENT,
	CD_MRDW_MARKET_SEGMENT,
	CD_MRDW_RATE_PGM,
	CD_MRDW_BOOK_SOURCE,
    NM_COMPANY,
	DT_DATE,
    QY_ROOMNIGHTS,
	STAY_NIGHTS,
    CD_1P_COMP,
	CD_3P_COMP,
	CD_YTP_COMP,
	CD_FYP_COMP,
	CD_1M_COMP,
	CD_3M_COMP,
	CD_YTM_COMP,
	CD_FYM_COMP,
    CD_MRDW_BOOK_SOURCE_OUT,
    CD_MRDW_MARKET_SEGMENT_OUT,
    CD_MARKET_PREFIX_VAR as CD_MARKET_PREFIX,
    case when CD_MARKET_PREFIX_VAR = '17' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '25' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '32' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '34' then CD_MARKET_SEGMENT_VAR
         else CD_MARKET_PREFIX_VAR end CD_MKT_PREFIX_LOOKUP,
    case when CD_MARKET_PREFIX_VAR = 'GR' then 'G'
         when CD_MARKET_PREFIX_VAR = 'XX' then 'X'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '5' then 'C'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '6' then 'M'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '0' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '1' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '2' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '3' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '4' then  'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '9' then 'T' end CD_MARKET_TYPE_DERIVED,
   CD_RATE_PGM_DERIVED,
 CD_REASON_TYPE,
 CU_REVENUE_USD_OUT,
 CU_COMMISSION_USD_OUT,
 CU_FEE_USD_OUT,
 CU_PREF_PYMT_USD_OUT,
CD_COMPANY,
 QY_COMM_RATE_PCT,
          CD_GROUP_IATA,
          ID_MINI_HOTEL_SEQ,
          ID_GROUP_CONNECT,
          CD_STAY_SOURCE_TABLE,
          CD_PHI_CTAC_CURRENCY,
          CD_MINI_HOTEL,
          ID_GI_YEAR_ACCTG ,
          ID_GI_PERIOD_ACCTG,
          CD_GI_WEEK_ACCTG,
          CU_GI_COMMISSION_USD,
CU_GI_COMMISSION_CTAC,
CU_GI_COMM_TAX_USD,
CU_GI_COMM_TAX_CTAC,
CU_PHI_REVENUE_CTAC,
CU_PHI_COMMISSION_CTAC,
          ID_DERIVED_ACCOUNT,
          NM_COMPANY_CLEANSED           
FROM
(SELECT CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	CD_WEEK_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	CD_PIATA_CURRENT,
	CD_PIATA_HISTORY,
    CD_CONFO_NUM,
	DT_DATE_ARRIVE,
	CD_CTAC_MARKET_SEGMENT,
	CD_MRDW_MARKET_SEGMENT,
	CD_MRDW_RATE_PGM,
	CD_MRDW_BOOK_SOURCE,
    NM_COMPANY,
	DT_DATE,
    QY_ROOMNIGHTS,
	STAY_NIGHTS,
    CD_1P_COMP,
	CD_3P_COMP,
	CD_YTP_COMP,
	CD_FYP_COMP,
	CD_1M_COMP,
	CD_3M_COMP,
	CD_YTM_COMP,
	CD_FYM_COMP, 
    case when CD_MRDW_MARKET_SEGMENT is not null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = TRUE then SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2) 
          when CD_MRDW_MARKET_SEGMENT is not null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = FALSE THEN 'GR'
          when CD_MRDW_MARKET_SEGMENT is null and try_to_numeric(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,2)) = TRUE THEN SUBSTR(CD_CTAC_MARKET_SEGMENT,1,2)
          when (ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 1,1)) >= 65 AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 1,1)) <= 90
          AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 2,1)) >= 65  AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 2,1)) <= 90)
              THEN 'GR' ELSE 'XX' END CD_MARKET_PREFIX_VAR,
         case when CD_MRDW_BOOK_SOURCE is null then 'Other'
         when CD_MRDW_BOOK_SOURCE = 'C'  then 'C0' 
         else CD_MRDW_BOOK_SOURCE end CD_MRDW_BOOK_SOURCE_OUT,
          case when CD_MRDW_MARKET_SEGMENT is null then RTRIM(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,6))
         else RTRIM(SUBSTR(CD_MRDW_MARKET_SEGMENT, 1, 6)) end CD_MARKET_SEGMENT_VAR,           
    case when CD_MRDW_MARKET_SEGMENT is null then RTRIM(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,6))
         else RTRIM(SUBSTR(CD_MRDW_MARKET_SEGMENT, 1, 6)) end CD_MRDW_MARKET_SEGMENT_OUT,
   case when CD_MRDW_RATE_PGM is null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = TRUE then substr(CD_CTAC_MARKET_SEGMENT,3,4)
        when CD_MRDW_RATE_PGM is null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = FALSE then substr(CD_CTAC_MARKET_SEGMENT,1,4)
        else CD_MRDW_RATE_PGM end CD_RATE_PGM_DERIVED,        
          case when CD_REASON_CODE = '    ' OR CD_REASON_CODE = 'PRPD' OR CD_REASON_CODE = 'CUTF' OR CD_REASON_CODE = 'PREC' then 'QUAL'
               when CD_REASON_CODE = 'CP17' OR CD_REASON_CODE = 'CP18' THEN 'NEGC'
               when CD_REASON_CODE = 'CNTA' OR CD_REASON_CODE = 'GRUP' OR CD_REASON_CODE = 'FAMT' OR CD_REASON_CODE = 'TRVL'
               then 'NONQ' else 'OTHR' END as CD_REASON_TYPE,
          case when CU_PH_REVENUE_USD  = 0  OR QY_ROOMNIGHTS_ORIG = 0 then 0 else (CU_PH_REVENUE_USD / ABS(QY_ROOMNIGHTS_ORIG)) end
           as CU_REVENUE_USD_OUT,
          case when CU_PH_COMMISSION_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_PH_COMMISSION_USD  /  ABS(QY_ROOMNIGHTS)) end 
          as CU_COMMISSION_USD_OUT,
          case when CU_BD_FEE_TAXES_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_BD_FEE_TAXES_USD / ABS(QY_ROOMNIGHTS)) end 
          as CU_FEE_USD_OUT,
          case when CU_PREF_PAY_TAXES_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_PREF_PAY_TAXES_USD / ABS(QY_ROOMNIGHTS)) end
          as CU_PREF_PYMT_USD_OUT,
          case when CD_COMPANY is null then '   ' else CD_COMPANY end CD_COMPANY,
          QY_COMM_RATE_PCT,
          CD_GROUP_IATA,
          ID_MINI_HOTEL_SEQ,
          ID_GROUP_CONNECT,
          CD_STAY_SOURCE_TABLE,
          CD_PHI_CTAC_CURRENCY,
          CD_MINI_HOTEL,
          ID_GI_YEAR_ACCTG ,
          ID_GI_PERIOD_ACCTG,
          CD_GI_WEEK_ACCTG,
          case when (CU_GI_COMMISSION_USD  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMMISSION_USD  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMMISSION_USD,
          case when (CU_GI_COMMISSION_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMMISSION_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMMISSION_CTAC,
          case when (CU_GI_COMM_TAX_USD  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMM_TAX_USD  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMM_TAX_USD,
          case when (CU_GI_COMM_TAX_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMM_TAX_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMM_TAX_CTAC,
          case when (CU_PHI_REVENUE_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_PHI_REVENUE_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_PHI_REVENUE_CTAC,
          case when (CU_PHI_COMMISSION_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0
               else (CU_PHI_COMMISSION_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_PHI_COMMISSION_CTAC,
          ID_DERIVED_ACCOUNT,
          NM_COMPANY_CLEANSED           
   FROM res.main.tmc_vs_extract_temp);
   
   
   CREATE TEMP TABLE RES.MAIN.TEMP_MAX_DT 
   AS select year_acctg, period_acctg_id, max(date_dt) date_dt
from res.main.mrdw_dim_date  group by year_acctg, period_acctg_id;

TRUNCATE TABLE res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL;
   
 insert into res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL (
	CD_PROPERTY,
	ID_PAY_YEAR_ACCTG,
	ID_PAY_QTR_ACCTG,
	ID_PAY_PERIOD_ACCTG,
	ID_PAY_YEAR_CAL,
	ID_PAY_QTR_CAL,
	ID_PAY_MONTH_CAL,
	ID_STAY_YEAR_ACCTG,
	ID_STAY_QTR_ACCTG,
	ID_STAY_PERIOD_ACCTG,
	ID_STAY_YEAR_CAL,
	ID_STAY_QTR_CAL,
	ID_STAY_MONTH_CAL,
	CD_BRAND,
	ID_REGION,
	CD_MGMT_TYPE,
	FL_COMPARABLE,
	CD_GIATA,
	ID_MINI_HOTEL_SEQ,
	DT_STAY,
	CD_CTAC_CURRENCY,
	QY_RMNTS_GRP,
	CU_REV_GRP_CTAC,
	CU_REV_GRP_USD,
	CU_COMM_GRP_CTAC,
	CU_COMM_GRP_USD,
	QY_RMNTS_TRN_NI,
	CU_REV_TRN_NI_CTAC,
	CU_REV_TRN_NI_USD,
	QY_RMNTS_TRN_INT,
	CU_REV_TRN_INT_CTAC,
	CU_REV_TRN_INT_USD,
	CU_COMM_TRN_INT_CTAC,
	CU_COMM_TRN_INT_USD,
	ID_DIVISION
)
select CD_PROPERTY,
          d1.ID_YEAR_ACCTG,
           d1.ID_QTR_ACCTG,
          d1.ID_PERIOD_ACCTG,
          d1.ID_YEAR_CAL,
          d1.ID_QTR_CAL,
          d1.ID_MONTH_CAL,
          d.YEAR_ACCTG,
           d.QTR_ACCTG_ID,
          d.PERIOD_ACCTG_ID,
          d.YEAR_CAL,
          d.QTR_CAL_ID,
          d.MONTH_CAL_ID,
          p.BRAND_CD,
		  p.REGION_ID,
          p.MGMT_TYPE_CD,
          p.COMPARABLE_IND,
          E.CD_GROUP_IATA,
          E.ID_MINI_HOTEL_SEQ,
          E.DT_DATE,
          E.CD_PHI_CTAC_CURRENCY,
          SUM(E.STAY_NIGHTS),
          SUM(CU_PHI_REVENUE_CTAC),
          SUM(CU_REVENUE_USD_OUT),
          SUM(CU_GI_COMMISSION_CTAC),
          SUM(CU_GI_COMMISSION_USD),
          SUM(STAY_NIGHTS),
          SUM(CU_PHI_REVENUE_CTAC),
          SUM(CU_REVENUE_USD_OUT),
  SUM(STAY_NIGHTS),
  SUM(CU_PHI_REVENUE_CTAC),
  SUM(CU_REVENUE_USD_OUT),
  SUM(CU_PHI_COMMISSION_CTAC),
  SUM(CU_COMMISSION_USD_OUT),
          p.DIVISION_ID
from RES.MAIN.TMC_VS_EXTRACT_STAYNIGHT_REVENUE_AND_PAYMENT E,
   (SELECT RDWD_DDM_DATE.YEAR_CAL as ID_YEAR_CAL
, RDWD_DDM_DATE.QTR_CAL_ID as ID_QTR_CAL
, RDWD_DDM_DATE.MONTH_CAL_ID as ID_MONTH_CAL
, RDWD_DDM_DATE.QTR_ACCTG_ID as ID_QTR_ACCTG
, RDWD_DDM_DATE.YEAR_ACCTG as ID_YEAR_ACCTG
, RDWD_DDM_DATE.PERIOD_ACCTG_ID as ID_PERIOD_ACCTG,
    RDWD_DDM_DATE.date_dt
FROM res.main.mrdw_dim_date RDWD_DDM_DATE
where id_year_acctg IN (select year_acctg from RES.MAIN.TEMP_MAX_DT) 
and id_period_acctg IN (select period_acctg_id from RES.MAIN.TEMP_MAX_DT)
and date_dt IN (select date_dt from RES.MAIN.TEMP_MAX_DT)) d1,
  res.main.mrdw_dim_date d,
  pty.main.mrdw_dim_property p
where d.year_acctg = d1.id_year_acctg
  and d.period_acctg_id = d1.id_period_acctg
  and d.date_dt = d1.date_dt
  and E.id_gi_year_acctg = d.year_acctg
and  E.id_gi_period_acctg = d.period_acctg_id
and E.cd_property = p.property_cd  
and E.cd_group_iata is not null
and trim(E.cd_group_iata) not in ('')
group by CD_PROPERTY,
          d1.ID_YEAR_ACCTG,
           d1.ID_QTR_ACCTG,
          d1.ID_PERIOD_ACCTG,
          d1.ID_YEAR_CAL,
          d1.ID_QTR_CAL,
          d1.ID_MONTH_CAL,
          d.YEAR_ACCTG,
           d.QTR_ACCTG_ID,
          d.PERIOD_ACCTG_ID,
          d.YEAR_CAL,
          d.QTR_CAL_ID,
          d.MONTH_CAL_ID,
          p.BRAND_CD,
          p.MGMT_TYPE_CD,
          p.COMPARABLE_IND,
          p.REGION_ID,
          E.CD_GROUP_IATA,
          E.ID_MINI_HOTEL_SEQ,
          E.DT_DATE,
          E.CD_PHI_CTAC_CURRENCY,
          P.DIVISION_ID;  
		  
		  CREATE OR REPLACE TABLE res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL_CLONE clone res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL;
		  
ALTER TABLE res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL_CLONE swap with res.main.RES_FACT_GI_PAY_STAY_DETAIL_REPL;

create or replace view RES.PROXY.res.main.RES_FACT_GI_PAY_STAY_DETAIL as select * FROM res.main.RES_FACT_GI_PAY_STAY_DETAIL;
	   
--------------------------------------cmd_extract----------------------------------------------------------------------

create or replace table res.main.t_cmd_extract_temp1 as
select 
       cs.cd_property,
       cs.id_year_acctg,
       cs.id_period_acctg,
       cs.cd_week_acctg,
       cs.id_seq_number,
       cs.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       cs.cd_reason_code,
       cs.cd_confo_num,
       cs.dt_date_arrive,
       cs.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      cs.qy_roomnights_orig,
       decode (cs.qy_roomnights_orig, 0, 1, cs.qy_roomnights_orig) as qy_roomnights,
       decode (cs.qy_roomnights_orig, 0, 0, (cs.qy_roomnights/cs.qy_roomnights_orig)) as stay_nights,
       cs.cu_ph_revenue_usd,
      (cu_ph_commission_usd + cu_th_comm_tax_usd) as cu_ph_commission_usd,
       0 as cu_bd_fee_taxes_usd,
       0 as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp) as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       qy_comm_rate_pct,
       cd_group_iata,
       id_mini_hotel_seq,
       id_group_connect,
       cu_gi_commission_usd,
       cu_gi_commission_ctac,
       cu_gi_comm_tax_usd,
       cu_gi_comm_tax_ctac,
       cd_stay_source_table,
       cd_phi_ctac_currency,
       cu_phi_revenue_ctac,
       cu_phi_commission_ctac,
       cd_mini_hotel,
       id_gi_year_acctg,
       id_gi_period_acctg,
       cd_gi_week_acctg,
       id_derived_account,
       nm_company_cleansed ,
       0 cu_pref_pay_taxes_ctac,
       0 pp_flag
 from res.main.RES_FACT_COMMISSION_STAYS cs,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO mc,
     res.main.mrdw_dim_date dt,
     pty.main.mrdw_dim_property pr
where pr.brand_cd != 'NR'
--and pr.cd_brand in ('OX','PR','TS')
--and  bp.cd_piata_current = '68369900'
and cs.id_property = pr.property_id
AND NOT EXISTS
(SELECT 'x' FROM res.main.RES_DIM_TMC_STAYS TS 
WHERE TS.ID_PROPERTY = CS.ID_PROPERTY AND TS.ID_SEQ_NUMBER = CS.ID_SEQ_NUMBER 
AND (CS.CD_STAY_SOURCE_TABLE = 'P' OR CS.CD_STAY_SOURCE_TABLE = 'R' OR CS.CD_STAY_SOURCE_TABLE IS NULL)) 
--AND cs.id_year_acctg =2015
AND (((cs.id_year_acctg*100)+cs.id_period_acctg IN (202209,202210,202211)) or ((cs.id_gi_year_acctg*100)+id_gi_period_acctg IN (202209,202210,202211)))   
and dt.date_dt between cs.dt_date_arrive and (cs.dt_date_arrive + (decode(cs.qy_roomnights_orig, 0, 0, cs.qy_roomnights_orig - 1))) 
and bp.cd_booking_iata(+) = cs.cd_booking_iata 
and bp.dt_date_arrive(+) = cs.dt_date_arrive 
and (cs.qy_roomnights + cs.cu_ph_revenue_usd + cs.cu_gi_commission_ctac) != 0
and mc.cd_property (+) = cs.cd_property
and mc.cd_confo_num_cur (+) = cs.cd_confo_num and mc.dt_date_arrive (+) = cs.dt_date_arrive; 


create or replace table res.main.t_cmd_extract_temp2 as
select 
       cs.cd_property,
       cs.id_year_acctg,
      cs.id_period_acctg,
      cs.cd_week_acctg,
       cs.id_seq_number,
        cs.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       cs.cd_reason_code,
       cs.cd_confo_num,
       cs.dt_date_arrive,
       cs.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      cs.qy_roomnights_orig,
       0 as qy_roomnights,
       decode (cs.qy_roomnights, 0, 0, 1) as stay_nights,
       0 as cu_ph_revenue_usd,
      (cu_ph_commission_usd + cu_th_comm_tax_usd) as cu_ph_commission_usd,
       0 as cu_bd_fee_taxes_usd,
       0 as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp) as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       qy_comm_rate_pct,
       cd_group_iata,
       id_mini_hotel_seq,
       id_group_connect,
       cu_gi_commission_usd,
       cu_gi_commission_ctac,
       cu_gi_comm_tax_usd,
       cu_gi_comm_tax_ctac,
       cd_stay_source_table,
       cd_phi_ctac_currency,
       0 as cu_phi_revenue_ctac,
       cu_phi_commission_ctac,
       cd_mini_hotel,
       id_gi_year_acctg,
       id_gi_period_acctg,
       cd_gi_week_acctg,
       mc.id_derived_account,
       mc.nm_company_cleansed ,
       0 cu_pref_pay_taxes_ctac,
       0 pp_flag
from res.main.RES_FACT_COMMISSION_STAYS cs,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO mc,
     res.main.mrdw_dim_date dt,
     pty.main.mrdw_dim_property pr
where pr.brand_cd != 'NR'
--and pr.cd_brand in ('OX','PR','TS')
--and  bp.cd_piata_current = '68369900'
and cs.id_property=pr.property_id
and EXISTS
(SELECT 'x' FROM res.main.RES_DIM_TMC_STAYS TS  
WHERE TS.ID_PROPERTY = CS.ID_PROPERTY AND TS.ID_SEQ_NUMBER = CS.ID_SEQ_NUMBER 
AND (CS.CD_STAY_SOURCE_TABLE = 'P' OR CS.CD_STAY_SOURCE_TABLE = 'R' OR CS.CD_STAY_SOURCE_TABLE IS NULL)) 
--AND cs.id_year_acctg =2015
and (((cs.id_year_acctg*100)+cs.id_period_acctg IN (202209,202210,202211)) or ((cs.id_gi_year_acctg*100)+id_gi_period_acctg IN (202209,202210,202211)))
and dt.date_dt between cs.dt_date_arrive and (cs.dt_date_arrive + (decode(cs.qy_roomnights_orig, 0, 0, cs.qy_roomnights_orig - 1))) 
and bp.cd_booking_iata(+) = cs.cd_booking_iata 
and bp.dt_date_arrive(+) = cs.dt_date_arrive 
and mc.cd_property (+) = cs.cd_property 
and mc.cd_confo_num_cur (+) = cs.cd_confo_num 
and mc.dt_date_arrive (+) = cs.dt_date_arrive;	   


create or replace table res.main.t_cmd_extract_temp3 as
select  
       ts.cd_property,
       ts.id_year_acctg,
       ts.id_period_acctg,
       ts.cd_week_acctg,
       ts.id_seq_number,
       ts.cd_booking_iata,
       bp.cd_piata_current,
       bp.cd_piata_history,
       ts.cd_tm6_reason_code cd_reason_code,
       ts.cd_confo_num,
       ts.dt_date_arrive,
       ts.cd_market_segment as cd_ctac_market_segment,
       mc.cd_market as cd_mrdw_market_segment,
       mc.cd_rate_pgm as cd_mrdw_rate_pgm,
       mc.cd_book_source as cd_mrdw_book_source,
       mc.cd_company,
       mc.nm_company,
       dt.date_dt,
      ts.qy_roomnights as qy_roomnights_orig,
       decode(ts.qy_roomnights, 0, 1, ts.qy_roomnights) as qy_roomnights,
       decode(ts.qy_roomnights, 0, 0, 1) as stay_nights,
       ts.cu_tm6_revenue_usd as cu_ph_revenue_usd,
       0 as cu_ph_commission_usd,
      (cu_tm9_fee_amt_usd + cu_tm9_city_tax_usd + cu_tm9_cntry_tax_usd + cu_tm9_prvn_tax_usd)  as cu_bd_fee_taxes_usd,
      (cu_tm6_pref_amt_usd -cu_tm9_fee_amt_usd) as cu_pref_pay_taxes_usd,
       decode(bp.cd_1p_comp, null, 'N', bp.cd_1p_comp) as cd_1p_comp,
       decode(bp.cd_3p_comp, null, 'N', bp.cd_3p_comp) as cd_3p_comp,
       decode(bp.cd_ytp_comp, null, 'N', bp.cd_ytp_comp) as cd_ytp_comp,
       decode(bp.cd_fyp_comp, null, 'N', bp.cd_fyp_comp) as cd_fyp_comp,
       decode(bp.cd_1m_comp, null, 'N', bp.cd_1m_comp)  as cd_1m_comp,
       decode(bp.cd_3m_comp, null, 'N', bp.cd_3m_comp) as cd_3m_comp,
       decode(bp.cd_ytm_comp, null, 'N', bp.cd_ytm_comp) as cd_ytm_comp,
       decode(bp.cd_fym_comp, null, 'N', bp.cd_fym_comp) as cd_fym_comp,
       0 qy_comm_rate_pct,
       NULL cd_group_iata,
       0 id_mini_hotel_seq,
       0 id_group_connect,
       0 cu_gi_commission_usd,
       0 cu_gi_commission_ctac,
       0 cu_gi_comm_tax_usd,
       0 cu_gi_comm_tax_ctac,
       NULL cd_stay_source_table,
       ts.cd_ctac_currency as cd_phi_ctac_currency,
       cu_tm6_revenue_ctac as cu_phi_revenue_ctac,
       0 cu_phi_commission_ctac,       
       NULL cd_mini_hotel,
       0 id_gi_year_acctg,
       0 id_gi_period_acctg,
       NULL cd_gi_week_acctg,
       mc.id_derived_account,
       mc.nm_company_cleansed ,
     (CU_TM6_PREF_AMT_CTAC-cu_tm9_fee_amt_ctac) as cu_pref_pay_taxes_ctac,
1 pp_flag 
from  res.main.RES_DIM_TMC_STAYS TS,
     res.main.RES_FACT_BIATA_PIATAS bp,
     res.main.RES_FACT_MRDW_CONFO mc,
     res.main.mrdw_dim_date dt, 
     pty.main.mrdw_dim_property pr 
  where pr.brand_cd != 'NR'
--and pr.cd_brand in ('OX','PR','TS')
--and  bp.cd_piata_current= '68369900'
and ts.id_property = pr.property_id
--and ts.id_year_acctg =2015
and ((ts.id_year_acctg*100)+ts.id_period_acctg IN (202209,202210,202211))
and bp.cd_booking_iata = ts.cd_booking_iata 
and bp.dt_date_arrive = ts.dt_date_arrive 
and (ts.qy_roomnights != 0 OR ts.cu_tm6_revenue_usd != 0
     OR (cu_tm9_fee_amt_usd + cu_tm9_city_tax_usd + cu_tm9_cntry_tax_usd + cu_tm9_prvn_tax_usd) != 0
     OR (cu_tm6_pref_amt_usd + cu_tm6_city_tax_usd + cu_tm6_cntry_tax_usd + cu_tm6_prvn_tax_usd) != 0
     OR (CU_TM6_PREF_AMT_CTAC + CU_TM6_CITY_TAX_CTAC + CU_TM6_CNTRY_TAX_CTAC + CU_TM6_PRVN_TAX_CTAC) != 0
     ) 
and mc.cd_property(+) = ts.cd_property 
and mc.cd_confo_num_cur(+) = ts.cd_confo_num 
and mc.dt_date_arrive(+) = ts.dt_date_arrive 
and dt.date_dt between ts.dt_date_arrive and (ts.dt_date_arrive + decode(ts.qy_roomnights, 0, 0, (abs(ts.qy_roomnights) - 1)));



create or replace table res.main.t_cmd_extract_temp as
select 
       cd_property,
       id_year_acctg,
       id_period_acctg,
       min(cd_week_acctg) as cd_week_acctg,
       id_seq_number,
       cd_booking_iata,
       cd_piata_current,
       cd_piata_history,
       cd_reason_code,
       cd_confo_num,
       dt_date_arrive,
       cd_ctac_market_segment,
       cd_mrdw_market_segment,
       cd_mrdw_rate_pgm,
       cd_mrdw_book_source,
       cd_company,
       nm_company,
       date_dt,
       qy_roomnights_orig,
       sum(qy_roomnights) as qy_roomnights,
       stay_nights,
       sum(cu_ph_revenue_usd) as cu_ph_revenue_usd,
       sum(cu_ph_commission_usd) as cu_ph_commission_usd,
       sum(cu_bd_fee_taxes_usd) as cu_bd_fee_taxes_usd,
       sum(cu_pref_pay_taxes_usd) as cu_pref_pay_taxes_usd,
       cd_1p_comp,
       cd_3p_comp,
       cd_ytp_comp,
       cd_fyp_comp,
       cd_1m_comp,
       cd_3m_comp,
       cd_ytm_comp,
       cd_fym_comp,
       max(qy_comm_rate_pct) as qy_comm_rate_pct,
       max(cd_group_iata) as cd_group_iata,
       max(id_mini_hotel_seq) as id_mini_hotel_seq,
       max(id_group_connect) as id_group_connect,
       sum(cu_gi_commission_usd) as cu_gi_commission_usd,
       sum(cu_gi_commission_ctac) as cu_gi_commission_ctac,
       sum(cu_gi_comm_tax_usd) as cu_gi_comm_tax_usd,
       sum(cu_gi_comm_tax_ctac) as cu_gi_comm_tax_ctac,
       max(cd_stay_source_table) as cd_stay_source_table,
       max(cd_phi_ctac_currency) as cd_phi_ctac_currency,
       sum(cu_phi_revenue_ctac) as cu_phi_revenue_ctac,
       sum(cu_phi_commission_ctac) as cu_phi_commission_ctac,
       max(cd_mini_hotel) as cd_mini_hotel,
       max(id_gi_year_acctg) as id_gi_year_acctg,
       max(id_gi_period_acctg) as id_gi_period_acctg,
       max(cd_gi_week_acctg) as cd_gi_week_acctg ,
       max(id_derived_account) as id_derived_account,
       max(nm_company_cleansed) as nm_company_cleansed,
       sum(cu_pref_pay_taxes_ctac) as cu_pref_pay_taxes_ctac,
       max(pp_flag) as pp_flag
       from(
       select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed,cu_pref_pay_taxes_ctac,pp_flag from res.main.t_cmd_extract_temp1
       union all
select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed,cu_pref_pay_taxes_ctac,pp_flag from res.main.t_cmd_extract_temp2
       union all
select cd_property,id_year_acctg,id_period_acctg,cd_week_acctg,id_seq_number,cd_booking_iata,cd_piata_current,cd_piata_history,cd_reason_code,
       cd_confo_num,dt_date_arrive,cd_ctac_market_segment,cd_mrdw_market_segment,cd_mrdw_rate_pgm,cd_mrdw_book_source,
       cd_company,nm_company,date_dt,qy_roomnights_orig,qy_roomnights,stay_nights,cu_ph_revenue_usd,cu_ph_commission_usd,
       cu_bd_fee_taxes_usd,cu_pref_pay_taxes_usd,cd_1p_comp,cd_3p_comp,cd_ytp_comp,cd_fyp_comp,
       cd_1m_comp,cd_3m_comp,cd_ytm_comp,cd_fym_comp,qy_comm_rate_pct,cd_group_iata,id_mini_hotel_seq,id_group_connect,cu_gi_commission_usd,cu_gi_commission_ctac,cu_gi_comm_tax_usd,
cu_gi_comm_tax_ctac,cd_stay_source_table,cd_phi_ctac_currency,cu_phi_revenue_ctac,cu_phi_commission_ctac,cd_mini_hotel,id_gi_year_acctg,
id_gi_period_acctg,cd_gi_week_acctg,id_derived_account,nm_company_cleansed,cu_pref_pay_taxes_ctac,pp_flag from res.main.t_cmd_extract_temp3)
       group by
     cd_property,
       id_year_acctg,
       id_period_acctg,
       id_seq_number,
       cd_booking_iata,
       cd_piata_current,
       cd_piata_history,
       cd_reason_code,
       cd_confo_num,
       dt_date_arrive,
       cd_ctac_market_segment,
       cd_mrdw_market_segment,
       cd_mrdw_rate_pgm,
       cd_mrdw_book_source,
       cd_company,
       nm_company,
       date_dt,
       qy_roomnights_orig,
       stay_nights,
       cd_1p_comp,
       cd_3p_comp,
       cd_ytp_comp,
       cd_fyp_comp,
       cd_1m_comp,
       cd_3m_comp,
       cd_ytm_comp,
       cd_fym_comp; 
	   
	   
CREATE or replace TABLE RES.MAIN.TMC_CMD_EXTRACT_STAYNIGHT_REVENUE_AND_PAYMENT AS
SELECT CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	CD_WEEK_ACCTG,
	ID_SEQ_NUMBER,
	CD_BOOKING_IATA,
	CD_PIATA_CURRENT,
	CD_PIATA_HISTORY,
    CD_CONFO_NUM,
	DT_DATE_ARRIVE,
	CD_CTAC_MARKET_SEGMENT,
	CD_MRDW_MARKET_SEGMENT,
	CD_MRDW_RATE_PGM,
	CD_MRDW_BOOK_SOURCE,
    NM_COMPANY,
	DT_DATE,
    QY_ROOMNIGHTS,
	STAY_NIGHTS,
    CD_1P_COMP,
	CD_3P_COMP,
	CD_YTP_COMP,
	CD_FYP_COMP,
	CD_1M_COMP,
	CD_3M_COMP,
	CD_YTM_COMP,
	CD_FYM_COMP,
    CD_MRDW_BOOK_SOURCE_OUT,
    CD_MRDW_MARKET_SEGMENT_OUT,
    CD_MARKET_PREFIX_VAR as CD_MARKET_PREFIX,
    case when CD_MARKET_PREFIX_VAR = '17' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '25' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '32' then CD_MARKET_SEGMENT_VAR
         when CD_MARKET_PREFIX_VAR = '34' then CD_MARKET_SEGMENT_VAR
         else CD_MARKET_PREFIX_VAR end CD_MKT_PREFIX_LOOKUP,
    case when CD_MARKET_PREFIX_VAR = 'GR' then 'G'
         when CD_MARKET_PREFIX_VAR = 'XX' then 'X'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '5' then 'C'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '6' then 'M'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '0' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '1' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '2' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '3' then 'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '4' then  'T'
       when SUBSTR(CD_MARKET_PREFIX_VAR, 1, 1) = '9' then 'T' end CD_MARKET_TYPE_DERIVED,
   CD_RATE_PGM_DERIVED,
 CD_REASON_TYPE,
 CU_REVENUE_USD_OUT,
 CU_COMMISSION_USD_OUT,
 CU_FEE_USD_OUT,
 CU_PREF_PYMT_USD_OUT,
CD_COMPANY,
 QY_COMM_RATE_PCT,
          CD_GROUP_IATA,
          ID_MINI_HOTEL_SEQ,
          ID_GROUP_CONNECT,
          CD_STAY_SOURCE_TABLE,
          CD_PHI_CTAC_CURRENCY,
          CD_MINI_HOTEL,
          ID_GI_YEAR_ACCTG ,
          ID_GI_PERIOD_ACCTG,
          CD_GI_WEEK_ACCTG,
          CU_GI_COMMISSION_USD,
CU_GI_COMMISSION_CTAC,
CU_GI_COMM_TAX_USD,
CU_GI_COMM_TAX_CTAC,
CU_PHI_REVENUE_CTAC,
CU_PHI_COMMISSION_CTAC,
          ID_DERIVED_ACCOUNT,
          NM_COMPANY_CLEANSED,
          CU_PREF_PAY_TAXES_CTAC,
          PP_FLAG
FROM
(select cd_property,
       id_year_acctg,
       id_period_acctg,
       cd_week_acctg,
       id_seq_number,
       cd_booking_iata,
       cd_piata_current,
       cd_piata_history,
       cd_confo_num,
       dt_date_arrive,
       cd_ctac_market_segment,
       cd_mrdw_market_segment,
       cd_mrdw_rate_pgm,
       cd_mrdw_book_source,
       nm_company,
       date_dt as dt_date,
       qy_roomnights,
       stay_nights,
       case when STAY_NIGHTS  = 0 OR QY_ROOMNIGHTS = 0 then 0 else STAY_NIGHTS end  stay_nights_out,      
       cd_1p_comp,
       cd_3p_comp,
       cd_ytp_comp,
       cd_fyp_comp,
       cd_1m_comp,
       cd_3m_comp,
       cd_ytm_comp,
       cd_fym_comp,
        case when CD_MRDW_MARKET_SEGMENT is not null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = TRUE then SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2) 
          when CD_MRDW_MARKET_SEGMENT is not null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = FALSE THEN 'GR'
          when CD_MRDW_MARKET_SEGMENT is null and try_to_numeric(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,2)) = TRUE THEN SUBSTR(CD_CTAC_MARKET_SEGMENT,1,2)
          when (ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 1,1)) >= 65 AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 1,1)) <= 90
          AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 2,1)) >= 65  AND ASCII(SUBSTR(CD_CTAC_MARKET_SEGMENT, 2,1)) <= 90)
              THEN 'GR' ELSE 'XX' END CD_MARKET_PREFIX_VAR,
         case when CD_MRDW_BOOK_SOURCE is null then 'Other'
         when CD_MRDW_BOOK_SOURCE = 'C'  then 'C0' 
         else CD_MRDW_BOOK_SOURCE end CD_MRDW_BOOK_SOURCE_OUT,
          case when CD_MRDW_MARKET_SEGMENT is null then RTRIM(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,6))
         else RTRIM(SUBSTR(CD_MRDW_MARKET_SEGMENT, 1, 6)) end CD_MARKET_SEGMENT_VAR,           
    case when CD_MRDW_MARKET_SEGMENT is null then RTRIM(SUBSTR(CD_CTAC_MARKET_SEGMENT,1,6))
         else RTRIM(SUBSTR(CD_MRDW_MARKET_SEGMENT, 1, 6)) end CD_MRDW_MARKET_SEGMENT_OUT,
   case when CD_MRDW_RATE_PGM is null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = TRUE then substr(CD_CTAC_MARKET_SEGMENT,3,4)
        when CD_MRDW_RATE_PGM is null and try_to_numeric(SUBSTR(CD_MRDW_MARKET_SEGMENT,1,2)) = FALSE then substr(CD_CTAC_MARKET_SEGMENT,1,4)
        else CD_MRDW_RATE_PGM end CD_RATE_PGM_DERIVED,        
          case when CD_REASON_CODE = '    ' OR CD_REASON_CODE = 'PRPD' OR CD_REASON_CODE = 'CUTF' OR CD_REASON_CODE = 'PREC' then 'QUAL'
               when CD_REASON_CODE = 'CP17' OR CD_REASON_CODE = 'CP18' THEN 'NEGC'
               when CD_REASON_CODE = 'CNTA' OR CD_REASON_CODE = 'GRUP' OR CD_REASON_CODE = 'FAMT' OR CD_REASON_CODE = 'TRVL'
               then 'NONQ' else 'OTHR' END as CD_REASON_TYPE,
          case when CU_PH_REVENUE_USD  = 0  OR QY_ROOMNIGHTS_ORIG = 0 then 0 else (CU_PH_REVENUE_USD / ABS(QY_ROOMNIGHTS_ORIG)) end
           as CU_REVENUE_USD_OUT,
          case when CU_PH_COMMISSION_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_PH_COMMISSION_USD  /  ABS(QY_ROOMNIGHTS)) end 
          as CU_COMMISSION_USD_OUT,
          case when CU_BD_FEE_TAXES_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_BD_FEE_TAXES_USD / ABS(QY_ROOMNIGHTS)) end 
          as CU_FEE_USD_OUT,
          case when CU_PREF_PAY_TAXES_USD  = 0 OR QY_ROOMNIGHTS = 0 then 0 else (CU_PREF_PAY_TAXES_USD / ABS(QY_ROOMNIGHTS)) end
          as CU_PREF_PYMT_USD_OUT,
          case when CD_COMPANY is null then '   ' else CD_COMPANY end CD_COMPANY,
          QY_COMM_RATE_PCT,
          CD_GROUP_IATA,
          ID_MINI_HOTEL_SEQ,
          ID_GROUP_CONNECT,
          CD_STAY_SOURCE_TABLE,
          CD_PHI_CTAC_CURRENCY,
          CD_MINI_HOTEL,
          ID_GI_YEAR_ACCTG ,
          ID_GI_PERIOD_ACCTG,
          CD_GI_WEEK_ACCTG,
          case when (CU_GI_COMMISSION_USD  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMMISSION_USD  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMMISSION_USD,
          case when (CU_GI_COMMISSION_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMMISSION_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMMISSION_CTAC,
          case when (CU_GI_COMM_TAX_USD  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMM_TAX_USD  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMM_TAX_USD,
          case when (CU_GI_COMM_TAX_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_GI_COMM_TAX_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_GI_COMM_TAX_CTAC,
          case when (CU_PHI_REVENUE_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0 
               else (CU_PHI_REVENUE_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_PHI_REVENUE_CTAC,
          case when (CU_PHI_COMMISSION_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then 0
               else (CU_PHI_COMMISSION_CTAC  /  ABS(QY_ROOMNIGHTS)) end CU_PHI_COMMISSION_CTAC,
          ID_DERIVED_ACCOUNT,
          NM_COMPANY_CLEANSED,
          case when (CU_PREF_PAY_TAXES_CTAC  = 0 OR QY_ROOMNIGHTS = 0) then  0 
              else (CU_PREF_PAY_TAXES_CTAC / ABS(QY_ROOMNIGHTS)) end as CU_PREF_PAY_TAXES_CTAC,
          PP_FLAG
          FROM res.main.t_cmd_extract_temp);
		  
		  
		  
           INSERT INTO RES.MAIN.RES_FACT_CMD_CTAC_RSRVN (
	CD_PROPERTY,
	ID_YEAR_ACCTG,
	ID_PERIOD_ACCTG,
	CD_WEEK_ACCTG,
	ID_SEQ_NUMBERIC,
	CD_BIATA,
	CD_PIATA_CURRENT,
	CD_PIATA_HISTORY,
	CD_CONFO_NUM,
	DT_DATE_ARRIVE,
	CD_BOOK_SOURCE,
	CD_MARKET_PREFIX,
	CD_MARKET_ROLLUP,
	CD_MARKET_TYPE,
	CD_RATE_PGM_DERIVED,
	CD_TMC_REASON_TYPE,
	DT_DATE_STAY,
	QY_ROOMNIGHTS,
	CU_REVENUE_USD,
	CU_COMMISSION_USD,
	CU_FEE_USD,
	CU_PREF_PYMNT_USD,
	CD_1P_COMP_FLAG,
	CD_3P_COMP_FLAG,
	CD_YTP_COMP_FLAG,
	CD_FYP_COMP_FLAG,
	CD_1M_COMP_FLAG,
	CD_3M_COMP_FLAG,
	CD_YTM_COMP_FLAG,
	CD_FYM_COMP_FLAG,
	CD_COMPANY,
	NM_COMPANY,
	QY_COMM_RATE_PCT,
	CD_GIATA,
	CD_1P_GI_COMP_FLAG,
	CD_3P_GI_COMP_FLAG,
	CD_YTP_GI_COMP_FLAG,
	CD_FYP_GI_COMP_FLAG,
	CD_1M_GI_COMP_FLAG,
	CD_3M_GI_COMP_FLAG,
	CD_YTM_GI_COMP_FLAG,
	CD_FYM_GI_COMP_FLAG,
	CU_GI_COMMISSION_USD,
	ID_MINI_HOTEL_SEQ,
	ID_GI_YEAR_ACCTG,
	ID_GI_PERIOD_ACCTG,
	CD_GI_WEEK_ACCTG,
	NM_ACCOUNT,
	ID_ACCOUNT,
	CD_CTAC_CURRENCY,
	CU_REVENUE_CTAC,
	CU_COMMISSION_CTAC,
	CU_PREF_PYMNT_CTAC,
	DW_LOAD_TS
)
select E.CD_PROPERTY,
	E.ID_YEAR_ACCTG,
	E.ID_PERIOD_ACCTG,
	E.CD_WEEK_ACCTG,
	E.ID_SEQ_NUMBER,
	E.CD_BOOKING_IATA,
	E.CD_PIATA_CURRENT,
	E.CD_PIATA_HISTORY,
        E.CD_CONFO_NUM,
	E.DT_DATE_ARRIVE,
        E.CD_MRDW_BOOK_SOURCE_OUT,
	E.CD_MARKET_PREFIX,
        NULL,
        E.CD_MARKET_TYPE_DERIVED, 
        E.CD_RATE_PGM_DERIVED,
	E.CD_REASON_TYPE,
        E.DT_DATE,
        E.QY_ROOMNIGHTS,
        E.CU_REVENUE_USD_OUT, 
        E.CU_COMMISSION_USD_OUT,
	E.CU_FEE_USD_OUT,
	E.CU_PREF_PYMT_USD_OUT, 
    E.CD_1P_COMP,
	E.CD_3P_COMP,
	E.CD_YTP_COMP,
	E.CD_FYP_COMP,
	E.CD_1M_COMP,
	E.CD_3M_COMP,
	E.CD_YTM_COMP,
	E.CD_FYM_COMP,
    E.CD_COMPANY,
	E.NM_COMPANY,
	E.QY_COMM_RATE_PCT,
	E.CD_GROUP_IATA,
        C.FL_GI_COM_1P,
        C.FL_GI_COM_3P,
        C.FL_GI_COM_YTP,
        C.FL_GI_COM_FYP,
        C.FL_GI_COM_1M,
        C.FL_GI_COM_3M,
        C.FL_GI_COM_YTM,
        C.FL_GI_COM_FYM,
        E.CU_GI_COMMISSION_USD,
        E.ID_MINI_HOTEL_SEQ,
	E.ID_GI_YEAR_ACCTG,
	E.ID_GI_PERIOD_ACCTG,
	E.CD_GI_WEEK_ACCTG,
	CASE WHEN A.ID_DERIVED_ACCOUNT IS NULL AND NM_COMPANY_CLEANSED IS NULL THEN NULL
         WHEN A.ID_DERIVED_ACCOUNT IS NULL AND NM_COMPANY_CLEANSED IS NOT NULL THEN NM_COMPANY_CLEANSED
         ELSE RTRIM(NM_HIGHEST_LEVEL_ACCOUNT) || ' | ' || 
             TO_CHAR(ID_HIGHEST_LEVEL_BUSINESS) END AS NM_ACCOUNT,
	A.ID_DERIVED_ACCOUNT,
	E.CD_PHI_CTAC_CURRENCY,
	E.CU_PHI_REVENUE_CTAC,
	E.CU_PHI_COMMISSION_CTAC,
	E.CU_PREF_PAY_TAXES_CTAC,
    CURRENT_DATE() 
from RES.MAIN.TMC_CMD_EXTRACT_STAYNIGHT_REVENUE_AND_PAYMENT E,
   RES.MAIN.RES_DIM_GI_COMPS C,
   RES.MAIN.RES_DIM_HIGHEST_LEVEL_ACCOUNT A
where E.CD_BOOKING_IATA = C.CD_BIATA
AND E.ID_DERIVED_ACCOUNT = A.ID_DERIVED_ACCOUNT 
AND E.ID_YEAR_ACCTG  >= 2009;

