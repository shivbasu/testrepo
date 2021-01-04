# Databricks notebook source
# MAGIC %md
# MAGIC # Mapping Queries for Consumption EOM Table for December 2020 Release
# MAGIC 1. This table will be used for the TREND dashboard 

# COMMAND ----------

# DBTITLE 1,ANN_POL_SUMM_EOM-1.a: All Contract with Annuity Policy and Billing and Employer details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ANNT_CTRT_DETAILS_TEMP
# MAGIC AS 
# MAGIC  (Select c.CONTRACT_ID ,c.COMPANY_CODE ,c.POLICY_NUMBER
# MAGIC  ,ap.POLICY_CONTRACT_STATUS
# MAGIC  ,ap.PREMIUM_TYPE
# MAGIC  ,Case when ap.POLICY_CONTRACT_STATUS <> 'A' Then 'Policy not Active'
# MAGIC     when ap.POLICY_CONTRACT_STATUS = 'A' and ap.PREMIUM_TYPE = 'S' Then 'Single Premium'
# MAGIC     when ap.POLICY_CONTRACT_STATUS = 'A' and ap.PREMIUM_TYPE = 'F' and cpbd.BILLING_METHOD <> 'N' Then 'Active Flow'
# MAGIC     when ap.POLICY_CONTRACT_STATUS = 'A' and ap.PREMIUM_TYPE = 'F' and cpbd.BILLING_METHOD = 'N' Then 'No Flow'
# MAGIC     Else 'Check' End AS FLOW_STATUS
# MAGIC  ,cpbd.ANNUAL_PAYMENT_AMOUNT
# MAGIC  ,cpbd.BILL_EMPLOYER_ID
# MAGIC  ,cpbd.BILL_EMPLOYER_ID
# MAGIC  ,Case when trim(cpbd.BILL_EMPLOYER_ID) NOT IN ('', 'NULL') and GRP.EMPR_NAME_AGGR IN ('', 'NULL') then 'EMPLOYER UNKNOWN'
# MAGIC     when trim(cpbd.BILL_EMPLOYER_ID) NOT IN ('', 'NULL') and GRP.EMPR_NAME_AGGR is null then 'EMPLOYER UNKNOWN'
# MAGIC     Else GRP.EMPR_NAME_AGGR End As EMPLOYER_NAME
# MAGIC from (domain_contract.contract c
# MAGIC inner join domain_contract.annuity_policy ap
# MAGIC on (c.CONTRACT_ID = ap.CONTRACT_ID)
# MAGIC INNER join domain_contract.CONTRACT_POLICY_BILLING_DETAIL cpbd
# MAGIC on (c.CONTRACT_ID = cpbd.CONTRACT_ID and cpbd.BILL_TYPE = 'Policy Premium' and cpbd.BILLING_DETAIL_DELETED_INDICATOR is null))
# MAGIC left join dallasvtg.GRP_DIM grp
# MAGIC on (ltrim(rtrim(cpbd.BILL_EMPLOYER_ID)) = ltrim(rtrim(GRP.EMPR_ID_AGGR)) and ltrim(rtrim(cpbd.BILL_GROUP_NUMBER)) = ltrim(rtrim(GRP.BILLING_ID_AGGR)) and ltrim(rtrim(GRP.DELETED)) = 'N')
# MAGIC where c.contract_deleted_indicator is null
# MAGIC );
# MAGIC Select * from ANNT_CTRT_DETAILS_TEMP

# COMMAND ----------

# DBTITLE 1,ANN_POL_SUMM_EOM-1.b: All Contract with Plan Qualification details
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTRCT_PLCY_PLAN_QUAL_TEMP
# MAGIC AS 
# MAGIC (Select c.CONTRACT_ID ,c.COMPANY_CODE ,c.POLICY_NUMBER
# MAGIC    ,cppq.QUAL_PLAN_OPTION_CODE
# MAGIC    ,Case When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('412') Then '412(i)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('415') Then '415'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('45G', '45I', '4NG', '4NI') Then '457(b) Def. Comp.'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('4RG', '4RI', 'RKG', 'RKI') Then 'Roth 457(b)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('4KG', '4KI') Then '401(k)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('CUG', 'CUI') Then 'Custodial IRA'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('DCG', 'DCI') Then 'Deferred Compensation'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('ENG', 'ENI', 'ERG', 'ERI') Then 'ERISA 403(b)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('HRG', 'HRI') Then 'Keogh / HR10'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('IRG', 'IRI') Then 'Traditional IRA'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('NDI') Then 'Non-Qualified Deferred Comp'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('NII') Then 'Non-Deductible IRA'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('NQG', 'NQI') Then 'Non-Qualified'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('ORG', 'ORI') Then 'ORP'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('PNG', 'PNI') Then 'Pension / Profit Sharing'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('REG', 'REI') Then 'Roth ERISA 403(b)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('RIG', 'RII') Then 'Roth IRA'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('RNG', 'RNI', 'RTG', 'RTI')	Then 'Roth 403(b)'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('TNG', 'TNI', 'TSG', 'TSI') Then '403(b) TSA'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('SEG', 'SEI') Then 'SEP'
# MAGIC       When (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) in ('SIG', 'SII') Then 'SIMPLE IRA'
# MAGIC       Else (ltrim(rtrim(cppq.QUAL_PLAN_OPTION_CODE))) End As QUALIFIED_CODE
# MAGIC   from domain_contract.CONTRACT c
# MAGIC   inner join domain_contract.contract_policy_plan_qual cppq
# MAGIC   on c.CONTRACT_ID = cppq.CONTRACT_ID
# MAGIC   where c.CONTRACT_DELETED_INDICATOR is null
# MAGIC   );
# MAGIC 
# MAGIC Select * from CTRCT_PLCY_PLAN_QUAL_TEMP

# COMMAND ----------

# DBTITLE 1,ANN_POL_SUMM_EOM-1.c: All Contract with Valuation Information
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW CTRT_PLCY_VALUATION_TEMP
# MAGIC AS 
# MAGIC   (Select c.CONTRACT_ID ,c.COMPANY_CODE ,c.POLICY_NUMBER
# MAGIC      ,apv.ACCUMULATED_CASH_VALUE
# MAGIC      ,apv.CASH_SURRENDER_VALUE
# MAGIC      ,apv.SURRENDER_CHARGE_AMOUNT
# MAGIC      ,apv.LOAN_PRINCIPAL_OUTSTANDING
# MAGIC      ,apv.LOAN_PAYOFF_AMOUNT
# MAGIC      ,apv.GUARANTEED_INTEREST_RATE
# MAGIC   from domain_contract.contract c
# MAGIC   left join (Select * from domain_contract.annuity_policy_value where POLICY_VALUE_EXPIRY_DATE = '9999-12-31') apv  
# MAGIC   on c.CONTRACT_ID = apv.CONTRACT_ID
# MAGIC   where c.CONTRACT_DELETED_INDICATOR is null
# MAGIC   );
# MAGIC 
# MAGIC Select * from CTRT_PLCY_VALUATION_TEMP

# COMMAND ----------

# DBTITLE 1,ANN_POL_SUMM_EOM-101: ANNUITY_POLICY_SUMMARY_EOM Final Extract
# MAGIC %sql
# MAGIC Select 
# MAGIC   (last_day (add_months(Current_date, -1))) as VALUATION_DATE
# MAGIC  ,acdt.POLICY_CONTRACT_STATUS
# MAGIC  ,acdt.PREMIUM_TYPE
# MAGIC  ,cpqt.QUALIFIED_CODE
# MAGIC  ,acdt.EMPLOYER_NAME
# MAGIC  ,acdt.FLOW_STATUS
# MAGIC  ,Count(acdt.POLICY_NUMBER) as POLICY_COUNT
# MAGIC  ,Sum(cpvt.ACCUMULATED_CASH_VALUE) as ACCUMULATED_CASH_VALUE
# MAGIC  ,Sum(cpvt.CASH_SURRENDER_VALUE) as CASH_SURRENDER_VALUE
# MAGIC  ,Sum(acdt.ANNUAL_PAYMENT_AMOUNT) as ANNUAL_PAYMENT_AMOUNT
# MAGIC  
# MAGIC from ANNT_CTRT_DETAILS_TEMP acdt
# MAGIC left join CTRCT_PLCY_PLAN_QUAL_TEMP cpqt
# MAGIC on acdt.CONTRACT_ID = cpqt.CONTRACT_ID
# MAGIC left join CTRT_PLCY_VALUATION_TEMP cpvt
# MAGIC on acdt.CONTRACT_ID = cpvt.CONTRACT_ID
# MAGIC 
# MAGIC Group by
# MAGIC   (last_day (add_months(Current_date, -1))) 
# MAGIC  ,acdt.POLICY_CONTRACT_STATUS
# MAGIC  ,acdt.PREMIUM_TYPE
# MAGIC  ,cpqt.QUALIFIED_CODE
# MAGIC  ,acdt.EMPLOYER_NAME
# MAGIC  ,acdt.FLOW_STATUS
