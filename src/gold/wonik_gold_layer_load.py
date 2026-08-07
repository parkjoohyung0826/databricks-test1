# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer 적재 개요
# MAGIC %md
# MAGIC # 예산관리 PoC — Gold Layer 적재
# MAGIC 실버: `wonik_poc.wonik_test1_silver`  
# MAGIC 골드: `wonik_poc.wonik_test1_gold`  
# MAGIC 적재 기준연도: `2026`

# COMMAND ----------

# DBTITLE 1,Cell 2 설정 및 스키마 생성
CATALOG      = "wonik_poc"
SILVER_SCHEMA = "wonik_test1_silver"
GOLD_SCHEMA  = "wonik_test1_gold"
PLAN_YEAR    = "2026"

S = f"{CATALOG}.{SILVER_SCHEMA}"
G = f"{CATALOG}.{GOLD_SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{GOLD_SCHEMA}`")
print(f"✓ 스키마: {CATALOG}.{GOLD_SCHEMA}")
print(f"  SILVER: {S}")
print(f"  GOLD  : {G}")
print(f"  PLAN_YEAR: {PLAN_YEAR}")

# COMMAND ----------

# DBTITLE 1,Cell 3 차원 테이블 설명
# MAGIC %md
# MAGIC ## 1. 차원 테이블 (dim_account / dim_dept)

# COMMAND ----------

# DBTITLE 1,Cell 4 dim_account
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.dim_account (
    plan_year          STRING NOT NULL COMMENT '계획연도 (PK-1)',
    acct_cd            STRING NOT NULL COMMENT '계정코드 (PK-2)',
    acct_nm            STRING          COMMENT '계정명',
    grp_cd             STRING          COMMENT '그룹코드',
    grp_nm             STRING          COMMENT '그룹명',
    tot_cost_acct      STRING          COMMENT '총비용계정',
    tot_cost_acct_comp STRING          COMMENT '총비용계정_비교',
    opex_acct_yn       STRING          COMMENT '영업경비여부 (Y/N)',
    _update_at         TIMESTAMP       COMMENT '골드 갱신 일시',
    CONSTRAINT pk_gold_dim_account PRIMARY KEY (plan_year, acct_cd)
) USING DELTA COMMENT '결산 및 예산 분석용 계정 마스터 차원'
""")
print("✓ DDL: dim_account")

spark.sql(f"""
INSERT OVERWRITE {G}.dim_account
SELECT
    plan_year, acct_cd, acct_nm, grp_cd, grp_nm,
    tot_cost_acct, tot_cost_acct_comp, opex_acct_yn,
    current_timestamp()
FROM {S}.bgt_account_master
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {G}.dim_account").collect()[0].c
print(f"✓ DML: dim_account → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 5 dim_dept
spark.sql(f"DROP TABLE IF EXISTS {G}.dim_dept")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.dim_dept (
    plan_year   STRING NOT NULL COMMENT '계획연도 (PK-1)',
    dept_cd     STRING NOT NULL COMMENT '부서코드 (PK-2)',
    dept_nm     STRING          COMMENT '부서명',
    biz_unit    STRING          COMMENT '사업부',
    cc_cd       STRING          COMMENT 'CC코드',
    cc_nm       STRING          COMMENT 'CC명',
    cc_cat      STRING          COMMENT 'CC구분',
    _update_at  TIMESTAMP       COMMENT '골드 갱신 일시',
    CONSTRAINT pk_gold_dim_dept PRIMARY KEY (plan_year, dept_cd)
) USING DELTA COMMENT '결산 및 예산 분석용 부서/조직 마스터 차원'
""")
print("✓ DDL: dim_dept")

spark.sql(f"""
INSERT OVERWRITE {G}.dim_dept
SELECT
    plan_year, dept_cd, dept_nm, biz_unit,
    cc_cd, cc_nm, cc_cat, current_timestamp()
FROM {S}.bgt_dept_master
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {G}.dim_dept").collect()[0].c
print(f"✓ DML: dim_dept → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 6 집계 테이블 설명
# MAGIC %md
# MAGIC ## 2. 골드 집계 테이블

# COMMAND ----------

# DBTITLE 1,Cell 7 bgt_inv_st
# 주의: silver.bgt_investment_pjt 데이터 없음 → DDL만 성공하면 0건 시도로 업스트림

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.bgt_inv_st (
    plan_year           STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    summary_type        STRING      NOT NULL COMMENT '요약구분 (확정상태/자산유형) (PK-2)',
    cat_nm              STRING      NOT NULL COMMENT '구분명 (PK-3)',
    yr_inv_plan         DECIMAL(18,2)       COMMENT '년_투자계획 합계',
    plan_cnt            BIGINT              COMMENT '건수_계획 (프로젝트 수)',
    yr_inv_exec         DECIMAL(18,2)       COMMENT '년_투자집행 합계',
    exec_rate           DECIMAL(5,2)        COMMENT '집행률(%)',
    total_act           DECIMAL(18,2)       COMMENT '실적_총실적 합계',
    total_progress_rate DECIMAL(5,2)        COMMENT '실적_총진행률(%)',
    act_exp_proc_amt    DECIMAL(18,2)       COMMENT '실적_비용처리금액 합계',
    act_exp_proc_rate   DECIMAL(5,2)        COMMENT '실적_비용처리진행률(%)',
    act_exp_unproc      DECIMAL(18,2)       COMMENT '실적_비용미처리금액 합계',
    act_exp_unproc_rate DECIMAL(5,2)        COMMENT '실적_비용미처리진행률(%)',
    rem_inv_plan_amt    DECIMAL(18,2)       COMMENT '투자계획_잔여금액 (계획-총실적)',
    _update_at          TIMESTAMP           COMMENT '골드 생성 일시',
    CONSTRAINT pk_bgt_inv_st PRIMARY KEY (plan_year, summary_type, cat_nm)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT '예산_투자현황 (확정상태 및 자산유형별 통합 요약)'
""")
print("✓ DDL: bgt_inv_st")

spark.sql(f"""
INSERT OVERWRITE {G}.bgt_inv_st
SELECT
    plan_year,
    summary_type,
    cat_nm,
    yr_inv_plan,
    plan_cnt,
    yr_inv_exec,
    CASE WHEN yr_inv_plan = 0 THEN CAST(0 AS DECIMAL(5,2))
         ELSE CAST((yr_inv_exec / yr_inv_plan) * 100 AS DECIMAL(5,2)) END AS exec_rate,
    (act_exp_proc + act_exp_unproc)                                        AS total_act,
    CASE WHEN yr_inv_plan = 0 THEN CAST(0 AS DECIMAL(5,2))
         ELSE CAST(((act_exp_proc + act_exp_unproc) / yr_inv_plan) * 100 AS DECIMAL(5,2)) END AS total_progress_rate,
    act_exp_proc                                                            AS act_exp_proc_amt,
    CASE WHEN yr_inv_plan = 0 THEN CAST(0 AS DECIMAL(5,2))
         ELSE CAST((act_exp_proc / yr_inv_plan) * 100 AS DECIMAL(5,2)) END AS act_exp_proc_rate,
    act_exp_unproc,
    CASE WHEN yr_inv_plan = 0 THEN CAST(0 AS DECIMAL(5,2))
         ELSE CAST((act_exp_unproc / yr_inv_plan) * 100 AS DECIMAL(5,2)) END AS act_exp_unproc_rate,
    (yr_inv_plan - yr_inv_exec)                                             AS rem_inv_plan_amt,
    current_timestamp()
FROM (
    -- A. 예산확정상태 기준
    SELECT
        plan_year,
        '예산확정상태'                                          AS summary_type,
        cat                                                         AS cat_nm,
        SUM(COALESCE(yr_inv_plan, 0))                               AS yr_inv_plan,
        COUNT(pjt_nm)                                               AS plan_cnt,
        SUM(COALESCE(yr_inv_exec, 0))                               AS yr_inv_exec,
        SUM(COALESCE(CAST(act_exp_proc   AS DECIMAL(18,2)), 0))     AS act_exp_proc,
        SUM(COALESCE(CAST(act_exp_unproc AS DECIMAL(18,2)), 0))     AS act_exp_unproc
    FROM {S}.bgt_investment_pjt
    GROUP BY plan_year, cat
    UNION ALL
    -- B. 투자자산유형 기준
    SELECT
        plan_year,
        '투자자산유형'                                          AS summary_type,
        inv_tp                                                      AS cat_nm,
        SUM(COALESCE(yr_inv_plan, 0))                               AS yr_inv_plan,
        COUNT(pjt_nm)                                               AS plan_cnt,
        SUM(COALESCE(yr_inv_exec, 0))                               AS yr_inv_exec,
        SUM(COALESCE(CAST(act_exp_proc   AS DECIMAL(18,2)), 0))     AS act_exp_proc,
        SUM(COALESCE(CAST(act_exp_unproc AS DECIMAL(18,2)), 0))     AS act_exp_unproc
    FROM {S}.bgt_investment_pjt
    GROUP BY plan_year, inv_tp
)
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {G}.bgt_inv_st").collect()[0].c
print(f"✓ DML: bgt_inv_st → {cnt:,}건")
if cnt == 0:
    print("  ※ silver.bgt_investment_pjt 0건 — 투자 PJT 데이터 업로드 후 재실행 필요")

# COMMAND ----------

# DBTITLE 1,Cell 8 bgt_expense_st
spark.sql(f"DROP TABLE IF EXISTS {G}.bgt_expense_st")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.bgt_expense_st (
    plan_year       STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    base_month      STRING      NOT NULL COMMENT '기준월 (PK-2)',
    expense_cat     STRING      NOT NULL COMMENT '경비구분 (경상/영업활동) (PK-3)',
    trns_include_yn STRING      NOT NULL COMMENT '이관포함여부 (Y/N) (PK-4)',
    biz_unit        STRING              COMMENT '사업부/본부',
    dept_cd         STRING      NOT NULL COMMENT '부서코드 (PK-5)',
    acct_cd         STRING      NOT NULL COMMENT '계정코드 (PK-6)',
    plan_amt        DECIMAL(18,2)       COMMENT '예산계획금액',
    trns_amt        DECIMAL(18,2)       COMMENT '예산이관/전용금액 (가감액)',
    final_bgt_amt   DECIMAL(18,2)       COMMENT '최종예산금액 (계획+이관)',
    act_amt         DECIMAL(18,2)       COMMENT '실적금액',
    rem_bgt_amt     DECIMAL(18,2)       COMMENT '예산잔여금액 (집행 가능 잔액)',
    excess_amt      DECIMAL(18,2)       COMMENT '예산초과금액 (예산 초과 집행액)',
    exec_rate       DECIMAL(5,2)        COMMENT '집행률(%)',
    _update_at      TIMESTAMP           COMMENT '골드 생성 일시',
    CONSTRAINT pk_bgt_expense_st PRIMARY KEY (plan_year, base_month, expense_cat, trns_include_yn, dept_cd, acct_cd)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT '예산_경비현황 (잔여 및 초과액 명시 모델)'
""")
print("✓ DDL: bgt_expense_st")

spark.sql(f"""
INSERT OVERWRITE {G}.bgt_expense_st
SELECT
    t.plan_year,
    t.base_month,
    t.expense_cat,
    t.trns_include_yn,
    m.biz_unit,
    t.dept_cd,
    t.acct_cd,
    t.plan_amt,
    t.trns_amt,
    t.final_bgt_amt,
    t.act_amt,
    CASE WHEN (t.final_bgt_amt - t.act_amt) > 0
         THEN  (t.final_bgt_amt - t.act_amt)
         ELSE  CAST(0 AS DECIMAL(18,2)) END       AS rem_bgt_amt,
    CASE WHEN (t.final_bgt_amt - t.act_amt) < 0
         THEN  ABS(t.final_bgt_amt - t.act_amt)
         ELSE  CAST(0 AS DECIMAL(18,2)) END       AS excess_amt,
    CASE WHEN t.final_bgt_amt = 0 THEN CAST(0.0 AS DECIMAL(5,2))
         ELSE TRY_CAST(ROUND(CAST(t.act_amt AS DOUBLE) / CAST(t.final_bgt_amt AS DOUBLE) * 100.0, 2) AS DECIMAL(5,2)) END AS exec_rate,
    current_timestamp()
FROM (
    SELECT
        COALESCE(p.plan_year,  a.plan_year,  tr.plan_year)  AS plan_year,
        COALESCE(p.month_val,  a.month_val,  tr.month_val)  AS base_month,
        CASE WHEN acc.opex_acct_yn = 'Y'
             THEN '영업활동경비' ELSE '경상경비' END              AS expense_cat,
        'Y'                                                 AS trns_include_yn,
        COALESCE(p.attr_dept_cd, a.dept_cd)                AS dept_cd,
        COALESCE(p.acct_cd, a.acct_cd, tr.acct_cd)         AS acct_cd,
        SUM(COALESCE(p.final_amount, 0))                    AS plan_amt,
        SUM(COALESCE(tr.final_amount, 0))                   AS trns_amt,
        SUM(COALESCE(p.final_amount, 0) + COALESCE(tr.final_amount, 0)) AS final_bgt_amt,
        SUM(COALESCE(a.dr_local_amount, 0)) AS act_amt  -- 차변 합계 기준 (순액 방식 사용 금지)
    FROM {S}.bgt_plan p
    FULL OUTER JOIN {S}.bgt_actual_gl a
        ON  p.plan_year    = a.plan_year
        AND p.month_val    = a.month_val
        AND p.acct_cd      = a.acct_cd
        AND p.attr_dept_cd = a.dept_cd
    FULL OUTER JOIN {S}.bgt_transfer tr
        ON  COALESCE(p.plan_year,  a.plan_year)  = tr.plan_year
        AND COALESCE(p.month_val,  a.month_val)  = tr.month_val
        AND COALESCE(p.acct_cd,    a.acct_cd)    = tr.acct_cd
    LEFT JOIN {S}.bgt_account_master acc
        ON  COALESCE(p.plan_year, a.plan_year, tr.plan_year) = acc.plan_year
        AND COALESCE(p.acct_cd,   a.acct_cd,  tr.acct_cd)   = acc.acct_cd
    GROUP BY
        COALESCE(p.plan_year,  a.plan_year,  tr.plan_year),
        COALESCE(p.month_val,  a.month_val,  tr.month_val),
        CASE WHEN acc.opex_acct_yn = 'Y' THEN '영업활동경비' ELSE '경상경비' END,
        'Y',
        COALESCE(p.attr_dept_cd, a.dept_cd),
        COALESCE(p.acct_cd, a.acct_cd, tr.acct_cd)
) t
LEFT JOIN {S}.bgt_dept_master m
    ON t.plan_year = m.plan_year
    AND t.dept_cd  = m.dept_cd
WHERE t.dept_cd IS NOT NULL
  AND t.acct_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {G}.bgt_expense_st").collect()[0].c
print(f"✓ DML: bgt_expense_st → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 9 최종 확인 설명
# MAGIC %md
# MAGIC ## 3. 최종 확인

# COMMAND ----------

# DBTITLE 1,Cell 10 최종 확인
print(f"=== {CATALOG}.{GOLD_SCHEMA} 골드 테이블 현황 ===\n")
tables = [
    t.tableName for t in
    spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{GOLD_SCHEMA}`").collect()
    if not t.isTemporary
]
total = 0
for tbl in sorted(tables):
    cnt = spark.sql(
        f"SELECT COUNT(*) AS c FROM `{CATALOG}`.`{GOLD_SCHEMA}`.`{tbl}`"
    ).collect()[0].c
    total += cnt
    print(f"  {tbl:<40} {cnt:>8,}건")
print(f"\n  {'[ TOTAL ]':<40} {total:>8,}건")
print(f"\n  테이블 수: {len(tables)}개  |  골드 레이어 구축 완료 ✓")