# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer 적재 개요
# MAGIC %md
# MAGIC # 예산관리 PoC — Silver Layer 적재
# MAGIC 브론즈: `wonik_poc.wonik_test1_bronze`  
# MAGIC 실버: `wonik_poc.wonik_test1_silver`  
# MAGIC 적재 기준연도: `2026`

# COMMAND ----------

# DBTITLE 1,Cell 2 설정 및 스키마 생성
CATALOG       = "wonik_poc"
BRONZE_SCHEMA = "wonik_test1_bronze"
SILVER_SCHEMA = "wonik_test1_silver"
PLAN_YEAR     = "2026"

B = f"{CATALOG}.{BRONZE_SCHEMA}"
S = f"{CATALOG}.{SILVER_SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SILVER_SCHEMA}`")
print(f"✓ 스키마: {CATALOG}.{SILVER_SCHEMA}")
print(f"  BRONZE: {B}")
print(f"  SILVER: {S}")
print(f"  PLAN_YEAR: {PLAN_YEAR}")

# COMMAND ----------

# DBTITLE 1,Cell 3 마스터 설명
# MAGIC %md
# MAGIC ## 1. 마스터 테이블

# COMMAND ----------

# DBTITLE 1,Cell 4 bgt_account_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_account_master (
    plan_year          STRING NOT NULL COMMENT '계획연도 (PK-1)',
    acct_cd            STRING NOT NULL COMMENT '계정코드 (PK-2)',
    acct_nm            STRING          COMMENT '계정명',
    grp_cd             STRING          COMMENT '그룹코드',
    grp_nm             STRING          COMMENT '그룹명',
    tot_cost_acct      STRING          COMMENT '총비용계정',
    tot_cost_acct_comp STRING          COMMENT '총비용계정_비교',
    opex_acct_yn       STRING          COMMENT '영업경비여부 (Y/N)',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_account PRIMARY KEY (plan_year, acct_cd)
) USING DELTA COMMENT '예산 계정 마스터 정보'
""")
print("✓ DDL: bgt_account_master")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_account_master
SELECT
    plan_year,
    TRIM(acct_cd)                AS acct_cd,
    acct_nm,
    grp_cd,
    grp_nm,
    tot_cost_acct,
    tot_cost_acct_comp,
    opex_acct_yn,
    current_timestamp()          AS _ingest_at
FROM {B}.bgt_acct_info
WHERE acct_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_account_master").collect()[0].c
print(f"✓ DML: bgt_account_master → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 5 bgt_dept_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_dept_master (
    plan_year   STRING NOT NULL COMMENT '계획연도 (PK-1)',
    dept_cd     STRING NOT NULL COMMENT '부서코드 (PK-2)',
    dept_nm     STRING          COMMENT '부서명',
    biz_unit    STRING          COMMENT '사업부',
    cc_cd       STRING          COMMENT 'CC코드',
    cc_nm       STRING          COMMENT 'CC명',
    cc_cat      STRING          COMMENT 'CC구분',
    _ingest_at  TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_dept PRIMARY KEY (plan_year, dept_cd)
) USING DELTA COMMENT '예산 조직/부서 마스터 정보'
""")
print("✓ DDL: bgt_dept_master")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_dept_master
SELECT
    plan_year,
    TRIM(dept_cd)           AS dept_cd,
    dept_nm,
    biz_unit,
    cc_cd,
    cc_nm,
    cc_cat,
    current_timestamp()     AS _ingest_at
FROM {B}.bgt_dept_cd
WHERE dept_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_dept_master").collect()[0].c
print(f"✓ DML: bgt_dept_master → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 6 bgt_dept_standard
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_dept_standard (
    plan_year   STRING NOT NULL COMMENT '계획연도 (PK-1)',
    dept_nm     STRING NOT NULL COMMENT '부서명 (PK-2)',
    dept_std    STRING          COMMENT '부서기준',
    _ingest_at  TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_dept_std PRIMARY KEY (plan_year, dept_nm)
) USING DELTA COMMENT '부서별 예산 관리 기준 정보'
""")
print("✓ DDL: bgt_dept_standard")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_dept_standard
SELECT
    plan_year,
    TRIM(dept_nm)       AS dept_nm,
    dept_std,
    current_timestamp() AS _ingest_at
FROM {B}.bgt_dept_std
WHERE dept_nm IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_dept_standard").collect()[0].c
print(f"✓ DML: bgt_dept_standard → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 7 bgt_dept_biz_unit
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_dept_biz_unit (
    plan_year   STRING NOT NULL COMMENT '계획연도 (PK-1)',
    dept_nm     STRING NOT NULL COMMENT '부서명 (PK-2)',
    biz_unit    STRING          COMMENT '사업부',
    _ingest_at  TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_dept_bu PRIMARY KEY (plan_year, dept_nm)
) USING DELTA COMMENT '부서별 사업부 매핑 정보'
""")
print("✓ DDL: bgt_dept_biz_unit")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_dept_biz_unit
SELECT
    plan_year,
    TRIM(dept_nm)       AS dept_nm,
    biz_unit,
    current_timestamp() AS _ingest_at
FROM {B}.bgt_dept_biz_unit
WHERE dept_nm IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_dept_biz_unit").collect()[0].c
print(f"✓ DML: bgt_dept_biz_unit → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 8 트랜잭션 설명
# MAGIC %md
# MAGIC ## 2. 트랜잭션 테이블

# COMMAND ----------

# DBTITLE 1,Cell 9 bgt_actual_gl
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_actual_gl (
    plan_year       STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    month_val       STRING      NOT NULL COMMENT '월 2자리 (PK-2)',
    slip_no         STRING      NOT NULL COMMENT '전표번호 (PK-3)',
    acct_cd         STRING      NOT NULL COMMENT '계정코드 (PK-4 / FK)',
    dept_cd         STRING      NOT NULL COMMENT '부서코드 (PK-5 / FK)',
    slip_dt         DATE                COMMENT '회계일',
    rmk             STRING              COMMENT '비고',
    crtr_id         STRING              COMMENT '작성자',
    slip_dept_cd    STRING              COMMENT '관리부서코드',
    curr_cd         STRING              COMMENT '통화',
    slip_path       STRING              COMMENT '전표생성경로',
    ref_no          STRING              COMMENT '참조번호',
    pjt_no          STRING              COMMENT 'Project No',
    dr_local_amount DECIMAL(18,0)       COMMENT '차변금액_자국',
    cr_local_amount DECIMAL(18,0)       COMMENT '대변금액_자국',
    _ingest_at      TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_actual PRIMARY KEY (plan_year, month_val, slip_no, acct_cd, dept_cd),
    CONSTRAINT fk_actual_acct FOREIGN KEY (plan_year, acct_cd)
        REFERENCES {S}.bgt_account_master (plan_year, acct_cd),
    CONSTRAINT fk_actual_dept FOREIGN KEY (plan_year, dept_cd)
        REFERENCES {S}.bgt_dept_master (plan_year, dept_cd)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT '정제된 ERP 예산 실적 원장'
""")
print("✓ DDL: bgt_actual_gl")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_actual_gl
SELECT DISTINCT
    plan_year,
    LPAD(CAST(CAST(month_val AS DOUBLE) AS BIGINT), 2, '0')                AS month_val,
    TRIM(slip_no)                                                          AS slip_no,
    TRIM(acct_cd)                                                          AS acct_cd,
    TRIM(dept_cd)                                                          AS dept_cd,
    TO_DATE(CAST(slip_dt AS STRING))                                       AS slip_dt,
    rmk,
    crtr_id,
    mgt_dept                                                               AS slip_dept_cd,
    curr_cd,
    slip_path,
    ref_no,
    pjt_no,
    CAST(REPLACE(CAST(dr_local_amount AS STRING), ',', '') AS DECIMAL(18,0)) AS dr_local_amount,
    CAST(REPLACE(CAST(cr_local_amount AS STRING), ',', '') AS DECIMAL(18,0)) AS cr_local_amount,
    current_timestamp()                                                    AS _ingest_at
FROM {B}.bgt_act_erp_gl
WHERE slip_no IS NOT NULL
  AND acct_cd IS NOT NULL
  AND dept_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_actual_gl").collect()[0].c
print(f"✓ DML: bgt_actual_gl → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 10 bgt_plan
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_plan (
    plan_year     STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    month_val     STRING      NOT NULL COMMENT '월 2자리 (PK-2)',
    seq_no        STRING      NOT NULL COMMENT '일련번호 (PK-3)',
    cat           STRING              COMMENT '구분',
    acct_cd       STRING      NOT NULL COMMENT '계정코드 (FK)',
    summary_desc  STRING              COMMENT '적요',
    budget_amount DECIMAL(18,2)       COMMENT '예산금액',
    final_amount  DECIMAL(18,2)       COMMENT '최종금액',
    slip_dept_cd  STRING              COMMENT '기표부서',
    attr_dept_cd  STRING              COMMENT '귀속부서',
    _ingest_at    TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_plan PRIMARY KEY (plan_year, month_val, seq_no),
    CONSTRAINT fk_plan_acct FOREIGN KEY (plan_year, acct_cd)
        REFERENCES {S}.bgt_account_master (plan_year, acct_cd),
    CONSTRAINT fk_plan_slip_dept FOREIGN KEY (plan_year, slip_dept_cd)
        REFERENCES {S}.bgt_dept_master (plan_year, dept_cd),
    CONSTRAINT fk_plan_attr_dept FOREIGN KEY (plan_year, attr_dept_cd)
        REFERENCES {S}.bgt_dept_master (plan_year, dept_cd)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT '정제된 예산 계획 데이터'
""")
print("✓ DDL: bgt_plan")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_plan
SELECT
    plan_year,
    LPAD(CAST(CAST(month_val AS DOUBLE) AS BIGINT), 2, '0')            AS month_val,
    CONCAT(
        TRIM(CAST(seq_no AS STRING)), '_',
        LPAD(
            CAST(
                ROW_NUMBER() OVER (
                    PARTITION BY month_val, seq_no
                    ORDER BY acct_cd, summary_desc, slip_dept, attr_dept
                ) AS STRING
            ),
            3,
            '0'
        )
    )                                                                  AS seq_no,
    cat,
    TRIM(acct_cd)                                                      AS acct_cd,
    summary_desc,
    CAST(REPLACE(CAST(bgt_amount AS STRING), ',', '') AS DECIMAL(18,2)) AS budget_amount,
    CAST(REPLACE(CAST(final_amount AS STRING), ',', '') AS DECIMAL(18,2)) AS final_amount,
    TRIM(slip_dept)                                                    AS slip_dept_cd,
    TRIM(attr_dept)                                                    AS attr_dept_cd,
    current_timestamp()                                                AS _ingest_at
FROM {B}.bgt_plan_xl
WHERE seq_no IS NOT NULL
  AND acct_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_plan").collect()[0].c
print(f"✓ DML: bgt_plan → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 11 bgt_transfer
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_transfer (
    plan_year     STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    month_val     STRING      NOT NULL COMMENT '월 2자리 (PK-2)',
    acct_cd       STRING      NOT NULL COMMENT '계정코드 (PK-3, FK)',
    slip_dept_cd  STRING      NOT NULL COMMENT '기표부서 (PK-4, FK)',
    attr_dept_cd  STRING      NOT NULL COMMENT '귀속부서 (PK-5, FK)',
    cat_1         STRING              COMMENT '구분1 (이관/임원/전용/초과)',
    cat_2         STRING              COMMENT '구분2',
    yr_int_acct   STRING              COMMENT '연_통합계정',
    summary_desc  STRING              COMMENT '적요',
    final_amount  DECIMAL(18,2)       COMMENT '최종금액',
    appr_dt       DATE                COMMENT '품의일시',
    appr_no       STRING              COMMENT '품의번호',
    bgt_mgt_no    STRING              COMMENT '예산관리번호',
    _ingest_at    TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_transfer PRIMARY KEY (plan_year, month_val, acct_cd, slip_dept_cd, attr_dept_cd),
    CONSTRAINT fk_trns_acct FOREIGN KEY (plan_year, acct_cd)
        REFERENCES {S}.bgt_account_master (plan_year, acct_cd),
    CONSTRAINT fk_trns_slip_dept FOREIGN KEY (plan_year, slip_dept_cd)
        REFERENCES {S}.bgt_dept_master (plan_year, dept_cd),
    CONSTRAINT fk_trns_attr_dept FOREIGN KEY (plan_year, attr_dept_cd)
        REFERENCES {S}.bgt_dept_master (plan_year, dept_cd)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT 'ERP 예산 이관/조정 데이터'
""")
print("✓ DDL: bgt_transfer")

spark.sql(f"""
INSERT OVERWRITE {S}.bgt_transfer
SELECT
    plan_year,
    LPAD(CAST(CAST(month_val AS DOUBLE) AS BIGINT), 2, '0')            AS month_val,
    TRIM(acct_cd)                                                      AS acct_cd,
    TRIM(slip_dept)                                                    AS slip_dept_cd,
    TRIM(attr_dept)                                                    AS attr_dept_cd,
    cat_1,
    cat_2,
    yr_int_acct,
    summary_desc,
    CAST(REGEXP_REPLACE(CAST(final_amount AS STRING), '[^0-9.-]', '') AS DECIMAL(18,2)) AS final_amount,
    TO_DATE(CAST(appr_dt AS STRING))                                   AS appr_dt,
    appr_no,
    bgt_mgt_no,
    current_timestamp()                                                AS _ingest_at
FROM {B}.bgt_erp_trns
WHERE acct_cd IS NOT NULL
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_transfer").collect()[0].c
print(f"✓ DML: bgt_transfer → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 12 bgt_investment_pjt
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.bgt_investment_pjt (
    plan_year      STRING      NOT NULL COMMENT '계획연도 (PK-1)',
    pjt_nm         STRING      NOT NULL COMMENT '프로젝트명 (PK-2)',
    pjt_code       STRING              COMMENT '프로젝트코드',
    dept_nm        STRING              COMMENT '부서명',
    cat            STRING              COMMENT '구분 (반영/미반영)',
    inv_tp         STRING              COMMENT '투자유형',
    yr_inv_plan    DECIMAL(18,0)       COMMENT '연_투자계획',
    yr_inv_exec    DECIMAL(18,0)       COMMENT '연_투자집행',
    act_exp_proc   DECIMAL(18,0)       COMMENT '실적_비용처리',
    act_exp_unproc DECIMAL(5,2)        COMMENT '실적_비용미처리(%)',
    remarks        STRING              COMMENT '비고',
    _ingest_at     TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_bgt_inv PRIMARY KEY (plan_year, pjt_nm)
) USING DELTA PARTITIONED BY (plan_year)
COMMENT '프로젝트별 투자 집행 현황'
""")
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.bgt_investment_pjt").collect()[0].c
print(f"✓ DDL: bgt_investment_pjt → {cnt:,}건 (DDL만 생성, 원천 브론즈 없음)")
print("  ※ 투자 PJT 브론즈 데이터 업로드 후 DML 실행 필요")

# COMMAND ----------

# DBTITLE 1,결산 테이블 DDL 섹션 헤더
# MAGIC %md
# MAGIC ## 4. 결산 테이블 DDL
# MAGIC 브론즈 결산 데이터를 정제하여 실버 레이어에 적재하기 위한 12개 테이블 DDL
# MAGIC
# MAGIC | # | 테이블명 | 설명 |
# MAGIC |---|---|---|
# MAGIC | 9 | acc_bu_pl_mth | 결산_사업부손익_통합 (1~12월) |
# MAGIC | 10 | acc_alloc_master | 결산_배부마스터 |
# MAGIC | 11 | acc_alloc_criteria_val | 결산_배부기준값_통합 |
# MAGIC | 12 | acc_vf_cost_standard | 결산_변고기준마스터 |
# MAGIC | 13 | acc_vf_cost_revision | 결산_변고검토 |
# MAGIC | 14 | acc_general_standard | 결산_일반기준 |
# MAGIC | 15 | acc_act_erp_gl | 결산_실적_ERP원장 |
# MAGIC | 16 | acc_erp_mgt_cost | 결산_인관전용_ERP |
# MAGIC | 17 | acc_mfg_gp_intg | 제조매출원가+품목별매출이익 통합 |
# MAGIC | 18 | acc_mfg_inv_intg | 제조비용+재고효과 통합 |
# MAGIC | 19 | setl_sls_mat_dtl | 결산_매출+재료비계획_상세 |
# MAGIC | 20 | setl_plan_detail_clean | 결산_계획_상세_원장 |

# COMMAND ----------

# DBTITLE 1,DDL 09. acc_bu_pl_mth (결산_사업부손익_통합)
# 9. [실버] 결산_사업부손익_통합 — 브론즈 14~25번(1월~12월) 통합
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_bu_pl_mth (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    month_val          STRING NOT NULL COMMENT '월 (PK-2)',
    cat_1              STRING NOT NULL COMMENT '구분1 (PK-3)',
    cat_2              STRING NOT NULL COMMENT '구분2 (PK-4)',
    gss_amount         DECIMAL(18,2)   COMMENT 'GSS 금액',
    gpu_amount         DECIMAL(18,2)   COMMENT 'GPU 금액',
    scr_amount         DECIMAL(18,2)   COMMENT 'SCR 금액',
    enc_amount         DECIMAL(18,2)   COMMENT 'EnC 금액',
    bpc_amount         DECIMAL(18,2)   COMMENT 'BPC 금액',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_bu_pl PRIMARY KEY (fisc_year, month_val, cat_1, cat_2)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '월별 사업부 손익 실적 통합 (1월~12월)'
""")
print("✓ DDL: acc_bu_pl_mth")

# COMMAND ----------

# DBTITLE 1,DDL 10. acc_alloc_master (결산_배부마스터)
# 10. [실버] 결산_배부마스터 — 브론즈 11번(acc_alloc_mst) 기반
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_alloc_master (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    dept_code          STRING NOT NULL COMMENT '부서코드 (PK-2)',
    category           STRING NOT NULL COMMENT '구분 (PK-3)',
    dept_nm            STRING          COMMENT '부서명',
    macro_team_system  STRING          COMMENT '대팀제',
    recogn_team        STRING          COMMENT '인식팀',
    recogn_dept        STRING          COMMENT '인식부서',
    pl_hq              STRING          COMMENT '손익본부',
    hq_nm              STRING          COMMENT '본부명',
    biz_unit_nm        STRING          COMMENT '사업부명',
    mfg_sgna_type      STRING          COMMENT '제조/판관 구분',
    dir_indir_type     STRING          COMMENT '직접/간접 구분',
    alloc_type         STRING          COMMENT '배부 구분',
    gss_alloc_rate     DECIMAL(7,4)    COMMENT 'GSS 배부비율',
    gpu_alloc_rate     DECIMAL(7,4)    COMMENT 'GPU 배부비율',
    scr_alloc_rate     DECIMAL(7,4)    COMMENT 'SCR 배부비율',
    enc_alloc_rate     DECIMAL(7,4)    COMMENT 'EnC 배부비율',
    epc_alloc_rate     DECIMAL(7,4)    COMMENT 'EPC 배부비율',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_alloc_master PRIMARY KEY (fisc_year, dept_code, category)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '결산_배부 마스터'
""")
print("✓ DDL: acc_alloc_master")

# COMMAND ----------

# DBTITLE 1,DDL 11. acc_alloc_criteria_val (결산_배부기준값_통합)
# 11. [실버] 결산_배부기준값_통합 — 브론즈 5~10번 (criteria_tp로 통합)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_alloc_criteria_val (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    criteria_tp        STRING NOT NULL COMMENT '기준유형 (구매팀/매출/인원 등) (PK-2)',
    category           STRING NOT NULL COMMENT '구분 (PK-3)',
    gss_val            DECIMAL(18,2)   COMMENT 'GSS 수치',
    gpu_val            DECIMAL(18,2)   COMMENT 'GPU 수치',
    scr_val            DECIMAL(18,2)   COMMENT 'SCR 수치',
    enc_val            DECIMAL(18,2)   COMMENT 'EnC 수치',
    epc_val            DECIMAL(18,2)   COMMENT 'EPC 수치',
    remarks            STRING          COMMENT '비고',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_alloc_val PRIMARY KEY (fisc_year, criteria_tp, category)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '결산_배부 기준값 통합'
""")
print("✓ DDL: acc_alloc_criteria_val")

# COMMAND ----------

# DBTITLE 1,DDL 12. acc_vf_cost_standard (결산_변고기준마스터)
# 12. [실버] 결산_변고기준마스터 — 브론즈 13번(acc_vf_cost_std) 기반
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_vf_cost_standard (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    acct_code          STRING NOT NULL COMMENT '계정코드 (PK-2)',
    dept_code          STRING NOT NULL COMMENT '부서코드 (PK-3)',
    macro_team_system  STRING          COMMENT '대팀제',
    expense_tp         STRING          COMMENT '비용구분',
    integrated_acct    STRING          COMMENT '통합계정',
    large_category     STRING          COMMENT '대구분',
    vf_type            STRING          COMMENT '변동비/고정비 구분',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_vf_std PRIMARY KEY (fisc_year, acct_code, dept_code)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '계정별 변동비/고정비 및 대팀제 분류 마스터'
""")
print("✓ DDL: acc_vf_cost_standard")

# COMMAND ----------

# DBTITLE 1,DDL 13. acc_vf_cost_revision (결산_변고검토)
# 13. [실버] 결산_변고검토 — 브론즈 12번(acc_vf_cost_rev) 기반
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_vf_cost_revision (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    acct_code          STRING NOT NULL COMMENT '계정코드 (PK-2)',
    dept_code          STRING NOT NULL COMMENT '부서코드 (PK-3)',
    expense_tp         STRING          COMMENT '비용구분',
    integrated_acct    STRING          COMMENT '통합계정',
    large_category     STRING          COMMENT '대구분',
    vf_type            STRING          COMMENT '변동비/고정비 구분',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_vf_rev PRIMARY KEY (fisc_year, acct_code, dept_code)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '계정별 변동비/고정비 분류 검토 및 이력 데이터'
""")
print("✓ DDL: acc_vf_cost_revision")

# COMMAND ----------

# DBTITLE 1,DDL 14. acc_general_standard (결산_일반기준)
# 14. [실버] 결산_일반기준 — 브론즈 30~32번(부서구분/원장조회/직간접구분) 통합
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_general_standard (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    std_type           STRING NOT NULL COMMENT '기준유형 (부서구분/원장조회/직간접구분) (PK-2)',
    cat_1              STRING NOT NULL COMMENT '구분1 (PK-3)',
    cat_2              STRING          COMMENT '구분2',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_gen_std PRIMARY KEY (fisc_year, std_type, cat_1)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '결산용 일반 기준 정보 통합 관리 (부서/원장/직간접 등)'
""")
print("✓ DDL: acc_general_standard")

# COMMAND ----------

# DBTITLE 1,DDL 15. acc_act_erp_gl (결산_실적_ERP원장)
# 15. [실버] 결산_실적_ERP원장 — 브론즈 28번(acc_act_erp_gl) 정제
spark.sql(f"DROP TABLE IF EXISTS {S}.acc_act_erp_gl")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_act_erp_gl (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    month_val          STRING NOT NULL COMMENT '월 (PK-2)',
    slip_no            STRING NOT NULL COMMENT '전표번호 (PK-3)',
    slip_seq           STRING NOT NULL COMMENT '전표번호순번 (PK-4)',
    acct_code          STRING NOT NULL COMMENT '계정코드 (PK-5)',
    dept_code          STRING NOT NULL COMMENT '부서코드 (PK-6)',
    m_team             STRING          COMMENT '대팀제',
    biz_unit           STRING          COMMENT '사업부',
    acct_nm            STRING          COMMENT '계정명',
    dept_name          STRING          COMMENT '부서명',
    curr_cd            STRING          COMMENT '통화',
    dr_amount          STRING          COMMENT '차변금액',
    cr_amount          STRING          COMMENT '대변금액',
    dr_local_amt       DECIMAL(18,2)   COMMENT '차변금액_자국',
    cr_local_amt       DECIMAL(18,2)   COMMENT '대변금액_자국',
    slip_dt            STRING          COMMENT '회계일 (전표일)',
    rmk                STRING          COMMENT '비고',
    _ingest_at         TIMESTAMP       COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_act_erp_gl PRIMARY KEY (fisc_year, month_val, slip_no, slip_seq, acct_code, dept_code)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '정제된 전사 실제 회계 전표 원장 (대팀제 및 금액 정제)'
""")
print("✓ DDL: acc_act_erp_gl")

# COMMAND ----------

# DBTITLE 1,DDL 16. acc_erp_mgt_cost (결산_인관전용_ERP)
# 16. [실버] 결산_인관전용_ERP — 브론즈 29번(acc_erp_mgt_cost) 정제
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_erp_mgt_cost (
    fisc_year          STRING NOT NULL COMMENT '회계연도 (PK-1)',
    month_val          STRING NOT NULL COMMENT '월 (PK-2)',
    acct_cd            STRING NOT NULL COMMENT '계정코드 (PK-3)',
    attr_dept_cd       STRING NOT NULL COMMENT '귀속부서코드 (PK-4)',
    cat_1              STRING NOT NULL COMMENT '구분1 (PK-5)',
    acct_nm            STRING          COMMENT '계정명',
    biz_unit           STRING          COMMENT '사업부',
    yr_int_acct        STRING          COMMENT '연_통합계정',
    cat_2              STRING          COMMENT '구분2',
    cat_3              STRING          COMMENT '구분3',
    tot_cost_acct      STRING          COMMENT '총비용계정',
    summary_desc       STRING          COMMENT '적요',
    final_amt          DECIMAL(18,2)   COMMENT '최종금액',
    mgt_dept_code      STRING          COMMENT '관리부서',
    _ingest_at         TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_acc_erp_mgt_cost PRIMARY KEY (fisc_year, month_val, acct_cd, attr_dept_cd, cat_1)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '관리 부서별 정제된 ERP 관리 비용 데이터'
""")
print("✓ DDL: acc_erp_mgt_cost")

# COMMAND ----------

# DBTITLE 1,DDL 17. acc_mfg_gp_intg (제조매출원가+품목별이익 통합)
# 17. [통합 실버] 제조매출원가+품목별매출이익 통합 — 브론즈 48번(acc_mfg_cogs) + 51번(acc_mfg_item_gp)
spark.sql(f"DROP TABLE IF EXISTS {S}.acc_mfg_gp_intg")
spark.sql(f"""
CREATE TABLE {S}.acc_mfg_gp_intg (
    fisc_year      STRING NOT NULL COMMENT '회계연도 (PK-1)',
    data_type      STRING NOT NULL COMMENT '데이터구분 (COGS:매출원가 / ITEM_GP:품목별이익) (PK-2)',
    cat_1          STRING NOT NULL COMMENT '구분1 (PK-3)',
    cat_2          STRING          COMMENT '구분2',
    m01 DECIMAL(18,2) COMMENT '1월',
    m02 DECIMAL(18,2) COMMENT '2월',
    m03 DECIMAL(18,2) COMMENT '3월',
    m04 DECIMAL(18,2) COMMENT '4월',
    m05 DECIMAL(18,2) COMMENT '5월',
    m06 DECIMAL(18,2) COMMENT '6월',
    m07 DECIMAL(18,2) COMMENT '7월',
    m08 DECIMAL(18,2) COMMENT '8월',
    m09 DECIMAL(18,2) COMMENT '9월',
    m10 DECIMAL(18,2) COMMENT '10월',
    m11 DECIMAL(18,2) COMMENT '11월',
    m12 DECIMAL(18,2) COMMENT '12월',
    total_amount   DECIMAL(18,2)   COMMENT '합계',
    _ingest_at     TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_mfg_cogs_gp PRIMARY KEY (fisc_year, data_type, cat_1)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '제조매출원가 및 품목별 매출이익 통합 테이블'
""")
print("✓ DDL: acc_mfg_gp_intg")

# COMMAND ----------

# DBTITLE 1,DDL 18. acc_mfg_inv_intg (제조비용+재고효과 통합)
# 18. [통합 실버] 제조비용+재고효과 통합 — 브론즈 44~47, 49, 50번(제조비용) + 33~35번(재고효과)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.acc_mfg_inv_intg (
    fisc_year      STRING NOT NULL COMMENT '회계연도 (PK-1)',
    source_type    STRING NOT NULL COMMENT '데이터 출처 (MFG_TOT/MFG_ENC/.../INV_GPU/INV_GSS/INV_SCR) (PK-2)',
    cat_1          STRING NOT NULL COMMENT '구분1 (PK-3)',
    cat_2          STRING NOT NULL COMMENT '구분2 (PK-4)',
    cat_3          STRING NOT NULL COMMENT '구분3 (PK-5)',
    sum_cat        STRING          COMMENT '요약구분 (재고효과 전용)',
    attr_cat       STRING          COMMENT '귀속구분 (재고효과 전용)',
    m01 DECIMAL(18,2) COMMENT '1월',
    m02 DECIMAL(18,2) COMMENT '2월',
    m03 DECIMAL(18,2) COMMENT '3월',
    m04 DECIMAL(18,2) COMMENT '4월',
    m05 DECIMAL(18,2) COMMENT '5월',
    m06 DECIMAL(18,2) COMMENT '6월',
    m07 DECIMAL(18,2) COMMENT '7월',
    m08 DECIMAL(18,2) COMMENT '8월',
    m09 DECIMAL(18,2) COMMENT '9월',
    m10 DECIMAL(18,2) COMMENT '10월',
    m11 DECIMAL(18,2) COMMENT '11월',
    m12 DECIMAL(18,2) COMMENT '12월',
    total_amount   DECIMAL(18,2)   COMMENT '합계',
    _ingest_at     TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_mfg_inv_eff PRIMARY KEY (fisc_year, source_type, cat_1, cat_2, cat_3)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '전체 발생기준 제조비용 및 사업부별 재고효과 통합 관리 테이블'
""")
print("✓ DDL: acc_mfg_inv_intg")

# COMMAND ----------

# DBTITLE 1,DDL 19. setl_sls_mat_dtl (결산_매출+재료비계획_상세)
# 19. [실버] 결산_매출+재료비계획_상세 통합 — 브론즈 1.4(setl_sls_pln_bu_pg) + 1.38(pln_mat_cst_bu_prd)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.setl_sls_mat_dtl (
    fisc_year      STRING NOT NULL COMMENT '회계연도 (PK-1)',
    plan_type      STRING NOT NULL COMMENT '계획유형 (SALES:매출 / MAT_COST:재료비) (PK-2)',
    cat_1          STRING NOT NULL COMMENT '구분1 (사업부) (PK-3)',
    cat_2          STRING NOT NULL COMMENT '구분2 (제품군) (PK-4)',
    cat_3          STRING NOT NULL COMMENT '구분3 (상세분류) (PK-5)',
    m01 DECIMAL(18,2) COMMENT '1월',
    m02 DECIMAL(18,2) COMMENT '2월',
    m03 DECIMAL(18,2) COMMENT '3월',
    m04 DECIMAL(18,2) COMMENT '4월',
    m05 DECIMAL(18,2) COMMENT '5월',
    m06 DECIMAL(18,2) COMMENT '6월',
    m07 DECIMAL(18,2) COMMENT '7월',
    m08 DECIMAL(18,2) COMMENT '8월',
    m09 DECIMAL(18,2) COMMENT '9월',
    m10 DECIMAL(18,2) COMMENT '10월',
    m11 DECIMAL(18,2) COMMENT '11월',
    m12 DECIMAL(18,2) COMMENT '12월',
    total_amount   DECIMAL(18,2)   COMMENT '합계',
    ratio_pct      DECIMAL(5,2)    COMMENT '비율(%)',
    _ingest_at     TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_sales_mat_plan_detail PRIMARY KEY (fisc_year, plan_type, cat_1, cat_2, cat_3)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '매출 및 재료비 상세 계획 통합 (정제)'
""")
print("✓ DDL: setl_sls_mat_dtl")

# COMMAND ----------

# DBTITLE 1,DDL 20. setl_plan_detail_clean (결산_계획_상세_원장)
# 20. [실버] 결산_계획_상세_원장 — 브론즈 1.2(setl_plan_xl) 정제
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.setl_plan_detail_clean (
    fisc_year       STRING NOT NULL COMMENT '회계연도 (PK-1)',
    plan_month      STRING NOT NULL COMMENT '계획월 (PK-2)',
    seq_no          STRING NOT NULL COMMENT '일련번호 (PK-3)',
    account_code    STRING NOT NULL COMMENT '계정코드 (FK)',
    account_name    STRING          COMMENT '계정명',
    attr_dept_cd    STRING          COMMENT '귀속부서코드 (FK)',
    slip_dept_cd    STRING          COMMENT '기표부서코드 (FK)',
    hq_name         STRING          COMMENT '본부',
    budget_amount   DECIMAL(18,2)   COMMENT '예산금액',
    final_amount    DECIMAL(18,2)   COMMENT '최종금액',
    direct_indirect STRING          COMMENT '직접/간접',
    pl_cat          STRING          COMMENT '손익구분',
    description     STRING          COMMENT '적요',
    _ingest_at      TIMESTAMP COMMENT '실버 적재 일시',
    CONSTRAINT pk_setl_plan_detail PRIMARY KEY (fisc_year, plan_month, seq_no)
) USING DELTA
PARTITIONED BY (fisc_year)
COMMENT '결산 상세 계획 원장 (정제)'
""")
print("✓ DDL: setl_plan_detail_clean")

# COMMAND ----------

# DBTITLE 1,DML 09. acc_bu_pl_mth (결산_사업부손익_통합 적재)
# 9. [실버 DML] 결산_사업부손익_통합 적재 — 브론즈 14~25번(1월~12월) 통합
month_sources = [
    ('01', 'acc_bu_pl_jan'), ('02', 'acc_bu_pl_feb'), ('03', 'acc_bu_pl_mar'), ('04', 'acc_bu_pl_apr'),
    ('05', 'acc_bu_pl_may'), ('06', 'acc_bu_pl_jun'), ('07', 'acc_bu_pl_jul'), ('08', 'acc_bu_pl_aug'),
    ('09', 'acc_bu_pl_sep'), ('10', 'acc_bu_pl_oct'), ('11', 'acc_bu_pl_nov'), ('12', 'acc_bu_pl_dec')
]

union_sql = "\nUNION ALL\n".join([
    f"""
    SELECT 
        fisc_year
        , '{mm}' AS month_val
        , cat_1
        , cat_2
        , TRY_CAST(REPLACE(gss_amount, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(gpu_amount, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(scr_amount, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(enc_amount, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(bpc_amount, ',', '') AS DECIMAL(18,2))
        , current_timestamp()
    FROM {B}.{tbl}
    WHERE cat_1 IS NOT NULL AND cat_2 IS NOT NULL
    """
    for mm, tbl in month_sources
])

spark.sql(f"""
INSERT OVERWRITE {S}.acc_bu_pl_mth
{union_sql}
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_bu_pl_mth").collect()[0].c
print(f"✓ DML: acc_bu_pl_mth → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 10. acc_alloc_master (결산_배부마스터 적재)
# 10. [실버 DML] 결산_배부마스터 적재
spark.sql(f"""
INSERT OVERWRITE {S}.acc_alloc_master
SELECT 
    fisc_year
    , TRIM(dept_code) AS dept_code
    , category
    , dept_name AS dept_nm
    , TRIM(macro_team_system) AS macro_team_system
    , TRIM(recogn_team) AS recogn_team
    , TRIM(recogn_dept) AS recogn_dept
    , TRIM(pl_hq_1) AS pl_hq
    , hq_name AS hq_nm
    , biz_unit_name AS biz_unit_nm
    , mfg_sgna_type
    , dir_indir_type
    , alloc_type
    , CASE WHEN REPLACE(gss_alloc, '%', '') RLIKE '^[0-9.]+$' 
         THEN CAST(REPLACE(gss_alloc, '%', '') AS DECIMAL(7,4)) / 100 ELSE 0 END AS gss_alloc_rate
    , CASE WHEN REPLACE(gpu_alloc, '%', '') RLIKE '^[0-9.]+$' 
         THEN CAST(REPLACE(gpu_alloc, '%', '') AS DECIMAL(7,4)) / 100 ELSE 0 END AS gpu_alloc_rate
    , CASE WHEN REPLACE(scr_alloc, '%', '') RLIKE '^[0-9.]+$' 
         THEN CAST(REPLACE(scr_alloc, '%', '') AS DECIMAL(7,4)) / 100 ELSE 0 END AS scr_alloc_rate
    , CASE WHEN REPLACE(enc_alloc, '%', '') RLIKE '^[0-9.]+$' 
         THEN CAST(REPLACE(enc_alloc, '%', '') AS DECIMAL(7,4)) / 100 ELSE 0 END AS enc_alloc_rate
    , CASE WHEN REPLACE(epc_alloc, '%', '') RLIKE '^[0-9.]+$' 
         THEN CAST(REPLACE(epc_alloc, '%', '') AS DECIMAL(7,4)) / 100 ELSE 0 END AS epc_alloc_rate
    , current_timestamp() AS _ingest_at
FROM {B}.acc_alloc_mst
WHERE dept_code IS NOT NULL AND category IS NOT NULL AND fisc_year IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_alloc_master").collect()[0].c
print(f"✓ DML: acc_alloc_master → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 11. acc_alloc_criteria_val (결산_배부기준값_통합 적재)
# 11. [실버 DML] 결산_배부기준값_통합 적재 (6개 브론즈 UNION ALL)
spark.sql(f"""
INSERT OVERWRITE {S}.acc_alloc_criteria_val
SELECT fisc_year, '구매팀사업별업무인원' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END AS gss_val
    , CASE WHEN REPLACE(gpu_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END AS gpu_val
    , CASE WHEN REPLACE(scr_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END AS scr_val
    , CASE WHEN REPLACE(enc_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(enc_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END AS enc_val
    , CASE WHEN REPLACE(epc_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(epc_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END AS epc_val
    , NULL AS remarks, current_timestamp() AS _ingest_at
FROM {B}.acc_alloc_pur_task
WHERE category IS NOT NULL
UNION ALL
SELECT fisc_year, '매출계획' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(gpu_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(scr_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(enc_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(enc_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(epc_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(epc_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , remarks, current_timestamp()
FROM {B}.acc_alloc_sales_plan
WHERE category IS NOT NULL
UNION ALL
SELECT fisc_year, '시스템사업부인원' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(gpu_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(scr_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , 0 AS enc_val, 0 AS epc_val
    , remarks, current_timestamp()
FROM {B}.acc_alloc_sys_hc
WHERE category IS NOT NULL
UNION ALL
SELECT fisc_year, '연구소' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_value, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_value, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(gpu_value, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_value, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(scr_value, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_value, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(enc_value, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(enc_value, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(epc_value, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(epc_value, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , remarks, current_timestamp()
FROM {B}.acc_alloc_rnd
WHERE category IS NOT NULL
UNION ALL
SELECT fisc_year, '인원계획' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(gpu_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(scr_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(enc_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(enc_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(epc_hc, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(epc_hc, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , remarks, current_timestamp()
FROM {B}.acc_alloc_hc_plan
WHERE category IS NOT NULL
UNION ALL
SELECT fisc_year, '재료비_참고용' AS criteria_tp, TRIM(category) AS category
    , CASE WHEN REPLACE(gss_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gss_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(gpu_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(gpu_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(scr_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(scr_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(enc_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(enc_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , CASE WHEN REPLACE(epc_amount, ',', '') RLIKE '^-?[0-9.]+$' THEN CAST(REPLACE(epc_amount, ',', '') AS DECIMAL(18,2)) ELSE 0 END
    , remarks, current_timestamp()
FROM {B}.acc_alloc_mat_cost_ref
WHERE category IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_alloc_criteria_val").collect()[0].c
print(f"✓ DML: acc_alloc_criteria_val → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 12. acc_vf_cost_standard (결산_변고기준마스터 적재)
# 12. [실버 DML] 결산_변고기준마스터 적재
# 브론즈 acc_vf_cost_std + 실버 bgt_dept_master JOIN
spark.sql(f"""
INSERT OVERWRITE {S}.acc_vf_cost_standard
SELECT 
    a.fisc_year
    , TRIM(a.account_code) AS acct_code
    , b.dept_cd AS dept_code
    , a.macro_team_system
    , a.expense_type AS expense_tp
    , a.integrated_account AS integrated_acct
    , a.large_category
    , a.var_fixed_type AS vf_type
    , current_timestamp() AS _ingest_at
FROM {B}.acc_vf_cost_std a
INNER JOIN {S}.bgt_dept_master b 
    ON a.fisc_year = b.plan_year
    AND TRIM(a.dept_name) = TRIM(b.dept_nm)
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_vf_cost_standard").collect()[0].c
print(f"✓ DML: acc_vf_cost_standard → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 13. acc_vf_cost_revision (결산_변고검토 적재)
# 13. [실버 DML] 결산_변고검토 적재
# 브론즈 acc_vf_cost_rev + 실버 bgt_dept_master JOIN
spark.sql(f"""
INSERT OVERWRITE {S}.acc_vf_cost_revision
SELECT 
    a.fisc_year
    , TRIM(a.account_code) AS acct_code
    , b.dept_cd AS dept_code
    , a.expense_type AS expense_tp
    , a.integrated_account AS integrated_acct
    , a.large_category
    , a.var_fixed_type AS vf_type
    , current_timestamp() AS _ingest_at
FROM {B}.acc_vf_cost_rev a
INNER JOIN {S}.bgt_dept_master b 
    ON a.fisc_year = b.plan_year
    AND TRIM(a.dept_name) = TRIM(b.dept_nm)
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_vf_cost_revision").collect()[0].c
print(f"✓ DML: acc_vf_cost_revision → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 14. acc_general_standard (결산_일반기준 적재)
# 14. [실버 DML] 결산_일반기준 적재 (3개 브론즈 통합)
spark.sql(f"""
INSERT OVERWRITE {S}.acc_general_standard
SELECT 
    fisc_year
    , '부서구분' AS std_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , current_timestamp() AS _ingest_at
FROM {B}.acc_gen_std_dept_cat
UNION ALL
SELECT 
    fisc_year
    , '원장조회' AS std_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , current_timestamp()
FROM {B}.acc_gen_std_ledger
UNION ALL
SELECT 
    fisc_year
    , '직간접구분' AS std_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , current_timestamp()
FROM {B}.acc_gen_std_dir_indir
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_general_standard").collect()[0].c
print(f"✓ DML: acc_general_standard → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 15. acc_act_erp_gl (결산_실적_ERP원장 적재)
# 15. [실버 DML] 결산_실적_ERP원장 적재
spark.sql(f"""
INSERT OVERWRITE {S}.acc_act_erp_gl
SELECT 
    fisc_year
    , LPAD(month_val, 2, '0') AS month_val
    , TRIM(slip_no) AS slip_no
    , TRIM(slip_seq) AS slip_seq
    , TRIM(acct_code) AS acct_code
    , TRIM(dept_code) AS dept_code
    , TRIM(m_team) AS m_team
    , biz_unit
    , acct_nm
    , dept_name
    , curr_cd
    , dr_amount
    , cr_amount
    , CAST(REPLACE(dr_local_amount, ',', '') AS DECIMAL(18,2)) AS dr_local_amt
    , CAST(REPLACE(cr_local_amount, ',', '') AS DECIMAL(18,2)) AS cr_local_amt
    , slip_dt
    , rmk
    , current_timestamp() AS _ingest_at
FROM {B}.acc_act_erp_gl
WHERE slip_no IS NOT NULL AND acct_code IS NOT NULL AND dept_code IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_act_erp_gl").collect()[0].c
print(f"✓ DML: acc_act_erp_gl → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 16. acc_erp_mgt_cost (결산_인관전용_ERP 적재)
# 16. [실버 DML] 결산_인관전용_ERP 적재
spark.sql(f"""
INSERT OVERWRITE {S}.acc_erp_mgt_cost
SELECT 
    fisc_year
    , LPAD(month_val, 2, '0') AS month_val
    , TRIM(acct_cd) AS acct_cd
    , TRIM(mgt_dept_code) AS attr_dept_cd
    , TRIM(cat_1) AS cat_1
    , acct_nm
    , biz_unit
    , yr_int_acct
    , cat_2
    , cat_3
    , tot_cost_acct
    , summary_desc
    , TRY_CAST(REPLACE(final_amount, ',', '') AS DECIMAL(18,2)) AS final_amt
    , mgt_dept_code
    , current_timestamp() AS _ingest_at
FROM {B}.acc_erp_mgt_cost
WHERE acct_cd IS NOT NULL AND cat_1 IS NOT NULL AND mgt_dept_code IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_erp_mgt_cost").collect()[0].c
print(f"✓ DML: acc_erp_mgt_cost → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 17. acc_mfg_gp_intg (제조매출원가+품목별이익 적재)
# 17. [실버 DML] 제조매출원가+품목별매출이익 통합 적재
spark.sql(f"""
INSERT OVERWRITE {S}.acc_mfg_gp_intg
SELECT 
    fisc_year
    , 'COGS' AS data_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2))
    , current_timestamp() AS _ingest_at
FROM {B}.acc_mfg_cogs
WHERE cat_1 IS NOT NULL
UNION ALL
SELECT 
    fisc_year
    , 'ITEM_GP' AS data_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2))
    , current_timestamp()
FROM {B}.acc_mfg_item_gp
WHERE cat_1 IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_mfg_gp_intg").collect()[0].c
print(f"✓ DML: acc_mfg_gp_intg → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 18. acc_mfg_inv_intg (제조비용+재고효과 통합 적재)
# 18. [실버 DML] 제조비용+재고효과 통합 적재 (9개 브론즈 UNION ALL)
mfg_sources = [
    ('MFG_TOT', 'acc_mfg_cost_tot_acr'),
    ('MFG_ENC', 'acc_mfg_cost_enc_acr'),
    ('MFG_EPC', 'acc_mfg_cost_epc_acr'),
    ('MFG_GPU', 'acc_mfg_cost_gpu_acr'),
    ('MFG_GSS', 'acc_mfg_cost_gss_acr'),
    ('MFG_SCR', 'acc_mfg_cost_scr_acr')
]

inv_sources = [
    ('INV_GPU', 'acc_inv_eff_gpu'),
    ('INV_GSS', 'acc_inv_eff_gss'),
    ('INV_SCR', 'acc_inv_eff_scr')
]

# 제조비용 부분 (cat_3 포함, sum_cat/attr_cat = NULL)
mfg_sql = "\nUNION ALL\n".join([
    f"""
    SELECT fisc_year, '{st}' AS source_type
        , TRIM(cat_1) AS cat_1, TRIM(cat_2) AS cat_2, COALESCE(TRIM(cat_3), '') AS cat_3
        , NULL AS sum_cat, NULL AS attr_cat
        , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2))
        , current_timestamp()
    FROM {B}.{tbl}
    WHERE cat_1 IS NOT NULL AND cat_2 IS NOT NULL
    """
    for st, tbl in mfg_sources
])

# 재고효과 부분 (sum_cat/attr_cat 포함)
inv_sql = "\nUNION ALL\n".join([
    f"""
    SELECT fisc_year, '{st}' AS source_type
        , TRIM(cat_1) AS cat_1, TRIM(cat_2) AS cat_2, COALESCE(TRIM(cat_3), '') AS cat_3
        , TRIM(sum_cat) AS sum_cat, TRIM(attr_cat) AS attr_cat
        , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
        , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2))
        , current_timestamp()
    FROM {B}.{tbl}
    WHERE cat_1 IS NOT NULL AND cat_2 IS NOT NULL
    """
    for st, tbl in inv_sources
])

spark.sql(f"""
INSERT OVERWRITE {S}.acc_mfg_inv_intg
{mfg_sql}
UNION ALL
{inv_sql}
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.acc_mfg_inv_intg").collect()[0].c
print(f"✓ DML: acc_mfg_inv_intg → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 19. setl_sls_mat_dtl (결산_매출+재료비계획 적재)
# 19. [실버 DML] 결산_매출+재료비계획_상세 통합 적재
spark.sql(f"""
INSERT OVERWRITE {S}.setl_sls_mat_dtl
-- A. 매출계획 사업부/제품군 상세
SELECT 
    fisc_year
    , 'SALES' AS plan_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , COALESCE(TRIM(cat_3), '') AS cat_3
    , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2)) AS total_amount
    , TRY_CAST(ratio_pct AS DECIMAL(5,2)) AS ratio_pct
    , current_timestamp() AS _ingest_at
FROM {B}.setl_sls_pln_bu_pg
WHERE cat_1 IS NOT NULL AND cat_2 IS NOT NULL
UNION ALL
-- B. 재료비계획 사업부/제품군 상세
SELECT 
    fisc_year
    , 'MAT_COST' AS plan_type
    , TRIM(cat_1) AS cat_1
    , TRIM(cat_2) AS cat_2
    , COALESCE(TRIM(cat_3), '') AS cat_3
    , TRY_CAST(REPLACE(m01, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m02, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m03, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m04, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m05, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m06, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m07, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m08, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m09, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m10, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(m11, ',', '') AS DECIMAL(18,2)), TRY_CAST(REPLACE(m12, ',', '') AS DECIMAL(18,2))
    , TRY_CAST(REPLACE(total_amount, ',', '') AS DECIMAL(18,2)) AS total_amount
    , TRY_CAST(ratio_pct AS DECIMAL(5,2)) AS ratio_pct
    , current_timestamp()
FROM {B}.pln_mat_cst_bu_prd
WHERE cat_1 IS NOT NULL AND cat_2 IS NOT NULL
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.setl_sls_mat_dtl").collect()[0].c
print(f"✓ DML: setl_sls_mat_dtl → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,DML 20. setl_plan_detail_clean (결산_계획_상세_원장 적재)
# 20. [실버 DML] 결산_계획_상세_원장 적재
spark.sql(f"""
INSERT OVERWRITE {S}.setl_plan_detail_clean
SELECT 
    fisc_year
    , LPAD(plan_month, 2, '0') AS plan_month
    , seq_no
    , TRIM(account_code) AS account_code
    , account_name
    , attr_dept AS attr_dept_cd
    , slip_dept AS slip_dept_cd
    , hq_name
    , CAST(REPLACE(budget_amount, ',', '') AS DECIMAL(18,2)) AS budget_amount
    , CAST(REPLACE(final_amount, ',', '') AS DECIMAL(18,2)) AS final_amount
    , direct_indirect
    , pl_cat
    , description
    , current_timestamp() AS _ingest_at
FROM {B}.setl_plan_xl
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {S}.setl_plan_detail_clean").collect()[0].c
print(f"✓ DML: setl_plan_detail_clean → {cnt:,}건")

# COMMAND ----------

# DBTITLE 1,Cell 13 최종 확인 설명
# MAGIC %md
# MAGIC ## 3. 최종 확인

# COMMAND ----------

# DBTITLE 1,Cell 14 최종 확인
print(f"=== {CATALOG}.{SILVER_SCHEMA} 실버 테이블 현황 ===\n")
tables = [
    t.tableName for t in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SILVER_SCHEMA}`").collect()
    if not t.isTemporary
]
total = 0
for tbl in sorted(tables):
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM `{CATALOG}`.`{SILVER_SCHEMA}`.`{tbl}`").collect()[0].c
    total += cnt
    print(f"  {tbl:<40} {cnt:>8,}건")
print(f"\n  {'[ TOTAL ]':<40} {total:>8,}건")
print(f"\n  테이블 수: {len(tables)}개  |  실버 레이어 구축 완료 ✓")

# COMMAND ----------

# DBTITLE 1,Cell 14 최종 확인
print(f"=== {CATALOG}.{SILVER_SCHEMA} 실버 테이블 현황 ===\n")
tables = [
    t.tableName for t in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SILVER_SCHEMA}`").collect()
    if not t.isTemporary
]
total = 0
for tbl in sorted(tables):
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM `{CATALOG}`.`{SILVER_SCHEMA}`.`{tbl}`").collect()[0].c
    total += cnt
    print(f"  {tbl:<40} {cnt:>8,}건")
print(f"\n  {'[ TOTAL ]':<40} {total:>8,}건")
print(f"\n  테이블 수: {len(tables)}개  |  실버 레이어 구축 완료 ✓")