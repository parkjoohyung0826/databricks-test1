# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer DDL 개요
# MAGIC %md
# MAGIC # 예산/결산 PoC — Bronze Layer DDL 설정
# MAGIC 브론즈: `wonik_poc.wonik_test1_bronze`  
# MAGIC 열 64개 테이블 DDL (결산 51개 + 예산 13개)

# COMMAND ----------

# DBTITLE 1,Cell 2 설정 + 스키마
CATALOG = "wonik_poc"
BRONZE  = "wonik_test1_bronze"
B = f"{CATALOG}.{BRONZE}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{BRONZE}`")
print(f"✓ 스키마: {CATALOG}.{BRONZE}")
print(f"  B = {B}")

# COMMAND ----------

# DBTITLE 1,[TEMP] B override to wonik_ddl_ref
# [TEMP] B를 임시 비교 스키마로 오버라이드 (사용 후 삭제)
spark.sql("CREATE SCHEMA IF NOT EXISTS `wonik_poc`.`wonik_ddl_ref`")
B = "wonik_poc.wonik_ddl_ref"
print(f"[TEMP] B = {B}")

# COMMAND ----------

# DBTITLE 1,Cell 3 setl_summary_pl + setl_plan_xl
# ── 1. setl_summary_pl ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.setl_summary_pl (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
    comp_annual_plan STRING COMMENT '전사_연간계획',
    comp_annual_plan_pct STRING COMMENT '전사_연간계획_비율',
    comp_acc_plan STRING COMMENT '전사_누적계획',
    comp_acc_plan_pct STRING COMMENT '전사_누적계획_비율',
    comp_acc_actual STRING COMMENT '전사_누적실적',
    comp_acc_actual_pct STRING COMMENT '전사_누적실적_비율',
    comp_diff STRING COMMENT '전사_차이',
    comp_diff_pp STRING COMMENT '전사_차이_비율(p)',
    sys_hq_annual_plan STRING COMMENT 'System본부_연간계획',
    sys_hq_annual_plan_pct STRING COMMENT 'System본부_연간계획_비율',
    sys_hq_m01_plan STRING COMMENT 'System본부_1월계획', sys_hq_m01_actual STRING COMMENT 'System본부_1월실적',
    sys_hq_m02_plan STRING COMMENT 'System본부_2월계획', sys_hq_m02_actual STRING COMMENT 'System본부_2월실적',
    sys_hq_m03_plan STRING COMMENT 'System본부_3월계획', sys_hq_m03_actual STRING COMMENT 'System본부_3월실적',
    sys_hq_m04_plan STRING COMMENT 'System본부_4월계획', sys_hq_m04_actual STRING COMMENT 'System본부_4월실적',
    sys_hq_m05_plan STRING COMMENT 'System본부_5월계획', sys_hq_m05_actual STRING COMMENT 'System본부_5월실적',
    sys_hq_m06_plan STRING COMMENT 'System본부_6월계획', sys_hq_m06_actual STRING COMMENT 'System본부_6월실적',
    sys_hq_m07_plan STRING COMMENT 'System본부_7월계획', sys_hq_m07_actual STRING COMMENT 'System본부_7월실적',
    sys_hq_m08_plan STRING COMMENT 'System본부_8월계획', sys_hq_m08_actual STRING COMMENT 'System본부_8월실적',
    sys_hq_m09_plan STRING COMMENT 'System본부_9월계획', sys_hq_m09_actual STRING COMMENT 'System본부_9월실적',
    sys_hq_m10_plan STRING COMMENT 'System본부_10월계획', sys_hq_m10_actual STRING COMMENT 'System본부_10월실적',
    sys_hq_m11_plan STRING COMMENT 'System본부_11월계획', sys_hq_m11_actual STRING COMMENT 'System본부_11월실적',
    sys_hq_m12_plan STRING COMMENT 'System본부_12월계획', sys_hq_m12_actual STRING COMMENT 'System본부_12월실적',
    sys_hq_acc_plan STRING COMMENT 'System본부_누적계획',
    sys_hq_acc_plan_pct STRING COMMENT 'System본부_누적계획_비율',
    sys_hq_acc_actual STRING COMMENT 'System본부_누적실적',
    sys_hq_acc_actual_pct STRING COMMENT 'System본부_누적실적율_비율',
    sys_hq_diff STRING COMMENT 'System본부_차이',
    sys_hq_diff_pp STRING COMMENT 'System본부_차이_비율(p)',
    gss_annual_plan STRING COMMENT 'GSS_연간계획', gss_annual_plan_pct STRING COMMENT 'GSS_연간계획_비율',
    gss_acc_plan STRING COMMENT 'GSS_누적계획', gss_acc_plan_pct STRING COMMENT 'GSS_누적계획_비율',
    gss_acc_actual STRING COMMENT 'GSS_누적실적', gss_acc_actual_pct STRING COMMENT 'GSS_누적실적_비율',
    gss_diff STRING COMMENT 'GSS_차이', gss_diff_pp STRING COMMENT 'GSS_차이_비율(p)',
    gpu_annual_plan STRING COMMENT 'GPU_연간계획', gpu_annual_plan_pct STRING COMMENT 'GPU_연간계획_비율',
    gpu_acc_plan STRING COMMENT 'GPU_누적계획', gpu_acc_plan_pct STRING COMMENT 'GPU_누적계획_비율',
    gpu_acc_actual STRING COMMENT 'GPU_누적실적', gpu_acc_actual_pct STRING COMMENT 'GPU_누적실적_비율',
    gpu_diff STRING COMMENT 'GPU_차이', gpu_diff_pp STRING COMMENT 'GPU_차이_비율(p)',
    scr_annual_plan STRING COMMENT 'SCR_연간계획', scr_annual_plan_pct STRING COMMENT 'SCR_연간계획_비율',
    scr_acc_plan STRING COMMENT 'SCR_누적계획', scr_acc_plan_pct STRING COMMENT 'SCR_누적계획_비율',
    scr_acc_actual STRING COMMENT 'SCR_누적실적', scr_acc_actual_pct STRING COMMENT 'SCR_누적실적_비율',
    scr_diff STRING COMMENT 'SCR_차이', scr_diff_pp STRING COMMENT 'SCR_차이_비율(p)',
    epc_hq_annual_plan STRING COMMENT 'EPC본부_연간계획',
    epc_hq_annual_plan_pct STRING COMMENT 'EPC본부_연간계획_비율',
    epc_hq_m01_plan STRING COMMENT 'EPC본부_1월계획', epc_hq_m01_actual STRING COMMENT 'EPC본부_1월실적',
    epc_hq_m02_plan STRING COMMENT 'EPC본부_2월계획', epc_hq_m02_actual STRING COMMENT 'EPC본부_2월실적',
    epc_hq_m03_plan STRING COMMENT 'EPC본부_3월계획', epc_hq_m03_actual STRING COMMENT 'EPC본부_3월실적',
    epc_hq_m04_plan STRING COMMENT 'EPC본부_4월계획', epc_hq_m04_actual STRING COMMENT 'EPC본부_4월실적',
    epc_hq_m05_plan STRING COMMENT 'EPC본부_5월계획', epc_hq_m05_actual STRING COMMENT 'EPC본부_5월실적',
    epc_hq_m06_plan STRING COMMENT 'EPC본부_6월계획', epc_hq_m06_actual STRING COMMENT 'EPC본부_6월실적',
    epc_hq_m07_plan STRING COMMENT 'EPC본부_7월계획', epc_hq_m07_actual STRING COMMENT 'EPC본부_7월실적',
    epc_hq_m08_plan STRING COMMENT 'EPC본부_8월계획', epc_hq_m08_actual STRING COMMENT 'EPC본부_8월실적',
    epc_hq_m09_plan STRING COMMENT 'EPC본부_9월계획', epc_hq_m09_actual STRING COMMENT 'EPC본부_9월실적',
    epc_hq_m10_plan STRING COMMENT 'EPC본부_10월계획', epc_hq_m10_actual STRING COMMENT 'EPC본부_10월실적',
    epc_hq_m11_plan STRING COMMENT 'EPC본부_11월계획', epc_hq_m11_actual STRING COMMENT 'EPC본부_11월실적',
    epc_hq_m12_plan STRING COMMENT 'EPC본부_12월계획', epc_hq_m12_actual STRING COMMENT 'EPC본부_12월실적',
    epc_hq_acc_plan STRING COMMENT 'EPC본부_누적계획',
    epc_hq_acc_plan_pct STRING COMMENT 'EPC본부_누적계획_비율',
    epc_hq_acc_actual STRING COMMENT 'EPC본부_누적실적',
    epc_hq_acc_actual_pct STRING COMMENT 'EPC본부_누적실적_비율',
    epc_hq_diff STRING COMMENT 'EPC본부_차이', epc_hq_diff_pp STRING COMMENT 'EPC본부_차이_비율(p)',
    enc_annual_plan STRING COMMENT 'ENC_연간계획', enc_annual_plan_pct STRING COMMENT 'ENC_연간계획_비율',
    enc_m01_plan STRING COMMENT 'ENC_1월계획', enc_m01_actual STRING COMMENT 'ENC_1월실적',
    enc_m02_plan STRING COMMENT 'ENC_2월계획', enc_m02_actual STRING COMMENT 'ENC_2월실적',
    enc_m03_plan STRING COMMENT 'ENC_3월계획', enc_m03_actual STRING COMMENT 'ENC_3월실적',
    enc_m04_plan STRING COMMENT 'ENC_4월계획', enc_m04_actual STRING COMMENT 'ENC_4월실적',
    enc_m05_plan STRING COMMENT 'ENC_5월계획', enc_m05_actual STRING COMMENT 'ENC_5월실적',
    enc_m06_plan STRING COMMENT 'ENC_6월계획', enc_m06_actual STRING COMMENT 'ENC_6월실적',
    enc_m07_plan STRING COMMENT 'ENC_7월계획', enc_m07_actual STRING COMMENT 'ENC_7월실적',
    enc_m08_plan STRING COMMENT 'ENC_8월계획', enc_m08_actual STRING COMMENT 'ENC_8월실적',
    enc_m09_plan STRING COMMENT 'ENC_9월계획', enc_m09_actual STRING COMMENT 'ENC_9월실적',
    enc_m10_plan STRING COMMENT 'ENC_10월계획', enc_m10_actual STRING COMMENT 'ENC_10월실적',
    enc_m11_plan STRING COMMENT 'ENC_11월계획', enc_m11_actual STRING COMMENT 'ENC_11월실적',
    enc_m12_plan STRING COMMENT 'ENC_12월계획', enc_m12_actual STRING COMMENT 'ENC_12월실적',
    enc_acc_plan STRING COMMENT 'ENC_누적계획', enc_acc_plan_pct STRING COMMENT 'ENC_누적계획_비율',
    enc_acc_actual STRING COMMENT 'ENC_누적실적', enc_acc_actual_pct STRING COMMENT 'ENC_누적실적_비율',
    enc_diff STRING COMMENT 'ENC_차이', enc_diff_pp STRING COMMENT 'ENC_차이_비율(p)',
    epc_annual_plan STRING COMMENT 'EPC_연간계획', epc_annual_plan_pct STRING COMMENT 'EPC_연간계획_비율',
    epc_m01_plan STRING COMMENT 'EPC_1월계획', epc_m01_actual STRING COMMENT 'EPC_1월실적',
    epc_m02_plan STRING COMMENT 'EPC_2월계획', epc_m02_actual STRING COMMENT 'EPC_2월실적',
    epc_m03_plan STRING COMMENT 'EPC_3월계획', epc_m03_actual STRING COMMENT 'EPC_3월실적',
    epc_m04_plan STRING COMMENT 'EPC_4월계획', epc_m04_actual STRING COMMENT 'EPC_4월실적',
    epc_m05_plan STRING COMMENT 'EPC_5월계획', epc_m05_actual STRING COMMENT 'EPC_5월실적',
    epc_m06_plan STRING COMMENT 'EPC_6월계획', epc_m06_actual STRING COMMENT 'EPC_6월실적',
    epc_m07_plan STRING COMMENT 'EPC_7월계획', epc_m07_actual STRING COMMENT 'EPC_7월실적',
    epc_m08_plan STRING COMMENT 'EPC_8월계획', epc_m08_actual STRING COMMENT 'EPC_8월실적',
    epc_m09_plan STRING COMMENT 'EPC_9월계획', epc_m09_actual STRING COMMENT 'EPC_9월실적',
    epc_m10_plan STRING COMMENT 'EPC_10월계획', epc_m10_actual STRING COMMENT 'EPC_10월실적',
    epc_m11_plan STRING COMMENT 'EPC_11월계획', epc_m11_actual STRING COMMENT 'EPC_11월실적',
    epc_m12_plan STRING COMMENT 'EPC_12월계획', epc_m12_actual STRING COMMENT 'EPC_12월실적',
    epc_acc_plan STRING COMMENT 'EPC_누적계획', epc_acc_plan_pct STRING COMMENT 'EPC_누적계획_비율',
    epc_acc_actual STRING COMMENT 'EPC_누적실적', epc_acc_actual_pct STRING COMMENT 'EPC_누적실적_비율',
    epc_diff STRING COMMENT 'EPC_차이', epc_diff_pp STRING COMMENT 'EPC_차이_비율(p)',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (fisc_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ setl_summary_pl")

# ── 2. setl_plan_xl ──────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.setl_plan_xl (
    fisc_year STRING COMMENT '회계연도',
    seq_no STRING COMMENT 'No.', plan_month STRING COMMENT '월', cat STRING COMMENT '구분',
    annual_integrated_account STRING COMMENT '연_통합계정',
    total_cost_account STRING COMMENT '총비용계정',
    account_name STRING COMMENT '계정명', account_code STRING COMMENT '계정코드',
    description STRING COMMENT '적요',
    budget_amount STRING COMMENT '예산금액', final_amount STRING COMMENT '최종금액',
    slip_dept_y25m01 STRING COMMENT '기표부서_25.01',
    attr_dept_y25m01 STRING COMMENT '귀속부서_25.01',
    slip_dept STRING COMMENT '기표부서', attr_dept STRING COMMENT '귀속부서',
    hq_name STRING COMMENT '본부', direct_indirect STRING COMMENT '직접_간접',
    dept_section STRING COMMENT '계_부', report_variable_fixed STRING COMMENT '보고_변_고',
    outsourcing_payment STRING COMMENT '외주지급',
    gss_flag STRING COMMENT 'GSS', gpu_flag STRING COMMENT 'GPU',
    scr_flag STRING COMMENT 'SCR', enc_flag STRING COMMENT 'EnC', epc_flag STRING COMMENT 'EPC',
    gss_amount STRING COMMENT 'GSS금액', gpu_amount STRING COMMENT 'GPU금액',
    scr_amount STRING COMMENT 'SCR금액', enc_amount STRING COMMENT 'EnC금액',
    bpc_amount STRING COMMENT 'BPC금액', total_amount STRING COMMENT '합계',
    true_false_flag STRING COMMENT '참트루',
    pl_cat STRING COMMENT '손익구분', dir_ind_cat STRING COMMENT '직_간접',
    pl_account STRING COMMENT 'PL계정', pl_final_account STRING COMMENT 'PL최정계정',
    classification STRING COMMENT '분류', month_2 STRING COMMENT '월_2',
    sys_flag STRING COMMENT 'SYSTEM', infra_flag STRING COMMENT 'INFRA',
    verification STRING COMMENT '검증',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (fisc_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ setl_plan_xl")

# COMMAND ----------

# DBTITLE 1,Cell 4 매출계획+배부기준 (3-11)
# ── 공통 월별 DDL 헬퍼 ───────────────────────────────────────────────
MONTH_COLS = "\n".join([
    f"    m{i:02d} STRING COMMENT '{i}월'," for i in range(1, 13)
])
META_COLS = """    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'"""
TBLPROP = "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')"

# ── 3. setl_sales_plan_sys ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.setl_sales_plan_sys (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
{MONTH_COLS}
    total_amount STRING COMMENT '합계', ratio_pct STRING COMMENT '비율_%',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ setl_sales_plan_sys")

# ── 4. setl_sls_pln_bu_pg ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.setl_sls_pln_bu_pg (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
{MONTH_COLS}
    total_amount STRING COMMENT '합계', ratio_pct STRING COMMENT '비율_%',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ setl_sls_pln_bu_pg")

# ── 5. acc_alloc_pur_task ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_pur_task (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_hc STRING COMMENT 'GSS 인원수', gpu_hc STRING COMMENT 'GPU 인원수',
    scr_hc STRING COMMENT 'SCR 인원수', enc_hc STRING COMMENT 'EnC 인원수',
    epc_hc STRING COMMENT 'EPC 인원수', total_hc STRING COMMENT '계',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_pur_task")

# ── 6. acc_alloc_sales_plan ────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_sales_plan (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_amount STRING COMMENT 'GSS 매출액', gpu_amount STRING COMMENT 'GPU 매출액',
    scr_amount STRING COMMENT 'SCR 매출액', enc_amount STRING COMMENT 'EnC 매출액',
    epc_amount STRING COMMENT 'EPC 매출액', total_amount STRING COMMENT '계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_sales_plan")

# ── 7. acc_alloc_sys_hc ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_sys_hc (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_hc STRING COMMENT 'GSS 인원수', gpu_hc STRING COMMENT 'GPU 인원수',
    scr_hc STRING COMMENT 'SCR 인원수', total_hc STRING COMMENT '계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_sys_hc")

# ── 8. acc_alloc_rnd ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_rnd (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_value STRING COMMENT 'GSS 배부값', gpu_value STRING COMMENT 'GPU 배부값',
    scr_value STRING COMMENT 'SCR 배부값', enc_value STRING COMMENT 'EnC 배부값',
    epc_value STRING COMMENT 'EPC 배부값', total_value STRING COMMENT '계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_rnd")

# ── 9. acc_alloc_hc_plan ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_hc_plan (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_hc STRING COMMENT 'GSS 계획 인원', gpu_hc STRING COMMENT 'GPU 계획 인원',
    scr_hc STRING COMMENT 'SCR 계획 인원', enc_hc STRING COMMENT 'EnC 계획 인원',
    epc_hc STRING COMMENT 'EPC 계획 인원', total_hc STRING COMMENT '계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_hc_plan")

# ── 10. acc_alloc_mat_cost_ref ──────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_mat_cost_ref (
    fisc_year STRING COMMENT '회계연도', category STRING COMMENT '구분',
    gss_amount STRING COMMENT 'GSS 재료비', gpu_amount STRING COMMENT 'GPU 재료비',
    scr_amount STRING COMMENT 'SCR 재료비', enc_amount STRING COMMENT 'EnC 재료비',
    epc_amount STRING COMMENT 'EPC 재료비', total_amount STRING COMMENT '계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_mat_cost_ref")

# ── 11. acc_alloc_mst ──────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_alloc_mst (
    fisc_year STRING COMMENT '회계연도',
    dept_code STRING COMMENT '부서', dept_name STRING COMMENT '부서명',
    recogn_team STRING COMMENT '인식팀', recogn_dept STRING COMMENT '인식부서',
    category STRING COMMENT '구분', pl_hq_1 STRING COMMENT '손익본부1',
    gss_alloc STRING COMMENT 'GSS배부', gpu_alloc STRING COMMENT 'GPU배부',
    scr_alloc STRING COMMENT 'SCR배부', enc_alloc STRING COMMENT 'EnC배부',
    epc_alloc STRING COMMENT 'EPC배부',
    mfg_sgna_type STRING COMMENT '제_판', dir_indir_type STRING COMMENT '직_간',
    alloc_type STRING COMMENT '배부', legacy_type STRING COMMENT '기존',
    alloc_dir_charge STRING COMMENT '배부_직과', pjt_code STRING COMMENT 'PJT',
    pjt_detail_code STRING COMMENT '세부PJT', dept_code_2 STRING COMMENT '부서2',
    hq_name STRING COMMENT '본부', biz_unit_name STRING COMMENT '사업부',
    macro_team_system STRING COMMENT '대팀제', class_purpose STRING COMMENT '구분용',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_alloc_mst")

# COMMAND ----------

# DBTITLE 1,Cell 5 acc_vf_cost+acc_bu_pl 월별 루프 (12-26)
# ── 12. acc_vf_cost_rev ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_vf_cost_rev (
    fisc_year STRING COMMENT '회계연도',
    account_code STRING COMMENT '계정코드', account_dept_code STRING COMMENT '계정_부서',
    account_name STRING COMMENT '계정명', expense_type STRING COMMENT '비용구분',
    integrated_account STRING COMMENT '통합계정', large_category STRING COMMENT '대구분',
    dept_name STRING COMMENT '부서명', var_fixed_type STRING COMMENT '변_고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_vf_cost_rev")

# ── 13. acc_vf_cost_std ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_vf_cost_std (
    fisc_year STRING COMMENT '회계연도',
    account_code STRING COMMENT '계정코드', account_dept_code STRING COMMENT '계정_부서',
    account_name STRING COMMENT '계정명', expense_type STRING COMMENT '비용구분',
    integrated_account STRING COMMENT '통합계정', large_category STRING COMMENT '대구분',
    dept_name STRING COMMENT '부서명', var_fixed_type STRING COMMENT '변_고',
    macro_team_system STRING COMMENT '대팀제', int_acct_m_team STRING COMMENT '통합계정_대팀제',
    as_was_val STRING COMMENT 'AS_WAS', to_be_val STRING COMMENT 'TO_BE',
    diff_val STRING COMMENT '차이', remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_vf_cost_std")

# ── 14-26: acc_bu_pl_월별 + acc 루프 ───────────────────────────────────
BU_PL_MONTHLY_TABLES = [
    ('acc_bu_pl_oct','10월'), ('acc_bu_pl_nov','11월'), ('acc_bu_pl_dec','12월'),
    ('acc_bu_pl_jan','1월'),  ('acc_bu_pl_feb','2월'),  ('acc_bu_pl_mar','3월'),
    ('acc_bu_pl_apr','4월'),  ('acc_bu_pl_may','5월'),  ('acc_bu_pl_jun','6월'),
    ('acc_bu_pl_jul','7월'),  ('acc_bu_pl_aug','8월'),  ('acc_bu_pl_sep','9월'),
    ('acc_bu_pl_acc','누적'),
]
for tbl, label in BU_PL_MONTHLY_TABLES:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {B}.{tbl} (
        fisc_year STRING COMMENT '회계연도',
        cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
        gss_amount STRING COMMENT 'GSS 금액', gpu_amount STRING COMMENT 'GPU 금액',
        scr_amount STRING COMMENT 'SCR 금액', enc_amount STRING COMMENT 'EnC 금액',
        bpc_amount STRING COMMENT 'BPC 금액', total_amount STRING COMMENT '합계',
        ratio_val STRING COMMENT '비율',
        _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
        _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
        _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
        _row_number LONG COMMENT '엑셀 행 번호'
    ) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
    """)
    print(f"✓ {tbl}  ({label})")

# COMMAND ----------

# DBTITLE 1,Cell 6 acc_bu_pl_sum+erp_gl+erp_mgt_cost+gen_std (27-32)
# ── 27. acc_bu_pl_sum ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_bu_pl_sum (
    fisc_year STRING COMMENT '회계연도', cat STRING COMMENT '구분',
    gss_amount STRING COMMENT 'GSS 금액', gpu_amount STRING COMMENT 'GPU 금액',
    scr_amount STRING COMMENT 'SCR 금액', enc_amount STRING COMMENT 'EnC 금액',
    bpc_amount STRING COMMENT 'BPC 금액', total_amount STRING COMMENT '합계',
    gss_prod_amount STRING COMMENT 'GSS_제품',
    gss_good_serv_amount STRING COMMENT 'GSS_상품_용역',
    gss_prod_mat_cost STRING COMMENT 'GSS_제품재료비',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_bu_pl_sum")

# ── 28. acc_act_erp_gl ───────────────────────────────────────────────
spark.sql(f"DROP TABLE IF EXISTS {B}.acc_act_erp_gl")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_act_erp_gl (
    fisc_year       STRING COMMENT '회계연도',
    m_team_acct     STRING COMMENT '대팀제계정',
    biz_unit        STRING COMMENT '사업부',
    m_team          STRING COMMENT '대팀제',
    acct_nm         STRING COMMENT '계정명',
    dept_vld        STRING COMMENT '부서검증',
    saving_tgt_dept STRING COMMENT '절감목표부서',
    dept_code       STRING COMMENT '부서',
    month_val       STRING COMMENT '월',
    expense_amount  STRING COMMENT '비용',
    acct_code       STRING COMMENT '계정',
    yr_acct         STRING COMMENT '연_계정',
    l_cat           STRING COMMENT '대구분',
    vf_tp           STRING COMMENT '변동_고정',
    dir_indir_tp    STRING COMMENT '직접_간접',
    outsourcing_pay STRING COMMENT '외주지급',
    biz_cat         STRING COMMENT '사업구분',
    gss_alloc       STRING COMMENT 'GSS배부',
    gpu_alloc       STRING COMMENT 'GPU배부',
    scr_alloc       STRING COMMENT 'SCR배부',
    enc_alloc       STRING COMMENT 'EnC배부',
    epc_alloc       STRING COMMENT 'EPC배부',
    gss_amount      STRING COMMENT 'GSS',
    gpu_amount      STRING COMMENT 'GPU',
    scr_amount      STRING COMMENT 'SCR',
    enc_amount      STRING COMMENT 'EnC',
    total_amount    STRING COMMENT '합계',
    slip_dt         STRING COMMENT '회계일 (전표일)',
    acct_cd         STRING COMMENT '계정코드',
    acct_code_2     STRING COMMENT '계정2',
    acct_nm_2       STRING COMMENT '계정명2',
    slip_no         STRING COMMENT '전표번호',
    rmk             STRING COMMENT '비고',
    crtr_id         STRING COMMENT '작성자 기표부서명/작성자명',
    curr_cd         STRING COMMENT '통화',
    dr_amount       STRING COMMENT '차변금액',
    cr_amount       STRING COMMENT '대변금액',
    dr_local_amount STRING COMMENT '차변금액_자국',
    cr_local_amount STRING COMMENT '대변금액_자국',
    vend_cd         STRING COMMENT '거래처',
    vend_nm         STRING COMMENT '거래처명',
    seq_no          STRING COMMENT '순번',
    biz_place_cd    STRING COMMENT '사업장코드',
    biz_place_nm    STRING COMMENT '사업장명',
    dept_name       STRING COMMENT '부서명',
    cc_cd           STRING COMMENT '코스트센터',
    cc_nm           STRING COMMENT '코스트센터명',
    dept_cd         STRING COMMENT '부서코드',
    slip_path       STRING COMMENT '전표생성경로',
    ref_no          STRING COMMENT '참조번호',
    pjt_no          STRING COMMENT 'ProjectNo',
    pl_cat          STRING COMMENT '손익구분',
    dir_indir_tp_2  STRING COMMENT '직_간접',
    pl_acct         STRING COMMENT 'PL계정',
    pl_final_acct   STRING COMMENT 'PL최종계정',
    class_tp        STRING COMMENT '분류',
    month_val_2     STRING COMMENT '월2',
    slip_seq        STRING COMMENT '전표번호순번',
    sys_amount      STRING COMMENT 'SYS',
    inf_amount      STRING COMMENT 'INF',
    sum_amount      STRING COMMENT 'SUM',
    vld_status      STRING COMMENT '검증',
    _ingest_at      TIMESTAMP DEFAULT current_timestamp() COMMENT '브론즈 적재 일시',
    _source_file    STRING COMMENT '소스 파일명',
    _sheet_name     STRING COMMENT '시트명',
    _data_year      STRING COMMENT '데이터 기준 연도',
    _data_month     STRING COMMENT '데이터 기준 월',
    _row_number     LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (fisc_year)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
)
""")
print("✓ acc_act_erp_gl")

# ── 29. acc_erp_mgt_cost ─────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_erp_mgt_cost (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
    month_val STRING COMMENT '월', cat_3 STRING COMMENT '구분3',
    yr_int_acct STRING COMMENT '연_통합계정', tot_cost_acct STRING COMMENT '총비용계정',
    acct_nm STRING COMMENT '계정명', acct_cd STRING COMMENT '계정코드',
    summary_desc STRING COMMENT '적요', final_amount STRING COMMENT '최종금액',
    mgt_dept_code STRING COMMENT '관리부서', tot_cost_acct_comp STRING COMMENT '총비용계정_비교',
    biz_unit STRING COMMENT '사업부', grp_nm STRING COMMENT '그룹명',
    grp_cd STRING COMMENT '그룹코드',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_erp_mgt_cost")

# ── 30-32. acc_gen_std_* (3개, 구조 동일) ────────────────────────────────
for tbl, comment in [
    ('acc_gen_std_dept_cat', '일반기준_부서구분'),
    ('acc_gen_std_ledger',   '일반기준_원장조회'),
    ('acc_gen_std_dir_indir','일반기준_직간접구분'),
]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {B}.{tbl} (
        fisc_year STRING COMMENT '회계연도',
        cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
        _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
        _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
        _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
        _row_number LONG COMMENT '엑셀 행 번호'
    ) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
    """)
    print(f"✓ {tbl}  ({comment})")

# COMMAND ----------

# DBTITLE 1,Cell 7 재고효과+재료비+재무기획 (33-40)
# ── 33-35. acc_inv_eff_gpu/gss/scr ────────────────────────────────────
for tbl, comment in [
    ('acc_inv_eff_gpu','재고효과_GPU'),
    ('acc_inv_eff_gss','재고효과_GSS'),
    ('acc_inv_eff_scr','재고효과_SCR'),
]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {B}.{tbl} (
        fisc_year STRING COMMENT '회계연도',
        cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
        {MONTH_COLS}
        total_amount STRING COMMENT '합계',
        attr_cat STRING COMMENT '귀속구분', sum_cat STRING COMMENT '요약구분',
        _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
        _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
        _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
        _row_number LONG COMMENT '엑셀 행 번호'
    ) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
    """)
    print(f"✓ {tbl}  ({comment})")

# ── 36. acc_inv_eff_bu ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_inv_eff_bu (
    fisc_year STRING COMMENT '회계연도', cat STRING COMMENT '구분',
    acct_nm STRING COMMENT '계정명', vf_tp STRING COMMENT '변_고',
    {MONTH_COLS}
    total_amount STRING COMMENT '합계',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_inv_eff_bu")

# ── 37. acc_mat_cost_plan_sys ─────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_mat_cost_plan_sys (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
    {MONTH_COLS}
    total_amount STRING COMMENT '합계', ratio_pct STRING COMMENT '비율_%',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_mat_cost_plan_sys")

# ── 38. pln_mat_cst_bu_prd ────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.pln_mat_cst_bu_prd (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
    {MONTH_COLS}
    total_amount STRING COMMENT '합계', ratio_pct STRING COMMENT '비율_%',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ pln_mat_cst_bu_prd")

# ── 재무기획 공통 컨럼 그룹 ──────────────────────────────────────────────
FIN_ACT_SERIES = """
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
    act_sys STRING COMMENT '실적_SYSTEM사업부', act_gss STRING COMMENT '실적_GSS',
    act_gpu STRING COMMENT '실적_GPU', act_scr STRING COMMENT '실적_SCR',
    act_inf STRING COMMENT '실적_INFRA사업부', act_tot STRING COMMENT '실적_합계',
    plan_sys STRING COMMENT '사업계획_SYSTEM사업부', plan_gss STRING COMMENT '사업계획_GSS',
    plan_gpu STRING COMMENT '사업계획_GPU', plan_scr STRING COMMENT '사업계획_SCR',
    plan_inf STRING COMMENT '사업계획_INFRA사업부', plan_enc STRING COMMENT '사업계획_EnC',
    plan_bpc STRING COMMENT '사업계획_BPC', plan_tot STRING COMMENT '사업계획_합계',
    var_sys STRING COMMENT '계획대비실적_SYSTEM사업부', var_gss STRING COMMENT '계획대비실적_GSS',
    var_gpu STRING COMMENT '계획대비실적_GPU', var_scr STRING COMMENT '계획대비실적_SCR',
    var_inf STRING COMMENT '계획대비실적_INFRA사업부', var_enc STRING COMMENT '계획대비실적_EnC',
    var_bpc STRING COMMENT '계획대비실적_BPC', var_tot STRING COMMENT '계획대비실적_합계',"""

# ── 39. acc_fin_plan_dept_perf ─────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_fin_plan_dept_perf (
    fisc_year STRING COMMENT '회계연도',
    {FIN_ACT_SERIES}
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_fin_plan_dept_perf")

# ── 40. acc_fin_plan_bep ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_fin_plan_bep (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
    act_sys STRING COMMENT '실적_SYSTEM사업부', act_gss STRING COMMENT '실적_GSS',
    act_gpu STRING COMMENT '실적_GPU', act_scr STRING COMMENT '실적_SCR',
    act_inf STRING COMMENT '실적_INFRA사업부', act_tot STRING COMMENT '실적_합계',
    plan_sys STRING COMMENT '사업계획_SYSTEM사업부', plan_gss STRING COMMENT '사업계획_GSS',
    plan_gpu STRING COMMENT '사업계획_GPU', plan_scr STRING COMMENT '사업계획_SCR',
    plan_inf STRING COMMENT '사업계획_INFRA사업부', plan_enc STRING COMMENT '사업계획_EnC',
    plan_bpc STRING COMMENT '사업계획_BPC', plan_tot STRING COMMENT '사업계획_합계',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_fin_plan_bep")

# COMMAND ----------

# DBTITLE 1,Cell 8 월별계획실적+결산+제조원가 (41-51)
# ── 월별 계획/실적 헬퍼 ──────────────────────────────────────────────────
def mth_series(label):
    biz_units  = ['sys','gss','gpu','scr','inf','enc','epc','tot']
    biz_labels = ['SYSTEM사업부','GSS','GPU','SCR','INFRA사업부','EnC','EPC','합계']
    yr_row  = "    " + ", ".join([f"yr_{u} STRING COMMENT '년.{label}_{l}'" for u,l in zip(biz_units,biz_labels)])
    mth_rows = []
    for m in range(1,13):
        row = "    " + ", ".join([f"m{m:02d}_{u} STRING COMMENT '{m}월.{label}_{l}'" for u,l in zip(biz_units,biz_labels)])
        mth_rows.append(row)
    vld_row = "    " + ", ".join([f"vld_{u} STRING COMMENT '검증.{label}_{l}'" for u,l in zip(biz_units,biz_labels)])
    return yr_row + ",\n" + ",\n".join(mth_rows) + ",\n" + vld_row + ","

# ── 41. acc_fin_plan_mth_plan ───────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_fin_plan_mth_plan (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
{mth_series('사업계획')}
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_fin_plan_mth_plan")

# ── 42. acc_fin_plan_mth_act ───────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_fin_plan_mth_act (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
{mth_series('사업실적')}
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_fin_plan_mth_act")

# ── 43. acc_settle_stmt ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_settle_stmt (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2', cat_3 STRING COMMENT '구분3',
    act_sys_tot STRING COMMENT '실적_SYSTEM_합계', act_sys_pct STRING COMMENT '실적_SYSTEM_비율',
    act_sys_gss_prod STRING COMMENT '실적_SYSTEM_GSS_제품', act_sys_gpu_prod STRING COMMENT '실적_SYSTEM_GPU_제품',
    act_sys_good_serv STRING COMMENT '실적_SYSTEM_상품_용역', act_sys_scr STRING COMMENT '실적_SYSTEM_SCR',
    act_inf_tot STRING COMMENT '실적_INFRA_합계', act_inf_pct STRING COMMENT '실적_INFRA_비율',
    act_inf_enc STRING COMMENT '실적_INFRA_ENC', act_inf_bpc STRING COMMENT '실적_INFRA_BPC',
    act_tot STRING COMMENT '실적_합계',
    plan_sys_tot STRING COMMENT '계획_SYSTEM_합계', plan_sys_pct STRING COMMENT '계획_SYSTEM_비율',
    plan_sys_gss_prod STRING COMMENT '계획_SYSTEM_GSS_제품', plan_sys_gpu_prod STRING COMMENT '계획_SYSTEM_GPU_제품',
    plan_sys_good_serv STRING COMMENT '계획_SYSTEM_상품_용역', plan_sys_scr STRING COMMENT '계획_SYSTEM_SCR',
    plan_inf_tot STRING COMMENT '계획_INFRA_합계', plan_inf_pct STRING COMMENT '계획_INFRA_비율',
    plan_inf_enc STRING COMMENT '계획_INFRA_ENC', plan_inf_bpc STRING COMMENT '계획_INFRA_BPC',
    plan_tot STRING COMMENT '계획_합계',
    var_sys_tot STRING COMMENT '계획대비실적_SYSTEM_합계',
    var_inf_tot STRING COMMENT '계획대비실적_INFRA_합계', var_tot STRING COMMENT '계획대비실적_합계',
    remarks STRING COMMENT '비고',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_settle_stmt")

# ── 44-50: acc_mfg_cost_*_acr (7테이블) ──────────────────────────────────
MFG_COST_TABLES = [
    ('acc_mfg_cost_enc_acr','EnC_발생기준',True),
    ('acc_mfg_cost_epc_acr','EPC_발생기준',True),
    ('acc_mfg_cost_gpu_acr','GPU_발생기준',True),
    ('acc_mfg_cost_gss_acr','GSS_발생기준',True),
    ('acc_mfg_cogs','PL상매출원가',False),
    ('acc_mfg_cost_scr_acr','SCR_발생기준',True),
    ('acc_mfg_cost_tot_acr','전체_발생기준',True),
]
for tbl, comment, has_cat3 in MFG_COST_TABLES:
    cat3_col = "cat_3 STRING COMMENT '구분3'," if has_cat3 else ""
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {B}.{tbl} (
        fisc_year STRING COMMENT '회계연도',
        cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
        {cat3_col}
        {MONTH_COLS}
        total_amount STRING COMMENT '합계',
        _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
        _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
        _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
        _row_number LONG COMMENT '엑셀 행 번호'
    ) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
    """)
    print(f"✓ {tbl}  ({comment})")

# ── 51. acc_mfg_item_gp ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.acc_mfg_item_gp (
    fisc_year STRING COMMENT '회계연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
    {MONTH_COLS}
    total_amount STRING COMMENT '합계',
{META_COLS}
) USING DELTA PARTITIONED BY (fisc_year) {TBLPROP}
""")
print("✓ acc_mfg_item_gp")

# COMMAND ----------

# DBTITLE 1,Cell 9 예산 마스터+계획+부서 DDL (52-55-2)
# ── 52. bgt_acct_info ──────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_acct_info (
    plan_year STRING COMMENT '계획연도',
    acct_nm STRING COMMENT '계정명', acct_cd STRING COMMENT '계정코드',
    grp_nm STRING COMMENT '그룹명', grp_cd STRING COMMENT '그룹코드',
    tot_cost_acct STRING COMMENT '총비용계정', tot_cost_acct_comp STRING COMMENT '총비용계정_비교',
    opex_acct_yn STRING COMMENT '영업활동경비해당계정',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_acct_info  (OR REPLACE)")

# ── 53. bgt_plan_xl ────────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_plan_xl (
    plan_year STRING COMMENT '계획연도',
    seq_no STRING COMMENT 'No.',
    month_val STRING COMMENT '월', cat STRING COMMENT '구분',
    tot_cost_acct STRING COMMENT '총비용계정',
    acct_nm STRING COMMENT '계정명', acct_cd STRING COMMENT '계정코드',
    summary_desc STRING COMMENT '적요',
    bgt_amount STRING COMMENT '예산금액', final_amount STRING COMMENT '최종금액',
    tot_cost_acct_comp STRING COMMENT '총비용계정_비교',
    slip_dept STRING COMMENT '기표부서', attr_dept STRING COMMENT '귀속부서',
    mgt_dept STRING COMMENT '관리부서',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_plan_xl  (OR REPLACE)")

# ── 54. bgt_dept_std ──────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_dept_std (
    plan_year STRING COMMENT '계획연도',
    dept_nm STRING COMMENT '부서명', dept_std STRING COMMENT '부서기준',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_dept_std  (OR REPLACE)")

# ── 55. bgt_dept_cd ───────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_dept_cd (
    plan_year STRING COMMENT '계획연도',
    biz_unit STRING COMMENT '사업부', dept_nm STRING COMMENT '부서명',
    dept_cd STRING COMMENT '부서코드', cc_nm STRING COMMENT 'CostCenter명',
    cc_cd STRING COMMENT 'CC코드', cc_cat STRING COMMENT 'CC구분',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_dept_cd  (OR REPLACE)")

# ── 55-2. bgt_dept_biz_unit ────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_dept_biz_unit (
    plan_year STRING COMMENT '계획연도',
    biz_unit STRING COMMENT '사업부', dept_nm STRING COMMENT '부서명',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_dept_biz_unit  (OR REPLACE)")

# COMMAND ----------

# DBTITLE 1,Cell 10 예산 ERP원장+이관+현황테이블 (56-59)
# ── 56. bgt_act_erp_gl ─────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_act_erp_gl (
    plan_year STRING COMMENT '계획연도',
    month_val STRING COMMENT '월', tot_cost_acct STRING COMMENT '총비용계정',
    mgt_dept STRING COMMENT '관리부서', slip_dt STRING COMMENT '회계일',
    acct_cd STRING COMMENT '계정코드', acct_nm STRING COMMENT '계정명',
    slip_no STRING COMMENT '전표번호', rmk STRING COMMENT '비고',
    crtr_id STRING COMMENT '작성자', curr_cd STRING COMMENT '통화',
    dr_local_amount STRING COMMENT '차변금액_자국',
    cr_local_amount STRING COMMENT '대변금액_자국',
    dept_nm STRING COMMENT '부서명', cc_cd STRING COMMENT '코스트센터',
    cc_nm STRING COMMENT '코스트센터명', dept_cd STRING COMMENT '부서코드',
    slip_path STRING COMMENT '전표생성경로', ref_no STRING COMMENT '참조번호',
    pjt_no STRING COMMENT 'ProjectNo', tot_cost_acct_comp STRING COMMENT '총비용계정_비교',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_act_erp_gl  (OR REPLACE)")

# ── 57. bgt_erp_trns ───────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_erp_trns (
    plan_year STRING COMMENT '계획연도',
    cat_1 STRING COMMENT '구분1', cat_2 STRING COMMENT '구분2',
    month_val STRING COMMENT '월', cat_3 STRING COMMENT '구분3',
    yr_int_acct STRING COMMENT '연_통합계정', tot_cost_acct STRING COMMENT '총비용계정',
    acct_nm STRING COMMENT '계정명', acct_cd STRING COMMENT '계정코드',
    summary_desc STRING COMMENT '적요', final_amount STRING COMMENT '최종금액',
    slip_dept STRING COMMENT '기표부서', attr_dept STRING COMMENT '귀속부서',
    mgt_dept STRING COMMENT '관리부서', tot_cost_acct_comp STRING COMMENT '총비용계정_비교',
    biz_unit STRING COMMENT '사업부', grp_nm STRING COMMENT '그룹명',
    grp_cd STRING COMMENT '그룹코드', appr_dt STRING COMMENT '품의일시',
    appr_no STRING COMMENT '품의번호', bgt_mgt_no STRING COMMENT 'No._예산관리',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_erp_trns  (OR REPLACE)")

# ── 현황 테이블 공통 메타 컨럼 ──────────────────────────────────────────
ST_META = """
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'"""

ST_DIM = """    plan_year STRING COMMENT '계획연도', base_month STRING COMMENT '기준월',
    bu_hq STRING COMMENT '사업부_본부', dept_code STRING COMMENT '부서',
    mgt_dept_std STRING COMMENT '관리부서기준', acct_subject STRING COMMENT '계정과목',"""

ST_MONTH_PLAN = "\n".join([
    f"    m{i:02d}_plan STRING COMMENT '{i}월_계획', m{i:02d}_act STRING COMMENT '{i}월_실적',\n    m{i:02d}_rem STRING COMMENT '{i}월_잔여예산', m{i:02d}_rate STRING COMMENT '{i}월_집행률',"
    for i in range(1, 13)
])

# ── 58. bgt_opex_st ──────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_opex_st (
    {ST_DIM}
    {ST_MONTH_PLAN}
    base_acc_plan STRING COMMENT '기준월누적_계획', base_acc_act STRING COMMENT '기준월누적_실적',
    base_acc_rem STRING COMMENT '기준월누적_잔여예산', base_acc_rate STRING COMMENT '기준월누적_집행률',
    yr_acc_plan STRING COMMENT '누적_1_12_계획', yr_acc_act STRING COMMENT '누적_1_12_실적',
    yr_acc_rem STRING COMMENT '누적_1_12_잔여예산', yr_acc_rate STRING COMMENT '누적_1_12_집행률',
    {ST_META}
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_opex_st  (OR REPLACE)")

# ── 59. bgt_opex_ex_st ───────────────────────────────────────────────
ST_MONTH_TRNS = "\n".join([
    f"    m{i:02d}_trns STRING COMMENT '{i}월_이관_전용', m{i:02d}_ovr STRING COMMENT '{i}월_초과',"
    for i in range(1, 13)
])
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_opex_ex_st (
    {ST_DIM}
    {ST_MONTH_TRNS}
    tot_trns STRING COMMENT '누적_이관_전용', tot_ovr STRING COMMENT '누적_초과',
    remarks STRING COMMENT '비고및특이사항',
    {ST_META}
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_opex_ex_st  (OR REPLACE)")

# COMMAND ----------

# DBTITLE 1,Cell 11 영업활동경비+투자현황 (60-64)
# ── 60. bgt_sales_st ────────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_sales_st (
    {ST_DIM}
    {ST_MONTH_PLAN}
    base_acc_plan STRING COMMENT '기준월누적_계획', base_acc_act STRING COMMENT '기준월누적_실적',
    base_acc_rem STRING COMMENT '기준월누적_잔여예산', base_acc_rate STRING COMMENT '기준월누적_집행률',
    yr_acc_plan STRING COMMENT '누적_1_12_계획', yr_acc_act STRING COMMENT '누적_1_12_실적',
    yr_acc_rem STRING COMMENT '누적_1_12_잔여예산', yr_acc_rate STRING COMMENT '누적_1_12_집행률',
    {ST_META}
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_sales_st  (OR REPLACE)")

# ── 61. bgt_sales_ex_st ────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_sales_ex_st (
    {ST_DIM}
    {ST_MONTH_TRNS}
    base_acc_trns STRING COMMENT '기준월누적_이관_전용', base_acc_ovr STRING COMMENT '기준월누적_초과',
    yr_acc_trns STRING COMMENT '누적_1_12_이관_전용', yr_acc_ovr STRING COMMENT '누적_1_12_초과',
    {ST_META}
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_sales_ex_st  (OR REPLACE)")

# ── 62. bgt_inv_pjt_st ──────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {B}.bgt_inv_pjt_st (
    plan_year STRING COMMENT '계획연도',
    dept_nm STRING COMMENT '부서명', cat STRING COMMENT '구분', inv_tp STRING COMMENT '투자유형',
    pjt_code STRING COMMENT 'pjt_code', pjt_nm STRING COMMENT '프로젝트명',
    yr_inv_plan STRING COMMENT '년_투자계획', yr_inv_exec STRING COMMENT '년_투자집행',
    act_exp_proc STRING COMMENT '실적_1비용처리', progress_rate_1 STRING COMMENT '1진행율',
    balance_amount_1 STRING COMMENT '1잔액',
    act_exp_unproc STRING COMMENT '실적_2비용미처리', progress_rate_1_2 STRING COMMENT '진행률_1_2',
    total_act STRING COMMENT '총실적', total_bal STRING COMMENT '총잔액',
    remarks STRING COMMENT '비고',
    _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
    _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
    _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
    _row_number LONG COMMENT '엑셀 행 번호'
) USING DELTA PARTITIONED BY (plan_year)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")
print("✓ bgt_inv_pjt_st  (OR REPLACE)")

# ── 63-64. bgt_inv_cnf_st / bgt_inv_asset_tp ──────────────────────────────
for tbl, comment in [
    ('bgt_inv_cnf_st', '투자_예산확정상태'),
    ('bgt_inv_asset_tp', '투자_투자자산유형'),
]:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {B}.{tbl} (
        plan_year STRING COMMENT '계획연도', cat STRING COMMENT '구분',
        yr_inv_plan STRING COMMENT '년_투자계획', plan_cnt STRING COMMENT '건수_계획',
        yr_inv_exec STRING COMMENT '년_투자집행', exec_rate STRING COMMENT '집행률',
        total_act STRING COMMENT '실적_총실적', total_progress_rate STRING COMMENT '실적_총진행률',
        act_exp_proc STRING COMMENT '실적_1비용처리', progress_rate_1 STRING COMMENT '실적_1진행률',
        act_exp_unproc STRING COMMENT '실적_2비용미처리', progress_rate_2 STRING COMMENT '실적_2.진행률',
        _ingest_at TIMESTAMP COMMENT '브론즈 적재 일시',
        _source_file STRING COMMENT '소스 파일명', _sheet_name STRING COMMENT '시트명',
        _data_year STRING COMMENT '데이터 기준 연도', _data_month STRING COMMENT '데이터 기준 월',
        _row_number LONG COMMENT '엑셀 행 번호'
    ) USING DELTA PARTITIONED BY (plan_year)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
    """)
    print(f"✓ {tbl}  ({comment}) (OR REPLACE)")

# COMMAND ----------

# DBTITLE 1,Cell 12 최종 확인
print(f"=== {B} 브론즈 테이블 현황 ===\n")
rows = spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{BRONZE}`").collect()
tables = sorted([r.tableName for r in rows if not r.isTemporary])
print(f"  수 테이블 수: {len(tables)}개\n")
for i, t in enumerate(tables, 1):
    print(f"  {i:3}. {t}")
print("\n브론즈 DDL 설정 완료 ✓")