# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze 결산(acc_*/setl_*) DML 개요
# MAGIC %md
# MAGIC # 브론즈 결산 DML 적재
# MAGIC 소스: `/Volumes/wonik_poc/wonik_test1/volume/BEP(결산) 파일_Databricks (1).xlsx`  
# MAGIC 대상: `wonik_poc.wonik_test1_bronze` — `acc_*` / `setl_*` 테이블 (44개)  
# MAGIC
# MAGIC | 시트 | 대상 테이블 |
# MAGIC |---|---|
# MAGIC | `Summaty(PL)` | `setl_summary_pl` |
# MAGIC | `정산서` | `acc_settle_stmt` |
# MAGIC | `재무기획` | `acc_fin_plan_dept_perf`, `acc_fin_plan_bep` |
# MAGIC | `재무기획(월별계획)` | `acc_fin_plan_mth_plan` |
# MAGIC | `재무기획(월별실적)` | `acc_fin_plan_mth_act` |
# MAGIC | `제조비용 재고효과` | `acc_mfg_*`(7), `acc_inv_eff_*`(4) |
# MAGIC | `26년계획(엑셀)` | `setl_plan_xl` |
# MAGIC | `26년실적(ERP원장)` | `acc_act_erp_gl` |
# MAGIC | `인관전용(ERP)` | `acc_erp_mgt_cost` |
# MAGIC | `26년매출계획(엑셀)` | `setl_sls_pln_bu_pg`, `setl_sales_plan_sys` |
# MAGIC | `26년재료비계획(엑셀)` | `pln_mat_cst_bu_prd`, `acc_mat_cost_plan_sys` |
# MAGIC | `사업부손익(엑셀)` | `acc_bu_pl_sum`, `acc_bu_pl_{month}` (13) |
# MAGIC | `일반기준` | `acc_gen_std_ledger`, `acc_gen_std_dept_cat`, `acc_gen_std_dir_indir` |
# MAGIC | `변고기준` | `acc_vf_cost_std` |
# MAGIC | `변고검토` | `acc_vf_cost_rev` |
# MAGIC | `배부기준` | `acc_alloc_rnd`, `_sales_plan`, `_hc_plan`, `_sys_hc`, `_mat_cost_ref`, `_pur_task` |
# MAGIC | `26배부테이블` | `acc_alloc_mst` |

# COMMAND ----------

# DBTITLE 1,Cell 2 설정 + 헬퍼
# MAGIC %pip install openpyxl -q
# MAGIC
# MAGIC import pandas as pd
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC CATALOG   = "wonik_poc"
# MAGIC BRONZE    = "wonik_test1_bronze"
# MAGIC B         = f"{CATALOG}.{BRONZE}"
# MAGIC FILE_PATH = "/Volumes/wonik_poc/wonik_test1/volume/BEP(\uacb0\uc0b0) \ud30c\uc77c_Databricks (1).xlsx"
# MAGIC FISC_YEAR = "2026"
# MAGIC SRC_FILE  = "BEP(\uacb0\uc0b0) \ud30c\uc77c_Databricks (1).xlsx"
# MAGIC
# MAGIC def _str(v):
# MAGIC     s = str(v) if v is not None else None
# MAGIC     return None if s in ('nan','NaT','None','<NA>','') else s
# MAGIC
# MAGIC def add_meta(df, sheet):
# MAGIC     """메타 컬럼 추가 (DDL 타입에 맞게: _ingest_at=timestamp, _row_number=long)"""
# MAGIC     df = df.copy()
# MAGIC     df.columns = [str(c) for c in df.columns]
# MAGIC     df['_ingest_at']   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# MAGIC     df['_source_file'] = SRC_FILE
# MAGIC     df['_sheet_name']  = sheet
# MAGIC     df['_data_year']   = FISC_YEAR
# MAGIC     df['_data_month']  = '05'
# MAGIC     df['_row_number']  = list(range(1, len(df)+1))
# MAGIC     return df
# MAGIC
# MAGIC def get_ddl_cols(tbl):
# MAGIC     """DDL table column list (deduplicated)"""
# MAGIC     desc = spark.sql(f"DESCRIBE TABLE {B}.{tbl}").collect()
# MAGIC     seen = set()
# MAGIC     cols = []
# MAGIC     for r in desc:
# MAGIC         if not r.col_name.startswith('#') and r.col_name not in seen:
# MAGIC             cols.append((r.col_name, r.data_type))
# MAGIC             seen.add(r.col_name)
# MAGIC     return cols
# MAGIC
# MAGIC def assign_ddl_names(df, tbl, skip_cols=None):
# MAGIC     """Position-based rename of DataFrame columns to DDL column names"""
# MAGIC     skip_cols = skip_cols or ['fisc_year']
# MAGIC     meta_cols = ['_ingest_at','_source_file','_sheet_name','_data_year','_data_month','_row_number']
# MAGIC     ddl_all = get_ddl_cols(tbl)
# MAGIC     ddl_data = [c[0] for c in ddl_all if c[0] not in skip_cols + meta_cols]
# MAGIC     df_data = [c for c in df.columns if c not in skip_cols + meta_cols]
# MAGIC     rename_map = {}
# MAGIC     for i, old_col in enumerate(df_data):
# MAGIC         if i < len(ddl_data):
# MAGIC             rename_map[old_col] = ddl_data[i]
# MAGIC     return df.rename(columns=rename_map)
# MAGIC
# MAGIC def write_bronze(df, tbl):
# MAGIC     """pandas -> Delta (DDL schema-aware: preserves table structure via insertInto) \ube0c\ub860\uc988 \uc6d0\uc2dc \uc801\uc7ac)"""
# MAGIC     df = df.copy()
# MAGIC     # 1) Get DDL column info
# MAGIC     ddl_cols = get_ddl_cols(tbl)
# MAGIC     target_names = [c[0] for c in ddl_cols]
# MAGIC     # 2) String conversion (except _row_number)
# MAGIC     for c in df.columns:
# MAGIC         if c != '_row_number':
# MAGIC             df[c] = df[c].apply(_str)
# MAGIC     # 3) Add missing cols as NULL, drop extras
# MAGIC     for col_name in target_names:
# MAGIC         if col_name not in df.columns:
# MAGIC             df[col_name] = None
# MAGIC     df = df[target_names]
# MAGIC     # 4) Create Spark DataFrame
# MAGIC     sdf = spark.createDataFrame(df.astype(object).where(pd.notnull(df), None))
# MAGIC     # 5) Type casting
# MAGIC     for col_name, col_type in ddl_cols:
# MAGIC         if col_name in sdf.columns:
# MAGIC             if col_type == 'timestamp':
# MAGIC                 sdf = sdf.withColumn(col_name, F.to_timestamp(F.col(col_name)))
# MAGIC             elif col_type in ('bigint', 'long'):
# MAGIC                 sdf = sdf.withColumn(col_name, F.col(col_name).cast('bigint'))
# MAGIC     # 6) Insert into existing table (preserves DDL schema)
# MAGIC     sdf.write.mode("overwrite").insertInto(f"{B}.{tbl}", overwrite=True)
# MAGIC     cnt = spark.table(f"{B}.{tbl}").count()
# MAGIC     print(f"  \u2713 {tbl:<45} {cnt:>7,}\uac74")
# MAGIC
# MAGIC def clean_col(c):
# MAGIC     return (str(c).strip()
# MAGIC             .replace(' ','_').replace('/','_').replace('(','').replace(')',''))
# MAGIC
# MAGIC print(f"\u2713 \uc124\uc815 \uc644\ub8cc  B={B}")
# MAGIC print(f"  FILE: {FILE_PATH}")

# COMMAND ----------

# DBTITLE 1,Cell 3 1:1 매핑 간단 테이블 (setl_plan_xl / acc_erp_mgt_cost / acc_alloc_mst)
print("=== Group 1: 1:1 \ub9e4\ud551 \ud14c\uc774\ube14 ===")

# ── setl_plan_xl  (26\ub144\uacc4\ud68d(\uc5d1\uc140)) ────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="26\ub144\uacc4\ud68d(\uc5d1\uc140)", skiprows=2, header=0).dropna(how="all")
df.columns = [clean_col(c) for c in df.columns]
df.insert(0, 'fisc_year', FISC_YEAR)
df = add_meta(df, "26\ub144\uacc4\ud68d(\uc5d1\uc140)")
df = assign_ddl_names(df, 'setl_plan_xl')
write_bronze(df, 'setl_plan_xl')

# ── acc_erp_mgt_cost  (\uc778\uad00\uc804\uc6a9(ERP)) ───────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="\uc778\uad00\uc804\uc6a9(ERP)", skiprows=1, header=0).dropna(how="all")
df.columns = [clean_col(c) for c in df.columns]
col = df.columns.tolist()
print(f"  \uc778\uad00\uc804\uc6a9 columns: {col}")
# DDL \ucf7c\ub7fc\uba85\uc73c\ub85c \ub9e4\ud551
def gc(kws, default_idx=None):
    for kw in kws:
        for c in col:
            if kw in str(c): return df[c]
    return df.iloc[:, default_idx] if default_idx is not None and default_idx < len(col) else pd.Series([None]*len(df))

result = pd.DataFrame({
    'fisc_year':    FISC_YEAR,
    'cat_1':        df.iloc[:, 0],
    'cat_2':        df.iloc[:, 1] if len(col) > 1 else None,
    'month_val':    gc(['\uc6d4'], 2),
    'cat_3':        df.iloc[:, 3] if len(col) > 3 else None,
    'yr_int_acct':  gc(['\ud1b5\ud569\uacc4\uc815'], 4),
    'tot_cost_acct': gc(['\ucd1d\ube44\uc6a9\uacc4\uc815'], 5),
    'acct_nm':       gc(['\uacc4\uc815\uba85'], 6),
    'acct_cd':       gc(['\uacc4\uc815\ucf54\ub4dc'], 7),
    'summary_desc':  gc(['\uc801\uc694'], 8),
    'final_amount':  gc(['\ucd5c\uc885\uae08\uc561'], 9),
    'mgt_dept_code': gc(['\uad00\ub9ac\ubd80\uc11c'], 12),
    'tot_cost_acct_comp': gc(['\ube44\uad50']),
    'biz_unit':      gc(['\uc0ac\uc5c5\ubd80']),
    'grp_nm':        gc(['\uadf8\ub8f9\uba85']),
    'grp_cd':        gc(['\uadf8\ub8f9\ucf54\ub4dc']),
})
result = add_meta(result, "\uc778\uad00\uc804\uc6a9(ERP)")
write_bronze(result, 'acc_erp_mgt_cost')

# ── acc_alloc_mst  (26\ubc30\ubd80\ud14c\uc774\ube14) ──────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="26\ubc30\ubd80\ud14c\uc774\ube14", skiprows=1, header=0).dropna(how="all")
col = df.columns.tolist()
print(f"  26\ubc30\ubd80\ud14c\uc774\ube14 columns: {col[:10]}")
def gc2(kws, default_idx=None):
    for kw in kws:
        for c in col:
            if kw in str(c): return df[c]
    return df.iloc[:, default_idx] if default_idx is not None and default_idx < len(col) else pd.Series([None]*len(df))

result = pd.DataFrame({
    'fisc_year':      FISC_YEAR,
    'dept_code':      df['\ubd80\uc11c'] if '\ubd80\uc11c' in col else df.iloc[:, 0],
    'dept_name':      df['\ubd80\uc11c\uba85'] if '\ubd80\uc11c\uba85' in col else df.iloc[:, 1],
    'recogn_team':    gc2(['\uc778\uc2dd\ud300']),
    'recogn_dept':    gc2(['\uc778\uc2dd\ubd80\uc11c']),
    'category':       gc2(['\uad6c\ubd84']),
    'pl_hq_1':        gc2(['\uc190\uc775\ubcf8\ubd80']),
    'gss_alloc':      gc2(['GSS\ubc30\ubd80']),
    'gpu_alloc':      gc2(['GPU\ubc30\ubd80']),
    'scr_alloc':      gc2(['SCR\ubc30\ubd80']),
    'enc_alloc':      gc2(['EnC\ubc30\ubd80', 'ENC\ubc30\ubd80']),
    'epc_alloc':      gc2(['EPC\ubc30\ubd80']),
    'mfg_sgna_type':  gc2(['\uc81c_\ud310']),
    'dir_indir_type': gc2(['\uc9c1_\uac04']),
    'alloc_type':     gc2(['\ubc30\ubd80']),
    'hq_name':        gc2(['\ubcf8\ubd80']),
    'biz_unit_name':  gc2(['\uc0ac\uc5c5\ubd80\uba85']),
    'macro_team_system': gc2(['\ub300\ud300\uc81c']),
})
result = add_meta(result, "26\ubc30\ubd80\ud14c\uc774\ube14")
write_bronze(result, 'acc_alloc_mst')

# COMMAND ----------

# DBTITLE 1,Cell 4 acc_act_erp_gl (결산 ERP원장)
print("=== acc_act_erp_gl (26\ub144\uc2e4\uc801 ERP\uc6d0\uc7a5) ===")
df = pd.read_excel(FILE_PATH, sheet_name="26\ub144\uc2e4\uc801(ERP\uc6d0\uc7a5)", skiprows=1, header=0).dropna(how="all")
col = df.columns.tolist()
print(f"  columns: {col[:12]}")
print(f"  shape: {df.shape}")

def gc(kws, default_idx=None):
    for kw in kws:
        for c in col:
            if kw == str(c): return df[c]   # \uc815\ud655 \ub9e4\uce6d \uba3c\uc800
    for kw in kws:
        for c in col:
            if kw in str(c): return df[c]   # \ud3ec\ud568 \ub9e4\uce6d
    return df.iloc[:, default_idx] if default_idx is not None and default_idx < len(col) else pd.Series([None]*len(df))

result = pd.DataFrame({
    'fisc_year':     FISC_YEAR,
    'm_team_acct':   gc(['\ub300\ud300\uc81c\uacc4\uc815'], 0),
    'biz_unit':      gc(['\uc0ac\uc5c5\ubd80'], 1),
    'm_team':        gc(['\ub300\ud300\uc81c'], 2),
    'acct_nm':       gc(['\uacc4\uc815\uba85'], 3),
    'dept_vld':      gc(['\ubd80\uc11c\uac80\uc99d'], 4),
    'saving_tgt_dept': gc(['\uc808\uac10\ubaa9\ud45c\ubd80\uc11c'], 5),
    'dept_code':     gc(['\ubd80\uc11c'], 6),
    'month_val':     gc(['\uc6d4'], 7),
    'expense_amount': gc(['\ube44\uc6a9'], 8),
    'acct_code':     df['\uacc4\uc815'] if '\uacc4\uc815' in col else gc(['\uacc4\uc815'], 9),
    'yr_acct':       gc(['\uc5f0) \uacc4\uc815', '\uc5f0)\uacc4\uc815', '\uc5f0_\uacc4\uc815']),
    'l_cat':         gc(['\ub300\uad6c\ubd84']),
    'vf_tp':         gc(['\ubcc0\ub3d9_\uace0\uc815', '\ubcc0\ub3d9/\uace0\uc815']),
    'dir_indir_tp':  gc(['\uc9c1\uc811_\uac04\uc811', '\uc9c1\uc811/\uac04\uc811']),
    'outsourcing_pay': gc(['\uc678\uc8fc\uc9c0\uae09']),
    'biz_cat':       gc(['\uc0ac\uc5c5\uad6c\ubd84']),
    'gss_alloc':     gc(['GSS\ubc30\ubd80']),
    'gpu_alloc':     gc(['GPU\ubc30\ubd80']),
    'scr_alloc':     gc(['SCR\ubc30\ubd80']),
    'enc_alloc':     gc(['EnC\ubc30\ubd80', 'ENC\ubc30\ubd80']),
    'epc_alloc':     gc(['EPC\ubc30\ubd80']),
    'gss_amount':    gc(['GSS']),
    'gpu_amount':    gc(['GPU']),
    'scr_amount':    gc(['SCR']),
    'enc_amount':    gc(['EnC', 'ENC']),
    'total_amount':  gc(['\ud569\uacc4']),
    'slip_dt':       gc(['\ud68c\uacc4\uc77c']),
    'acct_cd':       gc(['\uacc4\uc815\ucf54\ub4dc']),
    'slip_no':       gc(['\uc804\ud45c\ubc88\ud638']),
    'slip_seq':       gc(['\uc804\ud45c\ubc88\ud638\uc21c\ubc88'], 56),
    'rmk':           gc(['\ube44\uace0']),
    'crtr_id':       gc(['\uc791\uc131\uc790']),
    'curr_cd':       gc(['\ud1b5\ud654']),
    'dr_amount':     gc(['\ucc28\ubcc0\uae08\uc561']),
    'cr_amount':     gc(['\ub300\ubcc0\uae08\uc561']),
    'dr_local_amount': gc(['\ucc28\ubcc0\uae08\uc561\(\uc790\uad6d\)', '\ucc28\ubcc0\uae08\uc561\uc790\uad6d']),
    'cr_local_amount': gc(['\ub300\ubcc0\uae08\uc561\(\uc790\uad6d\)', '\ub300\ubcc0\uae08\uc561\uc790\uad6d']),
    'dept_name':       gc(['\ubd80\uc11c\uba85']),
    'pjt_no':          gc(['Project No', '\ud504\ub85c\uc81d\ud2b8']),
    'dept_cd':         gc(['\ubd80\uc11c\ucf54\ub4dc']),
    'sys_amount':      gc(['SYS', 'SYSTEM']),
    'inf_amount':      gc(['INF', 'INFRA']),
    'vend_cd':         gc(['거래처'], 38),
    'vend_nm':         gc(['거래처명'], 39),
    'seq_no':          gc(['순번'], 40),
    'biz_place_cd':    gc(['사업장코드'], 41),
    'biz_place_nm':    gc(['사업장명'], 42),
    'cc_cd':           gc(['코스트센터'], 44),
    'cc_nm':           gc(['코스트센터명'], 45),
    'slip_path':       gc(['전표생성경로'], 47),
    'ref_no':          gc(['참조번호'], 48),
    'acct_code_2':     gc(['계정.1'], 28),
    'acct_nm_2':       gc(['계정명.1'], 29),
    'pl_cat':          gc(['손익구분']),
    'dir_indir_tp_2':  gc(['직/간접']),
    'pl_acct':         gc(['PL계정']),
    'pl_final_acct':   gc(['PL최종계정']),
    'class_tp':        gc(['분류']),
    'month_val_2':     gc(['월.1'], 55),
    'sum_amount':      gc(['SUM']),
    'vld_status':      gc(['검증']),
})
result = add_meta(result, "26\ub144\uc2e4\uc801(ERP\uc6d0\uc7a5)")
write_bronze(result, 'acc_act_erp_gl')

# COMMAND ----------

# DBTITLE 1,Cell 5 Raw 로드 (Summaty_PL / 정산서 / 재무기획월별 / 변고)
print("=== Group 2: Raw \ub85c\ub4dc (\ucee4\ub7fc\uba85 clean\ud6c4 \uc800\uc7a5) ===")

def raw_load(sheet, tbl, skiprows=0, max_header_search=8):
    """\uc2e4\uc81c \ud5e4\ub354\ub97c \uc790\ub3d9 \ud0d0\uc9c0\ud574 \ub85c\ub4dc"""
    # \uc9c1\uc811 \uc9c0\uc815 skiprows \uc0ac\uc6a9
    df = pd.read_excel(FILE_PATH, sheet_name=sheet, skiprows=skiprows, header=0)
    df = df.dropna(how='all').dropna(how='all', axis=1)
    df.columns = [clean_col(c) for c in df.columns]
    df.insert(0, 'fisc_year', FISC_YEAR)
    df = assign_ddl_names(df, tbl)
    df = add_meta(df, sheet)
    write_bronze(df, tbl)

# Summaty(PL): \uc2a4\ud0b5\ub85c\uc6b0 1 (\uccab \ud589=\ud0c0\uc774\ud2c0)
print("  [Summaty(PL)]")
df = pd.read_excel(FILE_PATH, sheet_name="Summaty(PL)", skiprows=1, header=0)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.columns = [clean_col(str(c)) for c in df.columns]
df.insert(0, 'fisc_year', FISC_YEAR)
df = assign_ddl_names(df, 'setl_summary_pl')
df = add_meta(df, "Summaty(PL)")
write_bronze(df, 'setl_summary_pl')

# \uc815\uc0b0\uc11c: \uc6d4\uba85 \ud5e4\ub354 \uc18c\uc9c0 (\uc2a4\ud0b5\ub85c\uc6b0=0)
print("  [\uc815\uc0b0\uc11c]")
df = pd.read_excel(FILE_PATH, sheet_name="\uc815\uc0b0\uc11c", skiprows=0, header=None)
# \uc6d4\uba85 \ud5e4\ub354 \ud589 \ud0d0\uc9c0
for i in range(min(8, len(df))):
    row_str = ' '.join(str(v) for v in df.iloc[i] if pd.notna(v))
    if '1\uc6d4' in row_str and '2\uc6d4' in row_str:
        header_row = i; break
else:
    header_row = 0
df.columns = [clean_col(str(df.iloc[header_row][j])) if pd.notna(df.iloc[header_row][j]) else f'col_{j}' for j in range(len(df.columns))]
df = df.iloc[header_row+1:].reset_index(drop=True)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.insert(0, 'fisc_year', FISC_YEAR)
df = add_meta(df, "\uc815\uc0b0\uc11c")
df = assign_ddl_names(df, 'acc_settle_stmt')
write_bronze(df, 'acc_settle_stmt')

# \uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uacc4\ud68d): skip=4
print("  [\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uacc4\ud68d)]")
df = pd.read_excel(FILE_PATH, sheet_name="\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uacc4\ud68d)", skiprows=4, header=0)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.columns = [clean_col(str(c)) for c in df.columns]
df.insert(0, 'fisc_year', FISC_YEAR)
df = add_meta(df, "\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uacc4\ud68d)")
df = assign_ddl_names(df, 'acc_fin_plan_mth_plan')
write_bronze(df, 'acc_fin_plan_mth_plan')

# \uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uc2e4\uc801): skip=4
print("  [\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uc2e4\uc801)]")
df = pd.read_excel(FILE_PATH, sheet_name="\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uc2e4\uc801)", skiprows=4, header=0)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.columns = [clean_col(str(c)) for c in df.columns]
df.insert(0, 'fisc_year', FISC_YEAR)
df = add_meta(df, "\uc7ac\ubb34\uae30\ud68d(\uc6d4\ubcc4\uc2e4\uc801)")
df = assign_ddl_names(df, 'acc_fin_plan_mth_act')
write_bronze(df, 'acc_fin_plan_mth_act')

# \ubcc0\uace0\uae30\uc900: \uc2e4\uc81c \ub370\uc774\ud130\ub294 \ud5e4\ub354 \ub2e4\uc74c\ud589\ubd80\ud130 \uc2dc\uc791
print("  [\ubcc0\uace0\uae30\uc900]")
df_raw = pd.read_excel(FILE_PATH, sheet_name="\ubcc0\uace0\uae30\uc900", header=None, nrows=5)
for i in range(5):
    row_vals = [str(v) for v in df_raw.iloc[i] if pd.notna(v) and str(v) != 'nan']
    print(f"    row{i}: {row_vals[:5]}")
# skip=1 \uae30\uc900\uc73c\ub85c \uc2dc\ub3c4
df = pd.read_excel(FILE_PATH, sheet_name="\ubcc0\uace0\uae30\uc900", skiprows=1, header=0)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.columns = [clean_col(str(c)) for c in df.columns]
# \ud1b5\ud569\uacc4\uc815 / \uacc4\uc815\ucf54\ub4dc \ucef4\ub7fc\uc774 \uc788\ub294 \ud589\ub9cc \uc720\uc9c0
col = df.columns.tolist()
df = df.dropna(subset=[c for c in col if '\uacc4\uc815' in c][:1] or col[:1])
df.insert(0, 'fisc_year', FISC_YEAR)
df = add_meta(df, "\ubcc0\uace0\uae30\uc900")
df = assign_ddl_names(df, 'acc_vf_cost_std')
write_bronze(df, 'acc_vf_cost_std')

# \ubcc0\uace0\uac80\ud1a0: G\uc5f4 3\ud589\ubd80\ud130 \uc2e4\uc81c \ud14c\uc774\ube14 \uc2dc\uc791
print("  [\ubcc0\uace0\uac80\ud1a0]")
df_raw = pd.read_excel(FILE_PATH, sheet_name="\ubcc0\uace0\uac80\ud1a0", header=None)
col_start = 6  # G\uc5f4 = index 6
header_row_idx = 2  # 3\ud589 = index 2

# \ud5e4\ub354 \ucd94\ucd9c (G\uc5f4\ubd80\ud130 \uc5f0\uc18d\ub41c \ud5e4\ub354\ub9cc)
raw_headers = df_raw.iloc[header_row_idx, col_start:]
n_cols = 0
for v in raw_headers:
    if pd.notna(v) and str(v).strip() != '':
        n_cols += 1
    else:
        break
headers = [clean_col(str(raw_headers.iloc[i])) for i in range(n_cols)]
print(f"    \ud5e4\ub354({n_cols}\uac1c): {headers[:8]}")

# \ub370\uc774\ud130: 4\ud589(index 3)\ubd80\ud130
data = df_raw.iloc[header_row_idx+1:, col_start:col_start+n_cols].copy().reset_index(drop=True)
data.columns = headers
data = data.dropna(how='all')
data.insert(0, 'fisc_year', FISC_YEAR)
data = add_meta(data, "\ubcc0\uace0\uac80\ud1a0")
data = assign_ddl_names(data, 'acc_vf_cost_rev')
write_bronze(data, 'acc_vf_cost_rev')

# COMMAND ----------

# DBTITLE 1,Cell 6 사업부손익 (acc_bu_pl_sum + 월별 13개)
print("=== acc_bu_pl_* (\uc0ac\uc5c5\ubd80\uc190\uc775) \u2014 \uc6d4\ubcc4 \uc139\uc158 \ud30c\uc2f1 ===")
!pip install openpyxl

import openpyxl

# FILE_PATH = '/dbfs/path/to/your/excel/file.xlsx'
wb = openpyxl.load_workbook(FILE_PATH, read_only=True, data_only=True)


ws = wb["\uc0ac\uc5c5\ubd80\uc190\uc775(\uc5d1\uc140)"]

# \uc6d4\ubcc4 \ub9c8\ucee4 \uc704\uce58 (row 1-indexed, col 0-indexed)
# 3\uac1c\uc6d4\uc529 \uac00\ub85c \ubc30\uce58(col 0/10/20), 47\ud589 \uac04\uaca9\uc73c\ub85c 4\uac1c \ube14\ub85d
month_markers = [
    (49, 0, 'jan'), (49, 10, 'feb'), (49, 20, 'mar'),
    (96, 0, 'apr'), (96, 10, 'may'), (96, 20, 'jun'),
    (143, 0, 'jul'), (143, 10, 'aug'), (143, 20, 'sep'),
    (190, 0, 'oct'), (190, 10, 'nov'), (190, 20, 'dec'),
]

# \ub204\uc801\uc190\uc775(acc_bu_pl_sum): rows 5~47, cols B~J (1-idx: 2~10)
rows_acc = []
for row in ws.iter_rows(min_row=5, max_row=47, min_col=2, max_col=10, values_only=True):
    rows_acc.append(list(row))

df_sum = pd.DataFrame(rows_acc, columns=['cat_1','cat_2','gss_amount','gpu_amount','scr_amount','enc_amount','bpc_amount','total_amount','ratio_pct'])
df_sum['cat_1'] = df_sum['cat_1'].ffill()
df_sum = df_sum.dropna(subset=['cat_2'])
for c in ['gss_amount','gpu_amount','scr_amount','enc_amount','bpc_amount','total_amount','ratio_pct']:
    df_sum[c] = df_sum[c].apply(lambda v: str(v) if v is not None else None)
df_sum['cat_1'] = df_sum['cat_1'].apply(lambda v: str(v).strip() if v is not None else None)
df_sum['cat_2'] = df_sum['cat_2'].apply(lambda v: str(v).strip() if v is not None else None)
df_sum.insert(0, 'fisc_year', FISC_YEAR)
df_sum = add_meta(df_sum, "\uc0ac\uc5c5\ubd80\uc190\uc775(\uc5d1\uc140)")
df_sum = assign_ddl_names(df_sum, 'acc_bu_pl_sum')
write_bronze(df_sum, 'acc_bu_pl_sum')

# \uc6d4\ubcc4 \ud14c\uc774\ube14 (acc_bu_pl_jan ~ dec)
for m_idx, (marker_row, marker_col, m_name) in enumerate(month_markers, start=1):
    tbl_name = f'acc_bu_pl_{m_name}'
    data_start_row = marker_row + 2
    data_end_row = marker_row + 45
    col_start = marker_col + 2  # 1-indexed
    col_end = marker_col + 10
    
    rows_m = []
    for row in ws.iter_rows(min_row=data_start_row, max_row=data_end_row,
                            min_col=col_start, max_col=col_end, values_only=True):
        rows_m.append(list(row))
    
    if not rows_m:
        print(f"  \u26a0 {tbl_name}: \ub370\uc774\ud130 \uc5c6\uc74c (skip)")
        continue
    
    df_m = pd.DataFrame(rows_m, columns=['cat_1','cat_2','gss_amount','gpu_amount','scr_amount','enc_amount','bpc_amount','total_amount','ratio_pct'])
    df_m = df_m.dropna(subset=['cat_2'])
    if df_m.empty:
        print(f"  \u26a0 {tbl_name}: \uc720\ud6a8 \ub370\uc774\ud130 0\uac74 (skip)")
        continue
    df_m['cat_1'] = df_m['cat_1'].ffill()
    for c in ['gss_amount','gpu_amount','scr_amount','enc_amount','bpc_amount','total_amount','ratio_pct']:
        df_m[c] = df_m[c].apply(lambda v: str(v) if v is not None else None)
    df_m['cat_1'] = df_m['cat_1'].apply(lambda v: str(v).strip() if v is not None else None)
    df_m['cat_2'] = df_m['cat_2'].apply(lambda v: str(v).strip() if v is not None else None)
    
    df_m.insert(0, 'fisc_year', FISC_YEAR)
    df_m = add_meta(df_m, "\uc0ac\uc5c5\ubd80\uc190\uc775(\uc5d1\uc140)")
    df_m = assign_ddl_names(df_m, tbl_name)
    write_bronze(df_m, tbl_name)

wb.close()
print(f"  acc_bu_pl_* 13\uac1c \uc644\ub8cc")

# COMMAND ----------

# DBTITLE 1,검증: acc_bu_pl_jan 데이터 확인
# 검증: 수정된 브론즈 데이터 확인
display(spark.sql("SELECT cat_1, cat_2, gss_amount, gpu_amount, scr_amount FROM wonik_poc.wonik_test1_bronze.acc_bu_pl_jan ORDER BY _row_number LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Cell 7 제조비용 재고효과 12개 테이블
print("=== \uc81c\uc870\ube44\uc6a9 \uc7ac\uace0\ud6a8\uacfc 12\uac1c \ud14c\uc774\ube14 \ub85c\ub4dc ===")

df_full = pd.read_excel(FILE_PATH, sheet_name="\uc81c\uc870\ube44\uc6a9 \uc7ac\uace0\ud6a8\uacfc", header=None)

# \uc138\uc158 \uc815\uc758: (start_data_row, end_row_exclusive, section_name, table_name)
# - \ub370\uc774\ud130 \ud589 \uc2dc\uc791 = \uc2e4\uc81c \ub370\uc774\ud130 \uccab \ud589 \ubc88\ud638 (\ud5e4\ub354 \ud589 \uc81c\uc678)
SECTIONS = [
    (3,  24,  '\ud488\ubaa9\ubcc4\ub9e4\ucd9c\uc774\uc775\ubd84\uc11c',  'acc_mfg_item_gp'),
    (25, 38,  'PL\uc0c1\ub9e4\ucd9c\uc6d0\uac00',                 'acc_mfg_cogs'),
    (40, 96,  '\uc804\uccb4_\ubc1c\uc0dd\uae30\uc900',             'acc_mfg_cost_tot_acr'),
    (97, 150, 'GSS_\ubc1c\uc0dd\uae30\uc900',                   'acc_mfg_cost_gss_acr'),
    (151,204, 'GPU_\ubc1c\uc0dd\uae30\uc900',                   'acc_mfg_cost_gpu_acr'),
    (205,260, 'SCR_\ubc1c\uc0dd\uae30\uc900',                   'acc_mfg_cost_scr_acr'),
    (261,316, 'EnC_\ubc1c\uc0dd\uae30\uc900',                   'acc_mfg_cost_enc_acr'),
    (318,377, 'EPC_\ubc1c\uc0dd\uae30\uc900',                   'acc_mfg_cost_epc_acr'),
    (378,432, '\uc7ac\uace0\ud6a8\uacfc_GSS',                    'acc_inv_eff_gss'),
    (433,486, '\uc7ac\uace0\ud6a8\uacfc_GPU',                    'acc_inv_eff_gpu'),
    (487,545, '\uc7ac\uace0\ud6a8\uacfc_SCR',                    'acc_inv_eff_scr'),
]

def load_mfg_section(df_full, start, end, section, tbl):
    df = df_full.iloc[start:end].copy().reset_index(drop=True)
    df = df.dropna(how='all').dropna(how='all', axis=1)
    ncols = len(df.columns)
    base_cols = ['cat_1', 'cat_2'] if ncols >= 2 else ['cat_1']
    m_cols = [f'm{i:02d}' for i in range(1, min(13, ncols - len(base_cols) + 1))]
    extra = [f'col_{i}' for i in range(len(base_cols) + len(m_cols), ncols)]
    all_cols = (base_cols + m_cols + extra)[:ncols]
    df.columns = all_cols
    # Excel 병합셀 처리: cat_1 forward-fill (GSS/GPU/SCR 등 2행 병합)
    if 'cat_1' in all_cols:
        df['cat_1'] = df['cat_1'].ffill()
    df.insert(0, 'fisc_year', FISC_YEAR)
    df = assign_ddl_names(df, tbl)
    df = add_meta(df, "\uc81c\uc870\ube44\uc6a9 \uc7ac\uace0\ud6a8\uacfc")
    write_bronze(df, tbl)

for start, end, section, tbl in SECTIONS:
    load_mfg_section(df_full, start, end, section, tbl)

# acc_inv_eff_bu (\uc2dc\ud2b8 \uc5c6\uc74c - \ub3d9\uc77c \ub370\uc774\ud130 \ub85c BU \uc694\uc57d\uc73c\ub85c \uc800\uc7a5)
df_bu = pd.DataFrame({'fisc_year': [FISC_YEAR], 'remarks': ['\uc0ac\uc5c5\ubd80\ubcc4 \uc7ac\uace0\ud6a8\uacfc \ub370\uc774\ud130 \uc5c6\uc74c - \uc6d0\ubcf8 \uc2dc\ud2b8 \ubd80\uc5b8']})
df_bu = add_meta(df_bu, "\uc81c\uc870\ube44\uc6a9 \uc7ac\uace0\ud6a8\uacfc")
df_bu = assign_ddl_names(df_bu, 'acc_inv_eff_bu')
write_bronze(df_bu, 'acc_inv_eff_bu')

# COMMAND ----------

# DBTITLE 1,Cell 8 매출/재료비 계획 및 배부기준 6개 + 일반기준 3개
print("=== \ub9e4\ucd9c/\uc7ac\ub8cc\ube44 \uacc4\ud68d ===")

def load_plan_sheet(sheet, tbl1, tbl2):
    """\ub9e4\ucd9c/\uc7ac\ub8cc\ube44 \uacc4\ud68d: \uc0ac\uc5c5\ubd80\uc81c\ud488\uad70\ubcc4(tbl1) + System(tbl2) \ub3d9\uc77c \ub370\uc774\ud130 \uc800\uc7a5"""
    df_raw = pd.read_excel(FILE_PATH, sheet_name=sheet, header=None)
    # \ud5e4\ub354: row3=\uc6d4\ubcc4\ud5e4\ub354, row4=\uc6d4\ubc88\ud638
    # \ub370\uc774\ud130: row5+
    data_df = df_raw.iloc[5:].copy().reset_index(drop=True)
    data_df = data_df.dropna(how='all').dropna(how='all', axis=1)
    ncols = len(data_df.columns)
    col_names = ['cat_1'] + [f'm{i:02d}' for i in range(1, ncols)]
    if len(col_names) < ncols:
        col_names += [f'col_{i}' for i in range(len(col_names), ncols)]
    data_df.columns = col_names[:ncols]
    # Excel 계층구조 forward-fill (col 0=cat_1, col 1=cat_2)
    data_df.iloc[:, 0] = data_df.iloc[:, 0].ffill()
    if ncols > 1:
        data_df.iloc[:, 1] = data_df.iloc[:, 1].ffill()
    data_df.insert(0, 'fisc_year', FISC_YEAR)
    df1 = assign_ddl_names(data_df.copy(), tbl1)
    df1 = add_meta(df1, sheet)
    write_bronze(df1, tbl1)
    df2 = assign_ddl_names(data_df.copy(), tbl2)
    df2 = add_meta(df2, f"{sheet}(System)")
    write_bronze(df2, tbl2)

load_plan_sheet("26\ub144\ub9e4\ucd9c\uacc4\ud68d(\uc5d1\uc140)", 'setl_sls_pln_bu_pg', 'setl_sales_plan_sys')
load_plan_sheet("26\ub144\uc7ac\ub8cc\ube44\uacc4\ud68d(\uc5d1\uc140)", 'pln_mat_cst_bu_prd', 'acc_mat_cost_plan_sys')

# ===== \ubc30\ubd80\uae30\uc900 6\uac1c =====
print("=== \ubc30\ubd80\uae30\uc900 (acc_alloc_*) ===")
df_alloc = pd.read_excel(FILE_PATH, sheet_name="\ubc30\ubd80\uae30\uc900", header=None)

# \uc138\uc158 \uc2dc\uc791 \ud589 \ud0d0\uc9c0: '\u25ce' \ub610\ub294 '\uc5f0\uad6c\uc18c'/'\ub9e4\ucd9c'/'\uc778\uc6d0'/'\uc7ac\ub8cc\ube44'/'\uad6c\ub9e4' \ud45c\ud604
ALLOC_SECTIONS = [
    ('\uc5f0\uad6c\uc18c',   'acc_alloc_rnd'),
    ('\ub9e4\ucd9c\uacc4\ud68d',  'acc_alloc_sales_plan'),
    ('\uc778\uc6d0',     'acc_alloc_hc_plan'),
    ('\uc5f0\uad6c\uc18c',   'acc_alloc_sys_hc'),      # \uc2dc\uc2a4\ud15c\uc0ac\uc5c5\ubd80 \uc778\uc6d0\ub3c4 \ub3d9\uc77c \uc9c8\ub85c \uc800\uc7a5
    ('\uc7ac\ub8cc\ube44',   'acc_alloc_mat_cost_ref'),
    ('\uad6c\ub9e4',     'acc_alloc_pur_task'),
]
# \uc138\uc158 \ubd84\uac04: \ud5e4\ub354 \ud589 \uc704\uce58 \ud0d0\uc9c0
section_rows = []
for i in range(len(df_alloc)):
    row_vals = [str(v) for v in df_alloc.iloc[i] if pd.notna(v) and str(v) not in ('nan','')]
    if row_vals and ('\u25ce' in row_vals[0] or '\ub85c\ubc30\ubd80' in ' '.join(row_vals)):
        section_rows.append(i)

print(f"  \ubc30\ubd80\uae30\uc900 \uc138\uc158 \ud5e4\ub354 \ud589: {section_rows}")

# \uc138\uc158 \ubd84\uac04 \ub85c\ub4dc
if len(section_rows) >= 2:
    bounds = section_rows + [len(df_alloc)]
    for idx, (kw, tbl) in enumerate(ALLOC_SECTIONS):
        if idx < len(section_rows):
            s = section_rows[idx]
            e = bounds[idx+1] if idx+1 < len(bounds) else len(df_alloc)
            chunk = df_alloc.iloc[s+2:e].copy().reset_index(drop=True)
            chunk = chunk.dropna(how='all').dropna(how='all', axis=1)
            ncols = len(chunk.columns)
            if ncols == 0:
                chunk = pd.DataFrame({'fisc_year': [FISC_YEAR], 'remarks': ['no data']})
            else:
                col_names = ['category', 'gss', 'gpu', 'scr', 'enc', 'epc', 'total'] + [f'x{i}' for i in range(ncols)]
                chunk.columns = col_names[:ncols]
                chunk.insert(0, 'fisc_year', FISC_YEAR)
            chunk = add_meta(chunk, "\ubc30\ubd80\uae30\uc900")
            chunk = assign_ddl_names(chunk, tbl)
            write_bronze(chunk, tbl)
else:
    # \uc138\uc158 \ubd84\uac04 \uc2e4\ud328 - \uc804\uccb4 \ub85c\ub4dc
    df_all = df_alloc.iloc[2:].copy().reset_index(drop=True).dropna(how='all').dropna(how='all', axis=1)
    ncols = len(df_all.columns)
    col_names = ['category', 'gss', 'gpu', 'scr', 'enc', 'epc', 'total'] + [f'col_{i}' for i in range(7, ncols)]
    df_all.columns = col_names[:ncols]
    df_all.insert(0, 'fisc_year', FISC_YEAR)
    for _, tbl in ALLOC_SECTIONS:
        write_bronze(add_meta(df_all.copy(), "\ubc30\ubd80\uae30\uc900"), tbl)

# ===== \uc77c\ubc18\uae30\uc900 3\uac1c (openpyxl\ub85c \uc139\uc158 \ud5e4\ub354 \uc790\ub3d9 \ud0d0\uc9c0) =====
print("=== \uc77c\ubc18\uae30\uc900 (acc_gen_std_*) ===")
import openpyxl
wb_gen = openpyxl.load_workbook(FILE_PATH, read_only=True, data_only=True)
ws_gen = wb_gen["\uc77c\ubc18\uae30\uc900"]

# 1) \uc139\uc158 \ud5e4\ub354 \ud589/\uc5f4 \uc790\ub3d9 \ud0d0\uc9c0: '\uc6d0\uc7a5\uc870\ud68c', '\ubd80\uc11c\uad6c\ubd84', '\uc9c1/\uac04\uc811' \ud0a4\uc6cc\ub4dc \uac80\uc0c9
section_map = {}  # {keyword: (header_row, col_idx_1indexed)}
for i, row in enumerate(ws_gen.iter_rows(min_row=1, max_row=10, max_col=15, values_only=True), start=1):
    for j, v in enumerate(row):
        if v is None: continue
        vs = str(v)
        if '\uc6d0\uc7a5\uc870\ud68c' in vs:
            section_map['ledger'] = (i, j+1)  # 1-indexed col
        elif '\ubd80\uc11c\uad6c\ubd84' in vs:
            section_map['dept'] = (i, j+1)
        elif '\uc9c1' in vs and '\uac04\uc811' in vs:
            section_map['dir_indir'] = (i, j+1)

print(f"  \uc139\uc158 \ud5e4\ub354 \uc704\uce58: {section_map}")

# 2) \uac01 \uc139\uc158\uc758 \ub370\uc774\ud130 \uc5f4 \ubc94\uc704 \uacb0\uc815 (\ud5e4\ub354 \uc5f4\ubd80\ud130 2\uc5f4\uc529)
# \uc6d0\uc7a5\uc870\ud68c: \ud5e4\ub354 \uc5f4 \ub2e4\uc74c \ud589\ubd80\ud130, \ud5e4\ub354 \uc5f4 ~ \ud5e4\ub354\uc5f4+1 (\uacc4\uc815\ucf54\ub4dc, \uacc4\uc815\uba85)
# \ubd80\uc11c\uad6c\ubd84: \ud5e4\ub354 \uc5f4+1 ~ +2 (\ubd80\uc11c\uba85, \uc0ac\uc5c5\ubd80) - col E\uc5d0 '1' \uc0c1\uc218 \uc788\uc73c\ubbc0\ub85c +1\ubd80\ud130
# \uc9c1/\uac04\uc811: \ud5e4\ub354 \uc5f4 ~ +1 (\uad6c\ubd84, \uc9c1\uc811/\uac04\uc811)

def _load_gen_section(ws, data_start_row, col_start, col_end, tbl_name):
    rows = []
    for row in ws.iter_rows(min_row=data_start_row, max_row=200, min_col=col_start, max_col=col_end, values_only=True):
        rows.append(list(row))
    df = pd.DataFrame(rows, columns=['cat_1', 'cat_2'])
    df = df.dropna(subset=['cat_1'])
    if df.empty:
        print(f"  \u26a0 {tbl_name}: \ub370\uc774\ud130 0\uac74 (col {col_start}-{col_end}, row {data_start_row}+)")
        return
    df['cat_1'] = df['cat_1'].apply(lambda v: str(v).strip() if v is not None else None)
    df['cat_2'] = df['cat_2'].apply(lambda v: str(v).strip() if v is not None else None)
    df.insert(0, 'fisc_year', FISC_YEAR)
    df = add_meta(df, "\uc77c\ubc18\uae30\uc900")
    df = assign_ddl_names(df, tbl_name)
    write_bronze(df, tbl_name)

# \uc6d0\uc7a5\uc870\ud68c: \ud5e4\ub354 \uc5f4\uc5d0\uc11c \ub370\uc774\ud130 2\uc5f4 (\uacc4\uc815\ucf54\ub4dc, \uacc4\uc815\uba85)
if 'ledger' in section_map:
    hr, hc = section_map['ledger']
    _load_gen_section(ws_gen, hr+1, hc, hc+1, 'acc_gen_std_ledger')
else:
    print("  \u26a0 \uc6d0\uc7a5\uc870\ud68c \uc139\uc158 \ud5e4\ub354 \ubbf8\ubc1c\uacac")

# \ubd80\uc11c\uad6c\ubd84: \ud5e4\ub354 \uc5f4+1\ubd80\ud130 2\uc5f4 (col E\uc5d0 '1' \uc0c1\uc218 \uc788\uc73c\ubbc0\ub85c \uc2a4\ud0b5)
if 'dept' in section_map:
    hr, hc = section_map['dept']
    _load_gen_section(ws_gen, hr+1, hc, hc+1, 'acc_gen_std_dept_cat')
else:
    print("  \u26a0 \ubd80\uc11c\uad6c\ubd84 \uc139\uc158 \ud5e4\ub354 \ubbf8\ubc1c\uacac")

# \uc9c1/\uac04\uc811\uad6c\ubd84: \ud5e4\ub354 \uc5f4\uc5d0\uc11c 2\uc5f4
if 'dir_indir' in section_map:
    hr, hc = section_map['dir_indir']
    _load_gen_section(ws_gen, hr+1, hc, hc+1, 'acc_gen_std_dir_indir')
else:
    print("  \u26a0 \uc9c1/\uac04\uc811\uad6c\ubd84 \uc139\uc158 \ud5e4\ub354 \ubbf8\ubc1c\uacac")

wb_gen.close()

# COMMAND ----------

# DBTITLE 1,검증: 일반기준 3개 테이블 확인
# 검증: 일반기준 3개 테이블 데이터 확인
print("=== acc_gen_std_ledger (원장조회) ===")
display(spark.sql(f"SELECT cat_1, cat_2 FROM {B}.acc_gen_std_ledger LIMIT 10"))

print("\n=== acc_gen_std_dept_cat (부서구분) ===")
display(spark.sql(f"SELECT cat_1, cat_2 FROM {B}.acc_gen_std_dept_cat LIMIT 10"))

print("\n=== acc_gen_std_dir_indir (직/간접구분) ===")
display(spark.sql(f"SELECT cat_1, cat_2 FROM {B}.acc_gen_std_dir_indir LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Cell 9 재무기획 (acc_fin_plan_dept_perf / bep)
print("=== \uc7ac\ubb34\uae30\ud68d (acc_fin_plan_dept_perf + acc_fin_plan_bep) ===")
df_raw = pd.read_excel(FILE_PATH, sheet_name="\uc7ac\ubb34\uae30\ud68d", header=None)
print(f"  shape: {df_raw.shape}")

# \ub9e4\uc6d4 \ud5e4\ub354 \ud589 \ud0d0\uc9c0 (1\uc6d4/2\uc6d4 \ubd80\ud130)
header_row = 6  # \ud0d0\uc9c0 \uacb0\uacfc row06\uc774 '\uad6c \ubd84'\ub85c \uc2dc\uc791\ud568
df = df_raw.iloc[header_row:].copy().reset_index(drop=True)
df = df.dropna(how='all').dropna(how='all', axis=1)
col_raw = [str(df.iloc[0][j]) if pd.notna(df.iloc[0][j]) else f'col_{j}' for j in range(df.shape[1])]
df = df.iloc[1:].reset_index(drop=True)
# \ub2e4\ub2e8 \ud5e4\ub354 \uc5f4 clean
def dedup_cols(cols):
    seen = {}
    result = []
    for c in cols:
        c = clean_col(c)
        if c in seen:
            seen[c] += 1
            result.append(f'{c}_{seen[c]}')
        else:
            seen[c] = 0
            result.append(c)
    return result
df.columns = dedup_cols(col_raw)
df.insert(0, 'fisc_year', FISC_YEAR)
# \uc2dc\ud2b8 \uc804\uccb4 \ub370\uc774\ud130\ub97c \ub450 \ud14c\uc774\ube14\uc5d0 \ubaa8\ub450 \uc800\uc7a5
write_bronze(add_meta(df.copy(), "\uc7ac\ubb34\uae30\ud68d"), 'acc_fin_plan_dept_perf')
write_bronze(add_meta(df.copy(), "\uc7ac\ubb34\uae30\ud68d(BEP)"), 'acc_fin_plan_bep')

# COMMAND ----------

# DBTITLE 1,Cell 10 (Empty - 삭제되는 재배치 셀 제거)
# (placeholder - cell 8\uc5d0 \ub9e4\ud569\ud558\uc5ec \ubc30\ubd80\uae30\uc900 \ucf54\ub4dc \uc774\ub3d9)
print("Cell 8\uc5d0\uc11c \ubc30\ubd80\uae30\uc900/\uc77c\ubc18\uae30\uc900 \ucc98\ub9ac \uc644\ub8cc")

# COMMAND ----------

# DBTITLE 1,Cell 11 재무기획 2개 (acc_fin_plan_dept_perf / acc_fin_plan_bep)
print("=== \uc7ac\ubb34\uae30\ud68d (acc_fin_plan_dept_perf + acc_fin_plan_bep) ===")

# \uc7ac\ubb34\uae30\ud68d \uc2dc\ud2b8 \uc804\uccb4 \ud655\uc778
df_raw = pd.read_excel(FILE_PATH, sheet_name="\uc7ac\ubb34\uae30\ud68d", header=None)
print(f"  \uc2dc\ud2b8 \uc804\uccb4: {df_raw.shape}")
for i in range(min(10, len(df_raw))):
    row_vals = [str(v) for v in df_raw.iloc[i] if pd.notna(v) and str(v) != 'nan']
    if row_vals:
        print(f"  row{i:02d}: {row_vals[:8]}")

# \ub2e8\uc21c raw \ub85c\ub4dc: \uc2dc\ud2b8 \uc804\uccb4\ub97c acc_fin_plan_dept_perf\uc5d0 \uc800\uc7a5
# BEP\ub294 \uc138\uc158 \uad6c\ubd84 \ud6c4 \ubd84\ub9ac \uac00\ub2a5 (\ud601\uc7ac\ub294 \ud569\uc0b0 \ub85c\ub4dc)
df = pd.read_excel(FILE_PATH, sheet_name="\uc7ac\ubb34\uae30\ud68d", skiprows=0, header=None)
# \uc6d4 \ud5e4\ub354 \ud589 \ud0d0\uc9c0
header_row = 0
for i in range(min(8, len(df))):
    row_str = ' '.join(str(v) for v in df.iloc[i] if pd.notna(v))
    if '1\uc6d4' in row_str and '2\uc6d4' in row_str:
        header_row = i; break
print(f"  header_row={header_row}")
df.columns = [clean_col(str(df.iloc[header_row][j])) if pd.notna(df.iloc[header_row][j]) else f'col_{j}' for j in range(len(df.columns))]
df = df.iloc[header_row+1:].reset_index(drop=True)
df = df.dropna(how='all').dropna(how='all', axis=1)
df.insert(0, 'fisc_year', FISC_YEAR)
df_fp = add_meta(df.copy(), "\uc7ac\ubb34\uae30\ud68d")
df_fp = assign_ddl_names(df_fp, 'acc_fin_plan_dept_perf')
write_bronze(df_fp, 'acc_fin_plan_dept_perf')
# BEP \ud14c\uc774\ube14\uc5d0\ub3c4 \ub3d9\uc77c \ub370\uc774\ud130 \uadf8\ub300\ub85c \uc800\uc7a5 (\uc138\uc158 \ubd84\ub9ac \uad6c\uc870 \ud30c\uc545 \uc804)
df_bep = add_meta(df.copy(), "\uc7ac\ubb34\uae30\ud68d(BEP)")
df_bep = assign_ddl_names(df_bep, 'acc_fin_plan_bep')
write_bronze(df_bep, 'acc_fin_plan_bep')

# COMMAND ----------

# DBTITLE 1,Cell 12 최종 현황 확인
print(f"=== {CATALOG}.{BRONZE} \uacb0\uc0b0 \ud14c\uc774\ube14 \ud604\ud669 ===")
ACC_SETL_TABLES = [
    'setl_summary_pl','setl_plan_xl','setl_sales_plan_sys','setl_sls_pln_bu_pg',
    'acc_alloc_pur_task','acc_alloc_sales_plan','acc_alloc_sys_hc','acc_alloc_rnd',
    'acc_alloc_hc_plan','acc_alloc_mat_cost_ref','acc_alloc_mst',
    'acc_vf_cost_rev','acc_vf_cost_std',
] + [f'acc_bu_pl_{m}' for m in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] + [
    'acc_bu_pl_sum','acc_act_erp_gl','acc_erp_mgt_cost',
    'acc_gen_std_dept_cat','acc_gen_std_ledger','acc_gen_std_dir_indir',
    'acc_inv_eff_gpu','acc_inv_eff_gss','acc_inv_eff_scr','acc_inv_eff_bu',
    'acc_mat_cost_plan_sys','pln_mat_cst_bu_prd',
    'acc_fin_plan_dept_perf','acc_fin_plan_bep',
    'acc_fin_plan_mth_plan','acc_fin_plan_mth_act','acc_settle_stmt',
    'acc_mfg_cost_enc_acr','acc_mfg_cost_epc_acr','acc_mfg_cost_gpu_acr',
    'acc_mfg_cost_gss_acr','acc_mfg_cogs','acc_mfg_cost_scr_acr',
    'acc_mfg_cost_tot_acr','acc_mfg_item_gp',
]
total = 0
for tbl in ACC_SETL_TABLES:
    try:
        cnt = spark.table(f"{B}.{tbl}").count()
        total += cnt
        status = f"{cnt:>8,}\uac74"
    except Exception:
        status = "   (\ubbf8\uc801\uc7ac)"
    print(f"  {tbl:<45} {status}")
print(f"\n  [ TOTAL ] {total:,}\uac74  |  \uc644\ub8cc \u2713")