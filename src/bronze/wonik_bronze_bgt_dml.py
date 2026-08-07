# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,브론즈 예산 DML 적재 개요
# MAGIC %md
# MAGIC # 브론즈 예산 DML 적재
# MAGIC 대상: `wonik_poc.wonik_test1_bronze` 중 `bgt_*` 테이블 (11개, 투자 3개 제외)  
# MAGIC 소스: `/Volumes/wonik_poc/wonik_test1/volume/예산관리 PoC Data_260615.xlsx`  

# COMMAND ----------

# DBTITLE 1,Cell 2 설정 + 헬퍼 함수
# MAGIC %pip install openpyxl -q
# MAGIC
# MAGIC import pandas as pd
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC CATALOG   = "wonik_poc"
# MAGIC BRONZE    = "wonik_test1_bronze"
# MAGIC B         = f"{CATALOG}.{BRONZE}"
# MAGIC FILE_PATH = "/Volumes/wonik_poc/wonik_test1/volume/\uc608\uc0b0\uad00\ub9ac PoC Data_260615.xlsx"
# MAGIC PLAN_YEAR = "2026"
# MAGIC BASE_MONTH= "05"
# MAGIC SRC_FILE  = "\uc608\uc0b0\uad00\ub9ac PoC Data_260615.xlsx"
# MAGIC
# MAGIC def _str(s):
# MAGIC     v = str(s) if s is not None else None
# MAGIC     return None if v in ('nan','NaT','None','<NA>','') else v
# MAGIC
# MAGIC def add_meta(df, sheet, row_offset=0):
# MAGIC     df = df.copy()
# MAGIC     df['_ingest_at']  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# MAGIC     df['_source_file']= SRC_FILE
# MAGIC     df['_sheet_name'] = sheet
# MAGIC     df['_data_year']  = '2026'
# MAGIC     df['_data_month'] = '05'
# MAGIC     df['_row_number'] = [str(i + row_offset + 1) for i in range(len(df))]
# MAGIC     return df
# MAGIC
# MAGIC def write_bronze(df, tbl):
# MAGIC     """pandas DataFrame -> Delta \ud14c\uc774\ube14 OVERWRITE (target schema \ucf7c\ub7fc \uc815\ub82c)"""
# MAGIC     for c in df.columns:
# MAGIC         df[c] = df[c].apply(_str)
# MAGIC     sdf_src = spark.createDataFrame(df.astype(object).where(pd.notnull(df), None))
# MAGIC     full_tbl = f"{CATALOG}.{BRONZE}.{tbl}"
# MAGIC     target_schema = spark.table(full_tbl).schema
# MAGIC     select_exprs = []
# MAGIC     for f in target_schema:
# MAGIC         if f.name in sdf_src.columns:
# MAGIC             select_exprs.append(F.col(f.name).cast(f.dataType).alias(f.name))
# MAGIC         else:
# MAGIC             select_exprs.append(F.lit(None).cast(f.dataType).alias(f.name))
# MAGIC     sdf_src.select(*select_exprs).write.mode("overwrite").saveAsTable(full_tbl)
# MAGIC     cnt = spark.table(full_tbl).count()
# MAGIC     print(f"\u2713 {tbl:<45} {cnt:>8,}\uac74")
# MAGIC
# MAGIC print("\u2713 \uc124\uc815 \uc644\ub8cc  |  B =", B)
# MAGIC print("\u2713 FILE:", FILE_PATH)

# COMMAND ----------

# DBTITLE 1,Cell 3 마스터 4개 (bgt_acct_info / dept_biz_unit / dept_std / dept_cd)
# ── bgt_acct_info  (계정정보 600건) ────────────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="\uacc4\uc815\uc815\ubcf4", header=0).dropna(how="all")
print("\uacc4\uc815\uc815\ubcf4 columns:", list(df.columns))
# \uc608\uc0c1: \uacc4\uc815\uba85, \uacc4\uc815\ucf54\ub4dc, \uadf8\ub8f9\uba85, \uadf8\ub8f9\ucf54\ub4dc, \ucd1d\ube44\uc6a9\uacc4\uc815, \ucd1d\ube44\uc6a9\uacc4\uc815(\ube44\uad50), \uc601\uc5c5\ud65c\ub3d9\uacbd\ube44 \ud574\ub2f9 \uacc4\uc815
col = df.columns.tolist()
result = pd.DataFrame({
    'plan_year': PLAN_YEAR,
    'acct_nm':   df.iloc[:, 0],
    'acct_cd':   df.iloc[:, 1],
    'grp_nm':    df.iloc[:, 2],
    'grp_cd':    df.iloc[:, 3],
    'tot_cost_acct':      df.iloc[:, 4],
    'tot_cost_acct_comp': df.iloc[:, 5] if len(col) > 5 else None,
    'opex_acct_yn':       df.iloc[:, 6] if len(col) > 6 else None,
})
result = add_meta(result, "\uacc4\uc815\uc815\ubcf4")
write_bronze(result, 'bgt_acct_info')

# ── bgt_dept_biz_unit  (\uc870\uc9c1 88\uac74) ───────────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="\uc870\uc9c1", header=0)
if df.iloc[:, 0].isna().all(): df = df.iloc[:, 1:]
df = df.dropna(how="all")
print("\uc870\uc9c1 columns:", list(df.columns))
# \uc608\uc0c1: \ubd80\uc11c\uba85, \uc0ac\uc5c5\ubd80
col = df.columns.tolist()
dept_nm_col = [c for c in col if '\ubd80\uc11c' in c][0]
biz_unit_col = [c for c in col if '\uc0ac\uc5c5' in c][0]
result = pd.DataFrame({
    'plan_year': PLAN_YEAR,
    'biz_unit': df[biz_unit_col],
    'dept_nm':  df[dept_nm_col],
})
result = add_meta(result, "\uc870\uc9c1")
write_bronze(result, 'bgt_dept_biz_unit')

# ── bgt_dept_std  (\ubd80\uc11c\uae30\uc900 86\uac74) ──────────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="\ubd80\uc11c\uae30\uc900", header=0).dropna(how="all")
col = df.columns.tolist()
print("\ubd80\uc11c\uae30\uc900 columns:", col)
# \uc608\uc0c1: \ubd80\uc11c\uba85, \ubd80\uc11c\uae30\uc900
result = pd.DataFrame({
    'plan_year': PLAN_YEAR,
    'dept_nm':  df.iloc[:, 0],
    'dept_std': df.iloc[:, 1],
})
result = add_meta(result, "\ubd80\uc11c\uae30\uc900")
write_bronze(result, 'bgt_dept_std')

# ── bgt_dept_cd  (\ubd80\uc11c\ucf54\ub4dc 114\uac74) ──────────────────────────────────────
df = pd.read_excel(FILE_PATH, sheet_name="\ubd80\uc11c\ucf54\ub4dc", skiprows=2, header=0)
if df.iloc[:, 0].isna().all(): df = df.iloc[:, 1:]
df = df.dropna(how="all")
col = df.columns.tolist()
print("\ubd80\uc11c\ucf54\ub4dc columns:", col)
# \uc608\uc0c1: \ubd80\uc11c\uba85, \ubd80\uc11c\ucf54\ub4dc, Cost Center\uba85, C.C\ucf54\ub4dc, C.C\uad6c\ubd84, \uc0ac\uc5c5\ubd80
dept_nm_c  = [c for c in col if '\ubd80\uc11c\uba85'  in str(c)][0]
dept_cd_c  = [c for c in col if '\ubd80\uc11c\ucf54\ub4dc' in str(c)][0]
cc_nm_c    = [c for c in col if 'Center' in str(c) or 'center' in str(c) or 'CC' in str(c).upper() and '\uba85' in str(c)][0] if any('Center' in str(c) or 'center' in str(c) for c in col) else col[2]
cc_cd_c    = [c for c in col if ('CC' in str(c).upper() or 'C.C' in str(c)) and '\ucf54\ub4dc' in str(c)][0] if any(('CC' in str(c).upper() or 'C.C' in str(c)) and '\ucf54\ub4dc' in str(c) for c in col) else col[3]
cc_cat_c   = [c for c in col if ('CC' in str(c).upper() or 'C.C' in str(c)) and '\uad6c\ubd84' in str(c)][0] if any(('CC' in str(c).upper() or 'C.C' in str(c)) and '\uad6c\ubd84' in str(c) for c in col) else col[4]
biz_unit_c = [c for c in col if '\uc0ac\uc5c5' in str(c)][0] if any('\uc0ac\uc5c5' in str(c) for c in col) else col[5]
result = pd.DataFrame({
    'plan_year': PLAN_YEAR,
    'biz_unit': df[biz_unit_c],
    'dept_nm':  df[dept_nm_c],
    'dept_cd':  df[dept_cd_c],
    'cc_nm':    df[cc_nm_c],
    'cc_cd':    df[cc_cd_c],
    'cc_cat':   df[cc_cat_c],
})
result = add_meta(result, "\ubd80\uc11c\ucf54\ub4dc")
write_bronze(result, 'bgt_dept_cd')

# COMMAND ----------

# DBTITLE 1,Cell 4 bgt_plan_xl (26년계획 20840건)
# bgt_plan_xl (26년계획(엑셀))
df = pd.read_excel(FILE_PATH, sheet_name="26년계획(엑셀)", skiprows=2, header=0).dropna(how="all")
col = df.columns.tolist()
print("26년계획(엑셀) shape:", df.shape)
print("columns[:15]:", col[:15])

def find_col(kws, columns, default=None):
    for kw in kws:
        for c in columns:
            if kw in str(c): return c
    return default

def gc(kws, default_idx=None):
    """Safe column getter"""
    c = find_col(kws, col)
    if c is not None: return df[c]
    if default_idx is not None and default_idx < len(col): return df.iloc[:, default_idx]
    return pd.Series([None]*len(df), dtype=object)

result = pd.DataFrame({
    'plan_year':    PLAN_YEAR,
    'seq_no':       gc(['No','no','NO'], 0),
    'month_val':    gc(['월']),
    'cat':          gc(['구분']),
    'tot_cost_acct': gc(['총비용계정']),
    'acct_nm':      gc(['계정명']),
    'acct_cd':      gc(['계정코드']),
    'summary_desc': gc(['적요']),
    'bgt_amount':   gc(['예산금액']),
    'final_amount': gc(['최종금액']),
    'tot_cost_acct_comp': gc(['총비용계정(비교)', '비교']),
    'slip_dept':    gc(['기표부서']),
    'attr_dept':    gc(['귀속부서']),
    'mgt_dept':     gc(['관리부서']),
})
result = add_meta(result, "26년계획(엑셀)")
write_bronze(result, 'bgt_plan_xl')

# COMMAND ----------

# DBTITLE 1,Cell 5 bgt_erp_trns + bgt_act_erp_gl
# bgt_erp_trns (26년이관전용(ERP))
df = pd.read_excel(FILE_PATH, sheet_name="26년이관전용(ERP)", skiprows=1, header=0).dropna(how="all")
col = df.columns.tolist()
print("26년이관전용(ERP) columns:", col[:15])

def find_col(kws, columns, default=None):
    for kw in kws:
        for c in columns:
            if kw in str(c): return c
    return default

def gc(kws, default_idx=None):
    c = find_col(kws, col)
    if c is not None: return df[c]
    if default_idx is not None and default_idx < len(col): return df.iloc[:, default_idx]
    return pd.Series([None]*len(df), dtype=object)

result = pd.DataFrame({
    'plan_year':    PLAN_YEAR,
    'cat_1':        df.iloc[:, 0],
    'cat_2':        df.iloc[:, 1],
    'month_val':    gc(['월']),
    'cat_3':        df.iloc[:, 3] if len(col) > 3 else None,
    'yr_int_acct':  gc(['통합계정','연)통합']),
    'tot_cost_acct': gc(['총비용계정']),
    'acct_nm':       gc(['계정명']),
    'acct_cd':       gc(['계정코드']),
    'summary_desc':  gc(['적요']),
    'final_amount':  gc(['최종금액']),
    'slip_dept':     gc(['기표부서']),
    'attr_dept':     gc(['귀속부서']),
    'mgt_dept':      gc(['관리부서']),
    'tot_cost_acct_comp': gc(['비교']),
    'biz_unit':      gc(['사업부']),
    'grp_nm':        gc(['그룹명']),
    'grp_cd':        gc(['그룹코드']),
    'appr_dt':       gc(['품의일','품의_일시']),
    'appr_no':       gc(['품의번호']),
    'bgt_mgt_no':    gc(['예산관리','No예산']),
})
result = add_meta(result, "26년이관전용(ERP)")
write_bronze(result, 'bgt_erp_trns')

# bgt_act_erp_gl (26년실적(ERP원장))
df = pd.read_excel(FILE_PATH, sheet_name="26년실적(ERP원장)", skiprows=1, header=0).dropna(how="all")
col = df.columns.tolist()
print("26년실적(ERP원장) columns:", col[:15])

def gc2(kws, default_idx=None):
    c = find_col(kws, col)
    if c is not None: return df[c]
    if default_idx is not None and default_idx < len(col): return df.iloc[:, default_idx]
    return pd.Series([None]*len(df), dtype=object)

result = pd.DataFrame({
    'plan_year':    PLAN_YEAR,
    'month_val':    gc2(['월']),
    'tot_cost_acct': gc2(['총비용계정']),
    'mgt_dept':      gc2(['관리부서']),
    'slip_dt':       gc2(['회계일','전표일']),
    'acct_cd':       df['계정'] if '계정' in col else gc2(['계정코드']),
    'acct_nm':       df['계정명.1'] if '계정명.1' in col else gc2(['계정명']),
    'slip_no':       gc2(['전표번호']),
    'rmk':           gc2(['비고']),
    'crtr_id':       gc2(['작성자']),
    'curr_cd':       gc2(['통화']),
    'dr_local_amount': gc2(['차변금액(자국)']),
    'cr_local_amount': gc2(['대변금액(자국)']),
    'dept_nm':         gc2(['부서명']),
    'cc_cd':           gc2(['코스트센터','CC코드']),
    'cc_nm':           gc2(['코스트센터명','CC명']),
    'dept_cd':         gc2(['부서코드']),
    'slip_path':       gc2(['전표생성','전표경로']),
    'ref_no':          gc2(['참조번호']),
    'pjt_no':          gc2(['Project','프로젝트']),
    'tot_cost_acct_comp': gc2(['비교']),
})
result = add_meta(result, "26년실적(ERP원장)")
write_bronze(result, 'bgt_act_erp_gl')

# COMMAND ----------

# DBTITLE 1,Cell 6 현황 테이블 파서 헬퍼
# ── 3\ub2e8 \ud5e4\ub354 \ud30c\uc11c \ud568\uc218 (\uae30\uc874 build_hwanghwang_df \ub3d9\uc77c \ub85c\uc9c1) ────────────────────
def build_hwanghwang_df(sheet_name):
    df_raw = pd.read_excel(FILE_PATH, sheet_name=sheet_name, header=None)
    row1 = df_raw.iloc[1].ffill()
    row2 = df_raw.iloc[2].ffill()
    row3 = df_raw.iloc[3]

    def _norm(v):
        if not pd.notna(v): return ""
        s = str(v)
        try:
            f = float(s)
            return str(int(f)) if f == int(f) else s
        except (ValueError, OverflowError):
            return s

    headers = []
    for i in range(1, len(row2)):
        r1, r2, r3 = _norm(row1[i]), _norm(row2[i]), _norm(row3[i])
        if not r3 or r3 == "nan": headers.append(r2)
        elif r1 == r2:             headers.append(f"{r1}_{r3}")
        else:                      headers.append(f"{r1}_{r2}_{r3}")

    df_data = df_raw.iloc[4:, 1:].reset_index(drop=True)
    df_data.columns = headers
    return df_data.dropna(how="all")


def clean_col(c):
    return (str(c).strip()
            .replace(" ", "_").replace("/", "_")
            .replace("(", "").replace(")", "")
            .replace(".", "").replace("\u25a3", "")
            .replace("\u2605", "").replace("*", ""))


def map_plan_actual_df(df_raw, base_month_str):
    """
    3\ub2e8 \ud5e4\ub354 \ud30c\uc2f1 \uacb0\uacfc -> bgt_opex_st / bgt_sales_st DDL \ucf7c\ub7fc \ub9e4\ud551
    base_month_str: '5', '05' \ub4f1 \uae30\uc900\uc6d4 \ubb38\uc790\uc5f4
    """
    df = df_raw.copy()
    df.columns = [clean_col(c) for c in df.columns]
    cols = df.columns.tolist()

    # 1) \ucc28\uc6d0 \ucf7c\ub7fc
    dim_map = {
        'bu_hq':       [c for c in cols if '\uc0ac\uc5c5\ubd80' in c and '\ubcf8\ubd80' in c] or [c for c in cols if '\uc0ac\uc5c5\ubd80' in c],
        'dept_code':   [c for c in cols if c == '\ubd80\uc11c'],
        'mgt_dept_std':[c for c in cols if '\uad00\ub9ac\ubd80\uc11c' in c],
        'acct_subject':[c for c in cols if '\uacc4\uc815\uacfc\ubaa9' in c or '\uacc4\uc815\uacfc' in c],
    }
    result = {k: df[v[0]] if v else None for k, v in dim_map.items()}

    # 2) \uc6d4\ubcc4 \ucf7c\ub7fc (1~12)
    for i in range(1, 13):
        mi = f"m{i:02d}"
        result[f'{mi}_plan'] = df.get(f'{i}_\uacc4\ud68d')
        result[f'{mi}_act']  = df.get(f'{i}_\uc2e4\uc801')
        result[f'{mi}_rem']  = df.get(f'{i}_\uc794\uc5ec\uc608\uc0b0')
        result[f'{mi}_rate'] = df.get(f'{i}_\uc9d1\ud589\ub960')

    # 3) \uae30\uc900\uc6d4 \ub204\uc801 - \ucf7c\ub7fc\uba85 \ud328\ud134 \uac80\uc0c9
    bm = str(int(base_month_str))  # '05' -> '5'
    for c in cols:
        if f'\ub204\uc801{bm}\uc6d4' in c or f'\ub204\uc801({bm}\uc6d4)' in c:
            if '\uacc4\ud68d' in c and '\uc870\uc815' not in c: result['base_acc_plan'] = df[c]
            elif '\uc2e4\uc801' in c: result['base_acc_act'] = df[c]
            elif '\uc794\uc5ec' in c: result['base_acc_rem'] = df[c]
            elif '\uc9d1\ud589' in c: result['base_acc_rate'] = df[c]

    # 4) \uc5f0\uac04 \ub204\uc801
    for c in cols:
        if '\ub204\uc801' in c and ('1~12' in c or '1_12' in c or '\uc5f0\uac04' in c):
            if '\uacc4\ud68d' in c: result['yr_acc_plan'] = df[c]
            elif '\uc2e4\uc801' in c: result['yr_acc_act'] = df[c]
            elif '\uc794\uc5ec' in c: result['yr_acc_rem'] = df[c]
            elif '\uc9d1\ud589' in c: result['yr_acc_rate'] = df[c]

    return pd.DataFrame(result)


def map_trns_ovr_df(df_raw):
    """
    3\ub2e8 \ud5e4\ub354 \ud30c\uc2f1 \uacb0\uacfc -> bgt_opex_ex_st / bgt_sales_ex_st DDL \ucf7c\ub7fc \ub9e4\ud551
    """
    df = df_raw.copy()
    df.columns = [clean_col(c) for c in df.columns]
    cols = df.columns.tolist()

    dim_map = {
        'bu_hq':       [c for c in cols if '\uc0ac\uc5c5\ubd80' in c and '\ubcf8\ubd80' in c] or [c for c in cols if '\uc0ac\uc5c5\ubd80' in c],
        'dept_code':   [c for c in cols if c == '\ubd80\uc11c'],
        'mgt_dept_std':[c for c in cols if '\uad00\ub9ac\ubd80\uc11c' in c],
        'acct_subject':[c for c in cols if '\uacc4\uc815\uacfc\ubaa9' in c],
    }
    result = {k: df[v[0]] if v else None for k, v in dim_map.items()}

    for i in range(1, 13):
        mi = f"m{i:02d}"
        result[f'{mi}_trns'] = None
        result[f'{mi}_ovr']  = None
        for c in cols:
            if str(i) + '_' in c and '\uc774\uad00' in c: result[f'{mi}_trns'] = df[c]
            elif str(i) + '_' in c and '\ucd08\uacfc' in c: result[f'{mi}_ovr'] = df[c]

    # \ub204\uc801 \uc774\uad00/\ucd08\uacfc
    for c in cols:
        if '\ub204\uc801' in c and ('1~12' in c or '1_12' in c or '\uc5f0\uac04' in c):
            if '\uc774\uad00' in c: result['tot_trns'] = df[c]
            elif '\ucd08\uacfc' in c: result['tot_ovr'] = df[c]
    result.setdefault('tot_trns', None)
    result.setdefault('tot_ovr', None)

    # \ube44\uace0
    for c in cols:
        if '\ube44\uace0' in c: result['remarks'] = df[c]; break
    result.setdefault('remarks', None)

    return pd.DataFrame(result)

print("\u2713 \ud604\ud669 \ud14c\uc774\ube14 \ud30c\uc11c \ud5ec\ud37c \ub85c\ub4dc \uc644\ub8cc")

# COMMAND ----------

# DBTITLE 1,Cell 7 bgt_opex_st + bgt_opex_ex_st (경상경비)
# ── bgt_opex_st  (\ud604\ud669(\uacbd\uc0c1\uacbd\ube44) 1,320\uac74) ─────────────────────────────────
df_raw = build_hwanghwang_df("\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)")
print("\ud604\ud669(\uacbd\uc0c1\uacbd\ube44) shape:", df_raw.shape)
print("columns[:10]:", list(df_raw.columns[:10]))
print("columns[-10:]:", list(df_raw.columns[-10:]))

result = map_plan_actual_df(df_raw, BASE_MONTH)
result.insert(0, 'plan_year', PLAN_YEAR)
result.insert(1, 'base_month', BASE_MONTH)
result = add_meta(result, "\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)")
print("bgt_opex_st \ub9e4\ud551 columns:", list(result.columns[:10]))
write_bronze(result, 'bgt_opex_st')

# ── bgt_opex_ex_st  (\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)_\uc774\uad00\uc804\uc6a9\ucd08\uacfc 1,254\uac74) ─────────────────
df_raw2 = build_hwanghwang_df("\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)_\uc774\uad00\uc804\uc6a9\ucd08\uacfc")
print("\n\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)_\uc774\uad00\uc804\uc6a9\ucd08\uacfc shape:", df_raw2.shape)
print("columns[:10]:", list(df_raw2.columns[:10]))

result2 = map_trns_ovr_df(df_raw2)
result2.insert(0, 'plan_year', PLAN_YEAR)
result2.insert(1, 'base_month', BASE_MONTH)
result2 = add_meta(result2, "\ud604\ud669(\uacbd\uc0c1\uacbd\ube44)_\uc774\uad00\uc804\uc6a9\ucd08\uacfc")
write_bronze(result2, 'bgt_opex_ex_st')

# COMMAND ----------

# DBTITLE 1,Cell 8 bgt_sales_st + bgt_sales_ex_st (영업활동경비)
# ── bgt_sales_st  (\ud604\ud669(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44) 304\uac74) ──────────────────────────
df_raw = build_hwanghwang_df("\ud604\ud669(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44)")
print("\ud604\ud669(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44) shape:", df_raw.shape)
print("columns[:10]:", list(df_raw.columns[:10]))

result = map_plan_actual_df(df_raw, BASE_MONTH)
result.insert(0, 'plan_year', PLAN_YEAR)
result.insert(1, 'base_month', BASE_MONTH)
result = add_meta(result, "\ud604\ud669(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44)")
write_bronze(result, 'bgt_sales_st')

# ── bgt_sales_ex_st  (\uc694\uc57d(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44) 32\uac74) ─────────────────────────
# \uc18c\uc2a4 \uad6c\uc870: \uad6c\ubd84, \uacc4\uc815\uacfc\ubaa9, \ub204\uc801(1~2\uc6d4) \uacc4\ud68d/\uc2e4\uc801/\uc794\uc5ec/\uc9d1\ud589, \ub204\uc801(1~12\uc6d4) \uacc4\ud68d/\uc2e4\uc801/\uc794\uc5ec/\uc9d1\ud589
# -> \uc6d4\ubcc4 \uc774\uad00/\ucd08\uacfc \ub370\uc774\ud130 \uc5c6\uc74c; m01~m12_trns/ovr\ub294 NULL\ub85c \uc801\uc7ac
try:
    df_raw = pd.read_excel(FILE_PATH, sheet_name="\uc694\uc57d(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44)", header=None)
    # 2\ub2e8 \ud5e4\ub354 \ud30c\uc2f1
    header_row_idx = None
    for idx in range(min(10, len(df_raw))):
        row_vals = df_raw.iloc[idx].astype(str).tolist()
        if '\uad6c\ubd84' in row_vals or '\uacc4\uc815\uacfc\ubaa9' in row_vals:
            header_row_idx = idx; break
    if header_row_idx is None: raise ValueError("\ud5e4\ub354\uc5c6\uc74c")
    row_h1 = df_raw.iloc[header_row_idx].ffill()
    row_h2 = df_raw.iloc[header_row_idx + 1]
    headers = []
    for i in range(len(row_h1)):
        r1 = str(row_h1[i]) if pd.notna(row_h1[i]) else ''
        r2 = str(row_h2[i]) if pd.notna(row_h2[i]) else ''
        if r2 in ('nan','None','') or r2 == r1: headers.append(r1)
        else: headers.append(f"{r1}_{r2}")
    headers = [clean_col(h) for h in headers]
    df = df_raw.iloc[header_row_idx + 2:].reset_index(drop=True)
    df.columns = headers
    df = df.dropna(how='all')
    print("\uc694\uc57d(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44) shape:", df.shape, "columns:", list(df.columns))

    # DDL \ud14c\uc774\ube14\uc5d0 \uc5c6\ub294 \ucf7c\ub7fc -> NULL\ub85c \ucee4\ubc84
    result = pd.DataFrame({'plan_year': PLAN_YEAR, 'base_month': BASE_MONTH})
    acct_c = [c for c in df.columns if '\uacc4\uc815' in c or '\uacfc\ubaa9' in c]
    cat_c  = [c for c in df.columns if '\uad6c\ubd84' in c and c != '\uad6c\ubd84']
    result['bu_hq'] = None; result['dept_code'] = None; result['mgt_dept_std'] = None
    result['acct_subject'] = df[acct_c[0]] if acct_c else None
    # \uc6d4\ubcc4 m01~m12 trns/ovr NULL\ub85c \uc138\ud305
    for i in range(1, 13):
        mi = f"m{i:02d}"
        result[f'{mi}_trns'] = None; result[f'{mi}_ovr'] = None
    # \ub204\uc801 \ub370\uc774\ud130 \ub9e4\ud551 (\uac00\uc7a5 \uadfc\uc811 \ucf7c\ub7fc)
    for c in df.columns:
        if '1~2' in c or '1~' in c:
            if '\uacc4\ud68d' in c: result['base_acc_trns'] = df[c]
            elif '\uc2e4\uc801' in c: result['base_acc_ovr'] = df[c]
        elif '1~12' in c or '\uc5f0\uac04' in c:
            if '\uacc4\ud68d' in c: result['yr_acc_trns'] = df[c]
            elif '\uc2e4\uc801' in c: result['yr_acc_ovr'] = df[c]
    result.setdefault('base_acc_trns', None); result.setdefault('base_acc_ovr', None)
    result.setdefault('yr_acc_trns', None);   result.setdefault('yr_acc_ovr', None)
    result = add_meta(result, "\uc694\uc57d(\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44)")
    write_bronze(result, 'bgt_sales_ex_st')
except Exception as e:
    print(f"bgt_sales_ex_st \uc2a4\ud0b5: {e}")

# COMMAND ----------

# DBTITLE 1,Cell 9 한글 테이블 DROP + 최종 확인
# ── \ud55c\uae00 \uc774\ub984 \ube0c\ub860\uc988 \ud14c\uc774\ube14 DROP ───────────────────────────────────────────────
KOREAN_TABLES = [
    '\uc608\uc0b0_\uacc4\uc815\uc815\ubcf4',
    '\uc608\uc0b0_\uacc4\ud68d_\uc5d1\uc140',
    '\uc608\uc0b0_\ubd80\uc11c\uae30\uc900',
    '\uc608\uc0b0_\ubd80\uc11c\ucf54\ub4dc',
    '\uc608\uc0b0_\uc2e4\uc801_erp\uc6d0\uc7a5',
    '\uc608\uc0b0_\uc774\uad00\uc804\uc6a9_erp',
    '\uc608\uc0b0_\uc870\uc9c1',
    '\uc608\uc0b0_\ud604\ud669_\uacbd\uc0c1\uacbd\ube44',
    '\uc608\uc0b0_\ud604\ud669_\uacbd\uc0c1\uacbd\ube44_\uc774\uad00\uc804\uc6a9\ucd08\uacfc',
    '\uc608\uc0b0_\ud604\ud669_\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44',
    '\uc608\uc0b0_\ud604\ud669_\uc601\uc5c5\ud65c\ub3d9\uacbd\ube44_\uc774\uad00\uc804\uc6a9\ucd08\uacfc',
]
for t in KOREAN_TABLES:
    try:
        spark.sql(f"DROP TABLE IF EXISTS `{B}`.`{t}`")
        print(f"\u2713 DROP: {t}")
    except Exception as e:
        print(f"\u26a0 DROP \uc2e4\ud328 {t}: {e}")

print("\n=== \ube0c\ub860\uc988 bgt_* \ud14c\uc774\ube14 \ucd5c\uc885 \ud604\ud669 ===")
rows = spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{BRONZE}`").collect()
bgt_tables = sorted([r.tableName for r in rows if r.tableName.startswith('bgt_') and not r.isTemporary])
print(f"  bgt_* \ud14c\uc774\ube14 {len(bgt_tables)}\uac1c")
total = 0
for t in bgt_tables:
    cnt = spark.table(f"`{B}`.`{t}`").count()
    total += cnt
    print(f"  {t:<45} {cnt:>8,}\uac74")
print(f"  {'TOTAL':<45} {total:>8,}\uac74")