# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,설정 + CSV 파싱
# pipeline_rfq_strategy: 신규 RFQ 자동 전략 파이프라인
# 트리거: Volume(/Volumes/wonik_poc/wonik_silver/rfq_inbox/) CSV 도착
# Compute: ML Runtime (XGBoost 모델 로드 필수)

import os, glob
from pyspark.sql import functions as F
from datetime import datetime

# === 설정 ===
CATALOG = "wonik_poc"
SCHEMA  = "wonik_silver"
PREFIX  = f"{CATALOG}.{SCHEMA}"

VOLUME_PATH     = "/Volumes/wonik_poc/wonik_raw/rfq_inbox"
INPUT_TABLE     = f"{PREFIX}.silver_new_rfq_parsed_input"
LIKELY_COMP_TABLE = f"{PREFIX}.gold_rfq_likely_competitor"
SIMILAR_CASE_TABLE = f"{PREFIX}.gold_rfq_similar_case_result"
COST_REBASE_TABLE  = f"{PREFIX}.gold_rfq_cost_rebased_case"
SCENARIO_TABLE     = f"{PREFIX}.gold_rfq_quote_scenario"
RECOMMEND_TABLE    = f"{PREFIX}.gold_rfq_strategy_recommendation"

VS_ENDPOINT = "rfq_strategy_vs_endpoint"
VS_INDEX    = f"{PREFIX}.silver_rfq_case_corpus_vs_index"
MODEL_URI   = f"models:/{PREFIX}.rfq_win_model@champion"

ALLOWED_ACTION_TYPES = [
    "EPC_RELATIONSHIP", "VENDOR_REGISTRATION", "PRICE_STRATEGY",
    "TECH_DIFFERENTIATION", "LOCAL_SERVICE", "EARLY_ENGAGEMENT",
    "COMPETITOR_DEFENSE", "NO_ACTION_MONITORING"
]

# === STEP 1: Volume에서 CSV 읽기 (read_files — UC Volume 전용) ===
raw_df = spark.sql(f"""
    SELECT * FROM read_files(
        '{VOLUME_PATH}/',
        format => 'csv',
        header => 'true',
        inferSchema => 'true'
    )
""")
assert raw_df.count() > 0, f"[ERROR] {VOLUME_PATH}에 CSV 파일이 없거나 비어있습니다."
print(f"신규 RFQ {raw_df.count()}건 읽음")

# CSV 파싱 + 타겟 테이블 스키마 매칭
rfq_df = raw_df \
    .withColumn("qty", F.col("qty").cast("double")) \
    .withColumn("budget_hint", F.col("budget_hint").cast("double")) \
    .withColumn("required_lead_time_wks", F.col("required_lead_time_wks").cast("int"))

# 필수 컨텍스트 컨츌 + 타임스탬프
rfq_df = rfq_df.withColumn(
    "rfq_text_for_embedding",
    F.concat_ws(" | ",
        F.concat(F.lit("Customer: "), F.col("cust_nm")),
        F.concat(F.lit("Project: "), F.col("pjt_id")),
        F.concat(F.lit("Business Unit: "), F.col("biz_unit")),
        F.concat(F.lit("Product: "), F.col("product_cat")),
        F.concat(F.lit("Region: "), F.col("target_region")),
        F.concat(F.lit("EPC: "), F.col("epc_id")),
        F.concat(F.lit("Quantity: "), F.col("qty").cast("string")),
        F.concat(F.lit("Required Lead Time: "), F.col("required_lead_time_wks").cast("string"), F.lit(" weeks")),
        F.concat(F.lit("Spec: "), F.col("spec_summary")),
        F.concat(F.lit("Evaluation: "), F.col("evaluation_criteria")),
        F.concat(F.lit("Budget Hint: "), F.col("budget_hint").cast("string")),
    )
).withColumn("received_at", F.current_timestamp()) \
 .withColumn("source_note", F.lit("auto-pipeline")) \
 .withColumn("_update_at", F.current_timestamp())

# INPUT_TABLE에 적재 (MERGE — 중복 방지)
rfq_df.drop("_rescued_data").createOrReplaceTempView("stg_new_rfq")
spark.sql(f"""
    MERGE INTO {INPUT_TABLE} AS t
    USING stg_new_rfq AS s
    ON t.new_rfq_id = s.new_rfq_id
    WHEN NOT MATCHED THEN INSERT *
""")
# 실제 신규 건만 필터 (이미 처리된 RFQ 제외)
existing_ids = set(r[0] for r in spark.sql(f"""
    SELECT DISTINCT new_rfq_id FROM {SCENARIO_TABLE}
""").collect())
all_ids = [r[0] for r in rfq_df.select("new_rfq_id").collect()]
new_rfq_ids = [rid for rid in all_ids if rid not in existing_ids]
print(f"적재 완료: {len(new_rfq_ids)}건 — {new_rfq_ids}")

# 처리 완료된 파일을 별도 볼륨으로 이동 (inbox 밖 — 재귀 스캔 방지)
PROCESSED_PATH = "/Volumes/wonik_poc/wonik_raw/rfq_processed"
try:
    files = dbutils.fs.ls(VOLUME_PATH)
    for f in files:
        if f.name.endswith(".csv"):
            dbutils.fs.mv(f.path, f"{PROCESSED_PATH}/{f.name}")
            print(f"파일 이동: rfq_processed/{f.name}")
except Exception as e:
    print(f"[WARN] 파일 이동 실패 (수동 정리 필요): {e}")

# COMMAND ----------

# DBTITLE 1,STEP 2: 경쟁사 추정 + AI Search 유사사례 + 원가 재산정
# STEP 2: 경쟁사 추정 + AI Search 유사사례 + 원가 재산정
# databricks-vectorsearch는 Job 클러스터 라이브러리로 사전 설치됨
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F

for rfq_id in new_rfq_ids:
    new_rfq = spark.table(INPUT_TABLE).filter(F.col("new_rfq_id") == rfq_id)
    rfq = new_rfq.first().asDict()
    cust_nm, biz_unit = rfq["cust_nm"], rfq["biz_unit"]

    # --- 2A: 경쟁사 추정 (M3 gap 기반) ---
    comp_df = spark.sql(f"""
        SELECT g.competitor_nm AS comp_nm,
               g.m3_threat_score, g.win_rate_vs_wonik_pct,
               g.tech_score, g.price_score, g.service_infra_score, g.epc_network_score,
               COUNT(cr.reference_id) AS comp_ref_count
        FROM {PREFIX}.sales_competitor_gap g
        LEFT JOIN {PREFIX}.sales_competitor_reference cr
            ON cr.competitor_nm = g.competitor_nm AND cr.cust_nm = g.cust_nm
        WHERE g.cust_nm = '{cust_nm}' AND g.active_competitor_yn = true
        GROUP BY g.competitor_nm, g.m3_threat_score, g.win_rate_vs_wonik_pct,
                 g.tech_score, g.price_score, g.service_infra_score, g.epc_network_score
        ORDER BY g.m3_threat_score DESC
        LIMIT 5
    """)
    likely_rows = [{
        "new_rfq_id": rfq_id, "comp_nm": r["comp_nm"],
        "participation_likelihood": round(float(r["win_rate_vs_wonik_pct"]) / 100.0, 4),
        "reason": f"threat={r['m3_threat_score']}, ref={r['comp_ref_count']}, win_rate={r['win_rate_vs_wonik_pct']}",
        "tech_score": float(r["tech_score"]), "price_score": float(r["price_score"]),
        "service_infra_score": float(r["service_infra_score"]),
        "epc_network_score": float(r["epc_network_score"]),
        "comp_ref_count": int(r["comp_ref_count"]),
    } for r in comp_df.collect()]
    if likely_rows:
        spark.createDataFrame(likely_rows).withColumn("_update_at", F.current_timestamp()) \
            .write.mode("append").format("delta").saveAsTable(LIKELY_COMP_TABLE)

    # --- 2B: AI Search 유사 사례 ---
    query_text = rfq["rfq_text_for_embedding"]
    results = VectorSearchClient(disable_notice=True).get_index(VS_ENDPOINT, VS_INDEX).similarity_search(
        query_text=query_text,
        columns=["doc_id", "rfq_id", "cust_nm", "pjt_nm", "result_yn",
                 "reason_code", "total_quoted_amt", "final_margin_pct", "competitor_context", "target_region"],
        num_results=5, filters={"biz_unit": biz_unit}
    )
    similar_rows = [{
        "new_rfq_id": rfq_id, "similar_rfq_id": r[1], "similarity_score": r[-1],
        "past_cust_nm": r[2], "past_project_nm": r[3], "past_result_yn": r[4],
        "past_reason_code": r[5], "past_quoted_amt": float(r[6] or 0),
        "past_margin_pct": float(r[7] or 0),
        "past_competitors": r[8] or "", "lesson_learned": f"region={r[9]}",
        "evidence_case_id": r[0]
    } for r in results["result"]["data_array"]]
    if similar_rows:
        spark.createDataFrame(similar_rows).withColumn("_update_at", F.current_timestamp()) \
            .write.mode("append").format("delta").saveAsTable(SIMILAR_CASE_TABLE)

    # --- 2C: 원가 재산정 (Win 사례 우선 + SQL 보충) ---
    similar_win_ids = [r["similar_rfq_id"] for r in similar_rows if r["past_result_yn"] == "Win"]
    current_bom_idx = spark.sql(f"""
        SELECT ROUND(AVG(unit_cost_index), 4)
        FROM {PREFIX}.sales_bom_cost_index
        WHERE base_mth = (SELECT MAX(base_mth) FROM {PREFIX}.sales_bom_cost_index)
    """).first()[0] or 1.0

    ids_sql = ','.join(repr(x) for x in similar_win_ids) if similar_win_ids else "'__NONE__'"
    spark.sql(f"""
    INSERT INTO {COST_REBASE_TABLE}
    WITH quote_items AS (
        SELECT q.rfq_id AS similar_rfq_id,
            DATE_FORMAT(DATE_TRUNC('MONTH', q.quot_dt), 'yyyyMM') AS base_mth,
            qi.material_cd, qi.qty, CAST(qi.unit_price AS DOUBLE) AS past_unit_price
        FROM {PREFIX}.sales_quotation q
        JOIN {PREFIX}.sales_quotation_item qi ON q.quot_id = qi.quot_id
        WHERE q.rfq_id IN ({ids_sql})
        UNION ALL
        SELECT * FROM (
            SELECT q.rfq_id AS similar_rfq_id,
                DATE_FORMAT(DATE_TRUNC('MONTH', q.quot_dt), 'yyyyMM') AS base_mth,
                qi.material_cd, qi.qty, CAST(qi.unit_price AS DOUBLE) AS past_unit_price
            FROM {PREFIX}.sales_quotation q
            JOIN {PREFIX}.sales_quotation_item qi ON q.quot_id = qi.quot_id
            JOIN {PREFIX}.sales_win_loss_history wl ON wl.quot_id = q.quot_id
            WHERE q.cust_nm = '{cust_nm}' AND q.biz_unit = '{biz_unit}'
              AND wl.result_yn = 'Win'
              AND q.rfq_id NOT IN ({ids_sql})
            ORDER BY q.quot_dt DESC LIMIT 20
        )
    ),
    hist_idx AS (
        SELECT base_mth, ROUND(AVG(unit_cost_index), 4) AS past_cost_index
        FROM {PREFIX}.sales_bom_cost_index GROUP BY base_mth
    )
    SELECT '{rfq_id}', qi.similar_rfq_id, qi.material_cd, qi.past_unit_price,
        h.past_cost_index, {current_bom_idx}, 
        ROUND(qi.past_unit_price * {current_bom_idx} / h.past_cost_index, 4),
        ROUND(qi.qty * qi.past_unit_price * {current_bom_idx} / h.past_cost_index, 4),
        ROUND(({current_bom_idx} / h.past_cost_index - 1) * 100, 2),
        current_timestamp()
    FROM quote_items qi
    JOIN hist_idx h ON qi.base_mth = h.base_mth
    WHERE h.past_cost_index > 0
    """)
    print(f"  [{rfq_id}] 경쟁사 {len(likely_rows)}건 | 유사사례 {len(similar_rows)}건 | 원가재산정 완료")

# COMMAND ----------

# DBTITLE 1,STEP 3: 시나리오 21종 + ML 예측 + 최종 추천
# STEP 3: 시나리오 20종 생성 + ML 예측 + 최종 추천
import pandas as pd
import mlflow.xgboost
from sklearn.preprocessing import LabelEncoder

mlflow.set_registry_uri("databricks-uc")
_ml_model = mlflow.xgboost.load_model(MODEL_URI)
print(f"모델 로드: {MODEL_URI}")

# 훈련 데이터 기반 LabelEncoder 재현
_EXCL = ["order_id", "cust_nm", "biz_unit", "win_loss_label", "win_flag"]
_train = spark.table(f"{PREFIX}.ml_rfq_win_with_quot_train").toPandas()
_feat_cols = [c for c in _train.columns if c not in _EXCL]
_X_ref = _train[_feat_cols].copy()
for _col in _X_ref.select_dtypes(include=["object"]).columns:
    _X_ref[_col] = pd.to_numeric(_X_ref[_col], errors="ignore")
_encoders = {}
for _col in _X_ref.select_dtypes(include=["object"]).columns:
    _le = LabelEncoder(); _le.fit(_X_ref[_col].fillna("__NA__").astype(str))
    _encoders[_col] = _le; _X_ref[_col] = _le.transform(_X_ref[_col].fillna("__NA__").astype(str))
_X_ref = _X_ref.fillna(_X_ref.median(numeric_only=True))
_ref_med = _X_ref.median(numeric_only=True)

# 시나리오 룰 (설정 셀의 SCENARIO_RULES 사용)
# 시나리오 룰: m4_scenario_rules 테이블에서 동적 로드
import json as _json
_rules_df = spark.table(f"{PREFIX}.m4_scenario_rules").filter("is_active = true").collect()
SCENARIO_RULES = {}
for _r in _rules_df:
    SCENARIO_RULES[_r["scenario_id"]] = {
        "margin_delta_pct": float(_r["margin_delta_pct"]),
        "discount_delta_pct": float(_r["discount_delta_pct"]),
        "lead_time_delta_wks": int(_r["lead_time_delta_wks"]),
        "feature_overrides": _json.loads(_r["feature_overrides"]) if _r["feature_overrides"] else {},
    }
print(f"시나리오 {len(SCENARIO_RULES)}개 로드 완료")

if False:  # 레거시 하드코딩 — 테이블 전환 검증 후 삭제 예정
  _REMOVED = {
    "PRICE_WAR": {"margin_delta_pct": -6, "lead_time_delta_wks": 0, "discount_delta_pct": 3, "feature_overrides": {}},
    "PRICE_STANDARD": {"margin_delta_pct": 0, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {}},
    "PRICE_PREMIUM": {"margin_delta_pct": 6, "lead_time_delta_wks": 0, "discount_delta_pct": -2, "feature_overrides": {}},
    "FAST_DELIVERY": {"margin_delta_pct": -1, "lead_time_delta_wks": -4, "discount_delta_pct": 0, "feature_overrides": {"offered_lead_time": -3}},
    "EXTENDED_DELIVERY": {"margin_delta_pct": 2, "lead_time_delta_wks": 4, "discount_delta_pct": 0, "feature_overrides": {"offered_lead_time": 3}},
    "WITH_AUTOMATION": {"margin_delta_pct": 1, "lead_time_delta_wks": 1, "discount_delta_pct": 0, "feature_overrides": {"has_automation_yn": "Y"}},
    "VENDOR_UPGRADE": {"margin_delta_pct": 0, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"wonik_vendor_status": "Approved"}},
    "REFERENCE_PUSH": {"margin_delta_pct": 0, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"wonik_ref_cnt_cust": 3, "wonik_hist_win_rate_pct": 15}},
    "FULL_CAPABILITY": {"margin_delta_pct": 1, "lead_time_delta_wks": -1, "discount_delta_pct": 0, "feature_overrides": {"has_automation_yn": "Y", "wonik_vendor_status": "Approved", "wonik_ref_cnt_cust": 2}},
    "COMPETITOR_DEFENSE": {"margin_delta_pct": -2, "lead_time_delta_wks": 0, "discount_delta_pct": 2, "feature_overrides": {"active_competitor_count": -1}},
    "EPC_LEVERAGE": {"margin_delta_pct": 1, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"wonik_epc_score": 2}},
    "TECH_LEAD": {"margin_delta_pct": 3, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"wonik_tech_score": 2, "has_automation_yn": "Y"}},
    "COMPETITOR_BLOCK": {"margin_delta_pct": 0, "lead_time_delta_wks": -2, "discount_delta_pct": 1, "feature_overrides": {"active_competitor_count": -2, "wonik_hist_win_rate_pct": 10}},
    "RISK_MITIGATION": {"margin_delta_pct": 1, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"high_risk_cnt": -99, "medium_risk_cnt": -99}},
    "RISK_SHARED": {"margin_delta_pct": 2, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"high_risk_cnt": -1}},
    "RISK_PASS_THROUGH": {"margin_delta_pct": 4, "lead_time_delta_wks": 0, "discount_delta_pct": -1, "feature_overrides": {"high_risk_cnt": -99, "medium_risk_cnt": -99}},
    "STRATEGIC_PURSUIT": {"margin_delta_pct": -1, "lead_time_delta_wks": -2, "discount_delta_pct": 2, "feature_overrides": {"has_automation_yn": "Y", "wonik_vendor_status": "Approved", "high_risk_cnt": -99, "active_competitor_count": -1}},
    "PROFIT_OPTIMIZED": {"margin_delta_pct": 6, "lead_time_delta_wks": 2, "discount_delta_pct": -2, "feature_overrides": {"high_risk_cnt": -99, "medium_risk_cnt": -99, "wonik_epc_score": 1}},
    "RELATIONSHIP_PLAY": {"margin_delta_pct": 0, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {"wonik_vendor_status": "Approved", "wonik_ref_cnt_cust": 3, "wonik_hist_win_rate_pct": 20}},
    "SERVICE_DOMINANCE": {"margin_delta_pct": 0, "lead_time_delta_wks": -2, "discount_delta_pct": 0, "feature_overrides": {"wonik_service_score": 4, "active_competitor_count": -1}},
    "NO_BID": {"margin_delta_pct": 0, "lead_time_delta_wks": 0, "discount_delta_pct": 0, "feature_overrides": {}},
}

for rfq_id in new_rfq_ids:
    rfq = spark.table(INPUT_TABLE).filter(F.col("new_rfq_id") == rfq_id).first().asDict()
    cust_nm, biz_unit = rfq["cust_nm"], rfq["biz_unit"]

    # 원가 기반: 유사사례별 자재 합산 → 사례간 평균 = 기기 1대 원가
    cost_row = spark.sql(f"""
        SELECT ROUND(AVG(case_total), 2) AS unit_cost,
               ROUND(AVG(avg_increase), 2) AS avg_cost_increase_pct
        FROM (
            SELECT similar_rfq_id,
                   SUM(rebased_total_cost) AS case_total,
                   AVG(cost_increase_pct) AS avg_increase
            FROM {COST_REBASE_TABLE}
            WHERE new_rfq_id = '{rfq_id}'
            GROUP BY similar_rfq_id
        )
    """).first()
    unit_cost = cost_row[0] if cost_row and cost_row[0] else round((rfq["budget_hint"] or 0) * 0.80, 2)
    avg_cost_increase_pct = cost_row[1] if cost_row and cost_row[1] else 0.0
    qty = int(rfq.get("qty", 1) or 1)
    base_margin, min_margin = 18.0, 10.0

    # 시나리오 생성: 단가 × 수량 = 총 견적가
    scenario_rows = []
    for stype, rule in SCENARIO_RULES.items():
        margin = round(max(min_margin, base_margin + rule["margin_delta_pct"]), 2)
        lead = max(1, int(rfq["required_lead_time_wks"] + rule["lead_time_delta_wks"]))
        unit_quote = round(unit_cost / (1 - margin / 100.0), 2) if unit_cost else None
        total_quote = round(unit_quote * qty, 2) if unit_quote else None
        total_cost = round(unit_cost * qty, 2)
        profit = round(total_quote - total_cost, 2) if total_quote else 0.0
        scenario_rows.append({
            "new_rfq_id": rfq_id, "scenario_id": f"{rfq_id}_{stype}",
            "scenario_type": stype, "recommended_quote_amt": total_quote,
            "target_margin_pct": margin, "min_acceptable_margin_pct": min_margin,
            "offered_lead_time_wks": lead, "expected_win_probability": 0.0,
            "expected_profit_amt": profit, "pursue_recommendation": "TBD",
            "pricing_note": f"unit_cost={unit_cost}, unit_quote={unit_quote}, qty={qty}, cost_increase={avg_cost_increase_pct}%",
        })
    scenario_pdf = pd.DataFrame(scenario_rows)

    # ML 예측
    likely_comp_pdf = spark.sql(f"SELECT * FROM {LIKELY_COMP_TABLE} WHERE new_rfq_id = '{rfq_id}'").toPandas()
    _bom_val = spark.sql(f"SELECT ROUND(AVG(unit_cost_index),4) FROM {PREFIX}.sales_bom_cost_index WHERE base_mth=(SELECT MAX(base_mth) FROM {PREFIX}.sales_bom_cost_index)").first()[0] or 1.0
    _max_comp_win = float(likely_comp_pdf["participation_likelihood"].max() * 100) if len(likely_comp_pdf) > 0 else 50.0
    _avg_price = float(likely_comp_pdf["price_score"].mean()) if len(likely_comp_pdf) > 0 else 5.0
    _comp_cnt = max(1, len(likely_comp_pdf[likely_comp_pdf["participation_likelihood"] >= 0.4])) if len(likely_comp_pdf) > 0 else 2

    _cm_row = spark.sql(f"SELECT wonik_vendor_status, strategic_tier FROM {PREFIX}.sales_customer_master WHERE cust_nm='{cust_nm}' LIMIT 1").first()
    _wk_row = spark.sql(f"SELECT tech_score, price_score, service_infra_score, epc_network_score FROM {PREFIX}.sales_wonik_capability WHERE biz_unit='{biz_unit}' AND region='Global' LIMIT 1").first()
    _ref_cust = spark.sql(f"SELECT COUNT(*) FROM {PREFIX}.sales_wonik_reference WHERE cust_nm='{cust_nm}'").first()[0] or 0
    _hwrr = spark.sql(f"SELECT ROUND(100.0*SUM(win_cnt)/NULLIF(SUM(total_rfq_cnt),0),1) FROM {PREFIX}.gold_rfq_success_pattern WHERE cust_nm='{cust_nm}' AND biz_unit='{biz_unit}'").first()[0]

    # Mission3 기반 action 추천 (경쟁사 점수 기반)
    recommended_actions = []
    for _, _comp_row in likely_comp_pdf.iterrows():
        if _comp_row.get("participation_likelihood", 0) >= 0.65:
            recommended_actions.append("COMPETITOR_DEFENSE")
        if _comp_row.get("price_score", 0) >= 8:
            recommended_actions.append("PRICE_STRATEGY")
        if _comp_row.get("tech_score", 0) >= 8:
            recommended_actions.append("TECH_DIFFERENTIATION")
        if _comp_row.get("service_infra_score", 0) >= 8:
            recommended_actions.append("LOCAL_SERVICE")
        if _comp_row.get("epc_network_score", 0) >= 8:
            recommended_actions.append("EPC_RELATIONSHIP")
        if _comp_row.get("comp_ref_count", 0) >= 2:
            recommended_actions.append("EARLY_ENGAGEMENT")
    recommended_actions = [a for a in dict.fromkeys(recommended_actions) if a in ALLOWED_ACTION_TYPES]
    if not recommended_actions:
        recommended_actions = ["NO_ACTION_MONITORING"]

    # Evidence IDs (유사 사례 Top5)
    _ev_ids = ",".join(
        spark.sql(f"SELECT evidence_case_id FROM {SIMILAR_CASE_TABLE} WHERE new_rfq_id='{rfq_id}' ORDER BY similarity_score DESC LIMIT 5")
             .toPandas()["evidence_case_id"].fillna("N/A").tolist()
    ) if spark.catalog.tableExists(SIMILAR_CASE_TABLE) else ""

    _ml_rows = []
    for _, s in scenario_pdf.iterrows():
        rule = SCENARIO_RULES[s["scenario_type"]]
        _row = {
            "total_quoted_amt": float(s["recommended_quote_amt"] or 0),  # 총 견적가 (단가×수량)
            "overall_margin_pct": float(s["target_margin_pct"]),
            "discount_rate": 5.0 + rule.get("discount_delta_pct", 0),
            "offered_lead_time": int(s["offered_lead_time_wks"]),
            "has_automation_yn": "Y" if biz_unit in ("GSS","GPU") else "N",
            "high_risk_cnt": 0, "medium_risk_cnt": 0,
            "pjt_status": "Bidding", "target_region": rfq.get("target_region","Taiwan"),
            "est_investment_amt": float(rfq.get("budget_hint") or 6200000),
            "opportunity_score": 70, "construction_signal_yn": "N",
            "max_comp_win_rate": _max_comp_win, "avg_comp_price_score": _avg_price,
            "avg_comp_tech_score": float(likely_comp_pdf["tech_score"].mean()) if len(likely_comp_pdf)>0 else 5.0,
            "avg_comp_service_score": float(likely_comp_pdf["service_infra_score"].mean()) if len(likely_comp_pdf)>0 else 5.0,
            "avg_comp_epc_score": float(likely_comp_pdf["epc_network_score"].mean()) if len(likely_comp_pdf)>0 else 5.0,
            "active_competitor_count": _comp_cnt,
            "wonik_tech_score": float(_wk_row["tech_score"]) if _wk_row else 7.0,
            "wonik_service_score": float(_wk_row["service_infra_score"]) if _wk_row else 6.0,
            "wonik_epc_score": float(_wk_row["epc_network_score"]) if _wk_row else 6.0,
            "wonik_price_score": float(_wk_row["price_score"]) if _wk_row else 7.0,
            "wonik_vendor_status": _cm_row["wonik_vendor_status"] if _cm_row else "Qualified",
            "strategic_tier": _cm_row["strategic_tier"] if _cm_row else "Tier 2",
            "wonik_ref_cnt_bu": 0, "wonik_ref_cnt_cust": int(_ref_cust),
            "wonik_hist_win_rate_pct": float(_hwrr) if _hwrr else 50.0,
            "avg_bom_cost_index": float(_bom_val),
        }
        for f, v in rule.get("feature_overrides", {}).items():
            if f in _row:
                _row[f] = v if isinstance(v, str) else max(0.0, float(_row[f]) + float(v))
        _ml_rows.append(_row)

    _X = pd.DataFrame(_ml_rows, columns=_feat_cols)
    for _col, _le in _encoders.items():
        _X[_col] = _X[_col].apply(lambda v, le=_le: le.transform([str(v) if pd.notnull(v) else "__NA__"])[0] if str(v) in le.classes_ else int(len(le.classes_)/2))
    _X = _X.fillna(_ref_med)
    scenario_pdf["expected_win_probability"] = _ml_model.predict_proba(_X)[:, 1].tolist()
    scenario_pdf["pursue_recommendation"] = scenario_pdf["expected_win_probability"].apply(
        lambda x: "Pursue" if x >= 0.60 else ("Hold" if x >= 0.35 else "Pass"))

    # 저장 (타입 캐스팅 — pandas int64→Spark BIGINT vs 테이블 INT 불일치 방지)
    scenario_sdf = spark.createDataFrame(scenario_pdf)
    scenario_sdf = (
        scenario_sdf
        .withColumn("offered_lead_time_wks", F.col("offered_lead_time_wks").cast("int"))
        .withColumn("target_margin_pct", F.col("target_margin_pct").cast("double"))
        .withColumn("recommended_quote_amt", F.col("recommended_quote_amt").cast("double"))
        .withColumn("expected_win_probability", F.col("expected_win_probability").cast("double"))
        .withColumn("expected_profit_amt", F.col("expected_profit_amt").cast("double"))
        .withColumn("_update_at", F.current_timestamp())
    )
    scenario_sdf.write.mode("append").format("delta").saveAsTable(SCENARIO_TABLE)

    # 최종 추천 — 듀얼 모드 (Pass 상태면 승률최대 + Top5 내 최고마진)
    all_pass = (scenario_pdf["pursue_recommendation"] == "Pass").all()
    scenario_pdf["expected_value"] = scenario_pdf["expected_win_probability"] * scenario_pdf["expected_profit_amt"]

    if all_pass:
        best_winrate = scenario_pdf.sort_values("expected_win_probability", ascending=False).iloc[0]
        top5 = scenario_pdf.nlargest(5, "expected_win_probability")
        best_margin_top5 = top5.sort_values("target_margin_pct", ascending=False).iloc[0]
        best = best_winrate
        _alt_note = f"[Alt: {best_margin_top5['scenario_type']} | 승률={best_margin_top5['expected_win_probability']:.1%} | 마진={best_margin_top5['target_margin_pct']}%]"
    else:
        best = scenario_pdf.sort_values(["expected_value", "expected_win_probability"], ascending=False).iloc[0]
        _alt_note = ""

    rec = spark.createDataFrame([{
        "new_rfq_id": rfq_id, "recommended_scenario_id": best["scenario_id"],
        "recommended_quote_amt": float(best["recommended_quote_amt"]),
        "target_margin_pct": float(best["target_margin_pct"]),
        "min_acceptable_margin_pct": float(best["min_acceptable_margin_pct"]),
        "expected_win_probability": float(best["expected_win_probability"]),
        "expected_profit_amt": float(best["expected_profit_amt"]),
        "pursue_decision": best["pursue_recommendation"],
        "primary_action_type": recommended_actions[0],
        "recommended_action": " | ".join(recommended_actions),
        "similar_case_summary": f"VS results in {SIMILAR_CASE_TABLE}",
        "competitor_strategy_summary": " | ".join(recommended_actions),
        "cost_adjustment_summary": f"rebased in {COST_REBASE_TABLE}",
        "risk_note": f"automated pipeline run | {_alt_note}" if _alt_note else "automated pipeline run",
        "evidence_ids": _ev_ids,
    }]).withColumn("_update_at", F.current_timestamp())
    rec.write.mode("append").format("delta").saveAsTable(RECOMMEND_TABLE)

    if all_pass:
        print(f"  [{rfq_id}] ⚠️ 전체Pass — 추천1(승률최대): {best['scenario_type']} | 견적={best['recommended_quote_amt']:,.0f} | 마진={best['target_margin_pct']}% | 승률={best['expected_win_probability']:.1%}")
        print(f"           추천2(Top5마진): {best_margin_top5['scenario_type']} | 견적={best_margin_top5['recommended_quote_amt']:,.0f} | 마진={best_margin_top5['target_margin_pct']}% | 승률={best_margin_top5['expected_win_probability']:.1%}")
    else:
        print(f"  [{rfq_id}] 추천: {best['scenario_type']} | 견적={best['recommended_quote_amt']:,.0f} | 마진={best['target_margin_pct']}% | 승률={best['expected_win_probability']:.1%} | {best['pursue_recommendation']}")

print(f"\n✅ 파이프라인 완료: {len(new_rfq_ids)}건 RFQ 처리")