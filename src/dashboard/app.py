import os
import sys
import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================
# PROJECT CONFIG
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

try:
    from src.config.db_config import (
        DB_USER,
        DB_PASS,
        DB_HOST,
        DB_PORT,
        DB_NAME,
        DB_TABLE,
        FRAUD_ALERT_TABLE,
    )
except Exception:
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "adminpassword")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5433")
    DB_NAME = os.getenv("DB_NAME", "fraud_db")
    DB_TABLE = os.getenv("DB_TABLE", "transactions")
    FRAUD_ALERT_TABLE = os.getenv("FRAUD_ALERT_TABLE", "fraud_alerts")


# ============================================================
# CSS
# ============================================================

st.markdown(
    """
<style>
    .stApp {
        background: #f5f7fb;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-left: 3rem;
        padding-right: 3rem;
        padding-bottom: 2rem;
        max-width: 1550px;
        margin: 0 auto;
    }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    .header-title {
        font-size: 32px;
        font-weight: 900;
        color: #101828;
        margin-bottom: 2px;
    }

    .header-subtitle {
        font-size: 15px;
        color: #667085;
        margin-bottom: 18px;
    }

    .date-pill {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 12px 16px;
        text-align: center;
        color: #344054;
        font-weight: 700;
        box-shadow: 0 8px 24px rgba(16, 24, 40, 0.05);
        margin-top: 8px;
    }

    .kpi-card {
        border-radius: 18px;
        padding: 22px;
        color: white;
        box-shadow: 0 14px 30px rgba(16, 24, 40, 0.10);
        min-height: 122px;
    }

    .kpi-title {
        font-size: 13px;
        opacity: 0.92;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }

    .kpi-value {
        font-size: 30px;
        font-weight: 900;
        margin-top: 8px;
    }

    .kpi-foot {
        font-size: 12px;
        opacity: 0.92;
        margin-top: 7px;
    }

    .card-purple {
        background: linear-gradient(135deg, #8b5cf6 0%, #a78bfa 100%);
    }

    .card-blue {
        background: linear-gradient(135deg, #3b82f6 0%, #38bdf8 100%);
    }

    .card-red {
        background: linear-gradient(135deg, #fb4d61 0%, #ff7a7a 100%);
    }

    .card-orange {
        background: linear-gradient(135deg, #fb923c 0%, #f59e0b 100%);
    }

    .panel-title {
        font-size: 18px;
        font-weight: 900;
        color: #101828;
        margin-top: 14px;
        margin-bottom: 10px;
    }

    div[data-testid="stPlotlyChart"] {
        background: white;
        border-radius: 18px;
        padding: 8px;
        box-shadow: 0 10px 28px rgba(16, 24, 40, 0.06);
    }

    div[data-testid="stDataFrame"] {
        background: white;
        border-radius: 18px;
        padding: 8px;
        box-shadow: 0 10px 28px rgba(16, 24, 40, 0.06);
    }

    .topic-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 15px;
        min-height: 92px;
        box-shadow: 0 10px 24px rgba(16, 24, 40, 0.04);
    }

    .topic-icon {
        background: #7c3aed;
        color: white;
        border-radius: 10px;
        padding: 6px 10px;
        font-weight: 900;
        display: inline-block;
        margin-bottom: 8px;
    }

    .topic-name {
        color: #101828;
        font-size: 12px;
        font-weight: 800;
        word-break: break-all;
    }

    .active-dot {
        color: #12b76a;
        font-size: 13px;
        font-weight: 900;
        margin-top: 6px;
    }
</style>
""",
    unsafe_allow_html=True,
)


# ============================================================
# DATABASE
# ============================================================

@st.cache_resource
def get_db_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url, pool_pre_ping=True)


def load_kpi_metrics():
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            tx_sql = text(
                f"""
                SELECT
                    COUNT(*) AS total_transactions,
                    COUNT(*) FILTER (WHERE status = 'APPROVED') AS approved_count,
                    COUNT(*) FILTER (WHERE status = 'REJECTED') AS rejected_count,
                    COALESCE(AVG(risk_score), 0) AS avg_risk
                FROM "{DB_TABLE}"
                """
            )

            alert_sql = text(
                f"""
                SELECT COUNT(*) AS fraud_alert_count
                FROM "{FRAUD_ALERT_TABLE}"
                """
            )

            tx_result = conn.execute(tx_sql).mappings().first()
            alert_result = conn.execute(alert_sql).mappings().first()

        total_transactions = int(tx_result["total_transactions"] or 0)
        approved_count = int(tx_result["approved_count"] or 0)
        rejected_count = int(tx_result["rejected_count"] or 0)
        fraud_alert_count = int(alert_result["fraud_alert_count"] or 0)
        avg_risk = float(tx_result["avg_risk"] or 0)

    except Exception:
        total_transactions = 0
        approved_count = 0
        rejected_count = 0
        fraud_alert_count = 0
        avg_risk = 0.0

    fraud_rate = (
        rejected_count / total_transactions * 100
        if total_transactions > 0
        else 0
    )

    return {
        "total_transactions": total_transactions,
        "approved_count": approved_count,
        "rejected_count": rejected_count,
        "fraud_alert_count": fraud_alert_count,
        "fraud_rate": fraud_rate,
        "avg_risk": avg_risk,
    }


def load_risk_distribution():
    default_bins = ["0-20", "20-40", "40-60", "60-80", "80-90", "90-100"]
    default_df = pd.DataFrame(
        {
            "Risk Level": default_bins,
            "Count": [0, 0, 0, 0, 0, 0],
        }
    )

    try:
        engine = get_db_engine()

        query = text(
            f"""
            WITH risk_bins AS (
                SELECT
                    CASE
                        WHEN risk_score < 20 THEN '0-20'
                        WHEN risk_score < 40 THEN '20-40'
                        WHEN risk_score < 60 THEN '40-60'
                        WHEN risk_score < 80 THEN '60-80'
                        WHEN risk_score < 90 THEN '80-90'
                        ELSE '90-100'
                    END AS risk_level,
                    CASE
                        WHEN risk_score < 20 THEN 1
                        WHEN risk_score < 40 THEN 2
                        WHEN risk_score < 60 THEN 3
                        WHEN risk_score < 80 THEN 4
                        WHEN risk_score < 90 THEN 5
                        ELSE 6
                    END AS risk_order
                FROM "{DB_TABLE}"
                WHERE risk_score IS NOT NULL
            )
            SELECT
                risk_level AS "Risk Level",
                COUNT(*) AS "Count",
                MIN(risk_order) AS risk_order
            FROM risk_bins
            GROUP BY risk_level
            ORDER BY risk_order
            """
        )

        db_df = pd.read_sql(query, engine)

        if db_df.empty:
            return default_df

        merged = default_df.merge(
            db_df[["Risk Level", "Count"]],
            on="Risk Level",
            how="left",
            suffixes=("", "_db"),
        )

        merged["Count"] = merged["Count_db"].fillna(merged["Count"]).astype(int)
        return merged[["Risk Level", "Count"]]

    except Exception:
        return default_df


def read_table(table_name: str, limit: int = 800):
    try:
        engine = get_db_engine()
        query = f'SELECT * FROM "{table_name}" ORDER BY detected_at DESC LIMIT {limit}'
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def normalize_transactions(df: pd.DataFrame):
    if df.empty:
        return df

    df = df.copy()

    df["detected_at"] = pd.to_datetime(df["detected_at"], errors="coerce")
    df["detected_at"] = df["detected_at"].fillna(pd.Timestamp.now())
    df["time_str"] = df["detected_at"].dt.strftime("%H:%M:%S")

    if "risk_score" not in df.columns:
        df["risk_score"] = df["fraud_probability"] * 100

    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0)

    if "status" not in df.columns:
        df["status"] = df["fraud_probability"].apply(
            lambda x: "REJECTED" if x >= 0.70 else "APPROVED"
        )

    df["status"] = df["status"].astype(str).str.upper()

    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

    if "fraud_probability" in df.columns:
        df["fraud_probability"] = pd.to_numeric(
            df["fraud_probability"],
            errors="coerce",
        ).fillna(0)

    return df


def build_count_agg(df: pd.DataFrame):
    """
    Chỉ dùng cho Biểu đồ Tổng Số Giao Dịch.
    Bucket 30s + trục Y tối thiểu 1000.
    """
    chart_df = df.copy()

    chart_df["detected_at"] = pd.to_datetime(
        chart_df["detected_at"],
        errors="coerce",
    )

    chart_df = chart_df.dropna(subset=["detected_at"])
    chart_df = chart_df.sort_values("detected_at").tail(5000).reset_index(drop=True)

    if chart_df["detected_at"].nunique() < 10:
        start_time = pd.Timestamp.now() - pd.Timedelta(
            milliseconds=len(chart_df) * 25
        )
        chart_df["plot_time"] = [
            start_time + pd.Timedelta(milliseconds=i * 25)
            for i in range(len(chart_df))
        ]
    else:
        chart_df["plot_time"] = chart_df["detected_at"]

    chart_df["time_bucket"] = chart_df["plot_time"].dt.floor("30s")

    agg_df = (
        chart_df.groupby("time_bucket")
        .agg(transaction_count=("detected_at", "count"))
        .reset_index()
        .sort_values("time_bucket")
    )

    if len(agg_df) > 3:
        agg_df = agg_df.iloc[:-1]

    return agg_df.tail(30)


def build_amount_agg(df: pd.DataFrame):
    """
    Chỉ dùng cho Biểu đồ Khối Lượng Giao Dịch.
    Giữ bucket 10s như cũ, không dùng scale của count chart.
    """
    chart_df = df.copy()

    chart_df["detected_at"] = pd.to_datetime(
        chart_df["detected_at"],
        errors="coerce",
    )

    chart_df = chart_df.dropna(subset=["detected_at"])
    chart_df = chart_df.sort_values("detected_at").tail(1200).reset_index(drop=True)

    chart_df["Amount"] = pd.to_numeric(
        chart_df["Amount"],
        errors="coerce",
    ).fillna(0)

    if chart_df["detected_at"].nunique() < 10:
        start_time = pd.Timestamp.now() - pd.Timedelta(seconds=len(chart_df))
        chart_df["plot_time"] = [
            start_time + pd.Timedelta(seconds=i)
            for i in range(len(chart_df))
        ]
    else:
        chart_df["plot_time"] = chart_df["detected_at"]

    chart_df["time_bucket"] = chart_df["plot_time"].dt.floor("10s")

    agg_df = (
        chart_df.groupby("time_bucket")
        .agg(transaction_volume=("Amount", "sum"))
        .reset_index()
        .sort_values("time_bucket")
    )

    return agg_df.tail(30)


transactions = normalize_transactions(read_table(DB_TABLE, 5000))
fraud_alerts = normalize_transactions(read_table(FRAUD_ALERT_TABLE, 500))


# ============================================================
# HEADER
# ============================================================

header_left, header_right = st.columns([7, 3], gap="large")

with header_left:
    st.markdown(
        '<div class="header-title">Fraud Detection Dashboard</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="header-subtitle">'
        "Apache Kafka + Creditcard-Fraud · Mobile Banking Realtime Monitor"
        "</div>",
        unsafe_allow_html=True,
    )

with header_right:
    today = datetime.now().strftime("%b %d, %Y")
    auto_refresh = st.checkbox("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh", 2, 10, 3)
    st.markdown(
        f'<div class="date-pill">📅 {today} &nbsp; 🟢 Live</div>',
        unsafe_allow_html=True,
    )


# ============================================================
# EMPTY STATE
# ============================================================

if transactions.empty:
    st.warning(
        "⏳ Chưa có dữ liệu giao dịch. Hãy chạy fraud_detector.py trước, "
        "sau đó chạy transaction_producer.py."
    )

else:
    # ============================================================
    # KPI CARDS
    # ============================================================

    metrics = load_kpi_metrics()

    total_transactions = metrics["total_transactions"]
    approved_count = metrics["approved_count"]
    rejected_count = metrics["rejected_count"]
    fraud_alert_count = metrics["fraud_alert_count"]
    fraud_rate = metrics["fraud_rate"]

    def kpi_card(title, value, foot, css_class):
        st.markdown(
            f"""
            <div class="kpi-card {css_class}">
                <div class="kpi-title">{title}</div>
                <div class="kpi-value">{value}</div>
                <div class="kpi-foot">{foot}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4 = st.columns(4, gap="large")

    with c1:
        kpi_card(
            "Total Transactions",
            f"{total_transactions:,}",
            "Processed events",
            "card-purple",
        )

    with c2:
        kpi_card(
            "Approved",
            f"{approved_count:,}",
            "Low-risk transactions",
            "card-blue",
        )

    with c3:
        kpi_card(
            "Rejected",
            f"{rejected_count:,}",
            "Blocked events",
            "card-red",
        )

    with c4:
        kpi_card(
            "Fraud Alerts",
            f"{fraud_alert_count:,}",
            f"Fraud rate {fraud_rate:.2f}%",
            "card-orange",
        )

    # ============================================================
    # REAL-TIME AGGREGATED CHARTS
    # ============================================================

    count_agg_df = build_count_agg(transactions)
    amount_agg_df = build_amount_agg(transactions)

    col_tx_count, col_tx_amount = st.columns([1, 1], gap="large")

    with col_tx_count:
        st.markdown(
            '<div class="panel-title">Biểu đồ Tổng Số Giao Dịch (Real-time)</div>',
            unsafe_allow_html=True,
        )

        fig_count = go.Figure()

        fig_count.add_trace(
            go.Scatter(
                x=count_agg_df["time_bucket"],
                y=count_agg_df["transaction_count"],
                mode="lines+markers",
                name="Số giao dịch",
                line=dict(width=3, color="#3b82f6", shape="spline"),
                marker=dict(size=6),
                fill="tozeroy",
                fillcolor="rgba(59, 130, 246, 0.22)",
                hovertemplate=(
                    "Thời gian: %{x}<br>"
                    "Số giao dịch: %{y:,}<extra></extra>"
                ),
            )
        )

        max_count = (
            int(count_agg_df["transaction_count"].max())
            if not count_agg_df.empty
            else 0
        )
        y_max_count = max(1000, int(max_count * 1.2))

        fig_count.update_layout(
            height=330,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="white",
            plot_bgcolor="white",
            showlegend=False,
            xaxis_title="Thời gian",
            yaxis_title="Số lượng",
        )

        fig_count.update_xaxes(
            showgrid=True,
            gridcolor="#eef2f7",
            automargin=True,
            nticks=8,
        )

        fig_count.update_yaxes(
            showgrid=True,
            gridcolor="#eef2f7",
            automargin=True,
            range=[0, y_max_count],
        )

        st.plotly_chart(fig_count, use_container_width=True)

    with col_tx_amount:
        st.markdown(
            '<div class="panel-title">Biểu đồ Khối Lượng Giao Dịch ($ Real-time)</div>',
            unsafe_allow_html=True,
        )

        fig_amount = go.Figure()

        fig_amount.add_trace(
            go.Scatter(
                x=amount_agg_df["time_bucket"],
                y=amount_agg_df["transaction_volume"],
                mode="lines+markers",
                name="Khối lượng giao dịch",
                line=dict(width=3, color="#22c55e", shape="spline"),
                marker=dict(size=6),
                fill="tozeroy",
                fillcolor="rgba(34, 197, 94, 0.22)",
                hovertemplate=(
                    "Thời gian: %{x}<br>"
                    "Khối lượng: $%{y:,.2f}<extra></extra>"
                ),
            )
        )

        fig_amount.update_layout(
            height=330,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="white",
            plot_bgcolor="white",
            showlegend=False,
            xaxis_title="Thời gian",
            yaxis_title="Khối lượng ($)",
        )

        fig_amount.update_xaxes(
            showgrid=True,
            gridcolor="#eef2f7",
            automargin=True,
            nticks=8,
        )

        fig_amount.update_yaxes(
            showgrid=True,
            gridcolor="#eef2f7",
            automargin=True,
            rangemode="tozero",
        )

        st.plotly_chart(fig_amount, use_container_width=True)

    # ============================================================
    # STATUS RATIO + RISK DISTRIBUTION
    # ============================================================

    col_status_donut, col_risk_bar = st.columns([4, 6], gap="large")

    with col_status_donut:
        st.markdown(
            '<div class="panel-title">Transaction Status Ratio</div>',
            unsafe_allow_html=True,
        )

        fig_donut = go.Figure(
            data=[
                go.Pie(
                    labels=["Approved", "Rejected"],
                    values=[approved_count, rejected_count],
                    hole=0.72,
                    marker=dict(colors=["#7c3aed", "#ef4444"]),
                    textinfo="percent",
                )
            ]
        )

        approved_rate = (
            approved_count / total_transactions * 100
            if total_transactions
            else 0
        )

        fig_donut.add_annotation(
            text=(
                f"{approved_rate:.1f}%"
                "<br><span style='font-size:13px'>Approved</span>"
            ),
            x=0.5,
            y=0.5,
            font_size=24,
            showarrow=False,
        )

        fig_donut.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="white",
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=0.92,
            ),
        )

        st.plotly_chart(fig_donut, use_container_width=True)

    with col_risk_bar:
        st.markdown(
            '<div class="panel-title">Risk Score Distribution - All Transactions</div>',
            unsafe_allow_html=True,
        )

        risk_count = load_risk_distribution()

        fig_risk = go.Figure()

        fig_risk.add_trace(
            go.Bar(
                x=risk_count["Risk Level"],
                y=risk_count["Count"],
                text=risk_count["Count"],
                textposition="outside",
                marker=dict(
                    color=[
                        "#22c55e",
                        "#84cc16",
                        "#facc15",
                        "#f97316",
                        "#fb7185",
                        "#ef4444",
                    ],
                    line=dict(width=0),
                ),
                hovertemplate=(
                    "Risk level: %{x}<br>"
                    "Transactions: %{y:,}<extra></extra>"
                ),
            )
        )

        fig_risk.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="white",
            plot_bgcolor="white",
            showlegend=False,
            xaxis_title="Risk Score Level",
            yaxis_title="Number of Transactions",
        )

        fig_risk.update_xaxes(
            showgrid=False,
            automargin=True,
        )

        fig_risk.update_yaxes(
            showgrid=True,
            gridcolor="#eef2f7",
            automargin=True,
            rangemode="tozero",
        )

        st.plotly_chart(fig_risk, use_container_width=True)

    # ============================================================
    # TABLES
    # ============================================================

    col_table, col_alert = st.columns([6, 5], gap="large")

    with col_table:
        st.markdown(
            '<div class="panel-title">Recent Transactions</div>',
            unsafe_allow_html=True,
        )

        recent = transactions.head(12).copy()

        display_cols = [
            "transaction_id",
            "Amount",
            "transaction_type",
            "fraud_probability",
            "risk_score",
            "status",
            "time_str",
        ]

        recent = recent[[c for c in display_cols if c in recent.columns]]

        recent.rename(
            columns={
                "transaction_id": "Transaction ID",
                "Amount": "Amount",
                "transaction_type": "Type",
                "fraud_probability": "Fraud Probability",
                "risk_score": "Risk Score",
                "status": "Status",
                "time_str": "Time",
            },
            inplace=True,
        )

        if "Amount" in recent.columns:
            recent["Amount"] = recent["Amount"].apply(lambda x: f"${x:,.2f}")

        if "Fraud Probability" in recent.columns:
            recent["Fraud Probability"] = recent["Fraud Probability"].apply(
                lambda x: f"{x * 100:.2f}%"
            )

        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
            height=390,
        )

    with col_alert:
        st.markdown(
            '<div class="panel-title">Fraud Alerts</div>',
            unsafe_allow_html=True,
        )

        if fraud_alerts.empty:
            st.info("Hiện chưa có fraud alert.")
        else:
            alerts = fraud_alerts.head(12).copy()

            alert_cols = [
                "transaction_id",
                "Amount",
                "fraud_probability",
                "risk_score",
                "alert_level",
                "time_str",
            ]

            alerts = alerts[[c for c in alert_cols if c in alerts.columns]]

            alerts.rename(
                columns={
                    "transaction_id": "Transaction ID",
                    "Amount": "Amount",
                    "fraud_probability": "Fraud Probability",
                    "risk_score": "Risk Score",
                    "alert_level": "Severity",
                    "time_str": "Time",
                },
                inplace=True,
            )

            if "Amount" in alerts.columns:
                alerts["Amount"] = alerts["Amount"].apply(lambda x: f"${x:,.2f}")

            if "Fraud Probability" in alerts.columns:
                alerts["Fraud Probability"] = alerts["Fraud Probability"].apply(
                    lambda x: f"{x * 100:.2f}%"
                )

            st.dataframe(
                alerts,
                use_container_width=True,
                hide_index=True,
                height=390,
            )

    # ============================================================
    # KAFKA PIPELINE STATUS
    # ============================================================

    st.markdown(
        '<div class="panel-title">Kafka Pipeline Status</div>',
        unsafe_allow_html=True,
    )

    topics = [
        "banking.transactions.raw",
        "Spark Fraud Detector",
        "PostgreSQL transactions",
        "PostgreSQL fraud_alerts",
    ]

    topic_cols = st.columns(4, gap="large")

    for idx, topic in enumerate(topics):
        with topic_cols[idx]:
            st.markdown(
                f"""
                <div class="topic-card">
                    <div class="topic-icon">⌘</div>
                    <div class="topic-name">{topic}</div>
                    <div class="active-dot">● Active</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


# ============================================================
# AUTO REFRESH
# ============================================================

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()




