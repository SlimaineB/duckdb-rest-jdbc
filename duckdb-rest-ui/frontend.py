import streamlit as st
import os
import requests
from tabs.query_tab import run_query_tab
from tabs.cluster_tab import run_cluster_tab
from tabs.parquet_tabs.parquet_file_row_group_size_tab import run_parquet_file_row_group_size_tab
from tabs.parquet_tabs.parquet_partition_tab import run_parquet_partition_tab
from tabs.parquet_tabs.parquet_bloom_filter_tab import run_parquet_bloom_filter_tab


st.set_page_config(page_title="DuckDB Client", layout="wide")

# Bandeau en haut
st.markdown("""
<div style="background-color:#0e1117;padding:1rem;border-radius:10px;margin-bottom:2rem;">
    <h2 style="color:white;margin:0;">🧠 GridDB Frontend</h2>
    <p style="color:#bbb;margin:0;">Enter an SQL query, execute it via the GridDB backend, and view the results.</p>
</div>
""", unsafe_allow_html=True)

# Sidebar - backend URL
default_base_url = os.getenv("BACKEND_URL", "http://localhost:8080")
backend_base_url = st.sidebar.text_input("🔗 Backend base URL (without /query):", value=default_base_url)
disable_ssl_verification = st.sidebar.checkbox("Disable SSL Verification", value=False)

# Sidebar - S3 config
st.sidebar.markdown("---")
st.sidebar.markdown("### 🪣 S3 Configuration")

# Test backend status
API_URL = backend_base_url.rstrip("/") 


try:
    resp = requests.get(STATUS_URL, verify=not disable_ssl_verification, timeout=2)
    if resp.ok:
        st.sidebar.success("✅ Backend is up")
    else:
        st.sidebar.warning("⚠️ Backend might be unreachable")
except Exception as e:
    st.sidebar.error(f"❌ Error reaching backend: {e}")

# Onglets
main_tabs = st.tabs([
    "🧪 Run Query",
    "📊 Cluster  Status",
    "⚙️ Parquet Checker",  
])



with main_tabs[0]:
    run_query_tab(API_URL, disable_ssl_verification)

with main_tabs[1]:
    run_cluster_tab(API_URL+ "/ui/status", disable_ssl_verification)

with main_tabs[2]:
    parquet_tabs = st.tabs(["⚙️ Parquet File/Row Group Size", "🧩 Parquet Partitioning", "🧬 Parquet Bloom Filter" ])
    
    with parquet_tabs[0]:
        run_parquet_file_row_group_size_tab(API_URL+"/ui/parquet_checker", disable_ssl_verification)

    with parquet_tabs[1]:
        run_parquet_partition_tab(API_URL+"/ui/parquet_checker",disable_ssl_verification)

    with parquet_tabs[2]:
        run_parquet_bloom_filter_tab(API_URL+"/ui/parquet_checker", disable_ssl_verification)


