import streamlit as st
import os
import requests
from tabs.query_tab import run_query_tab
from tabs.cluster_tab import run_cluster_tab
from tabs.tuning_tab import run_tuning_tab
from tabs.partition_tab import run_partition_tab
from tabs.bloom_filter_tab import run_bloom_filter_tab
from tabs.query_optimizer_tab import run_query_optimizer_tab

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
STATUS_URL = backend_base_url.rstrip("/") + "/ui/status"


try:
    resp = requests.get(STATUS_URL, verify=not disable_ssl_verification, timeout=2)
    if resp.ok:
        st.sidebar.success("✅ Backend is up")
    else:
        st.sidebar.warning("⚠️ Backend might be unreachable")
except Exception as e:
    st.sidebar.error(f"❌ Error reaching backend: {e}")

# Onglets
tabs = st.tabs([
    "🧪 Run Query",
    "📊 Cluster",
    "⚙️ Tuning",
    "🧩 Partitioning",
    "🧬 Bloom Filter",    
    "🧠 SQL Query Optimizer"         
])



with tabs[0]:
    run_query_tab(API_URL, disable_ssl_verification)

with tabs[1]:
    run_cluster_tab(STATUS_URL, disable_ssl_verification)

with tabs[2]:
    run_tuning_tab(API_URL, disable_ssl_verification)

with tabs[3]:
    run_partition_tab(API_URL,disable_ssl_verification)

with tabs[4]:
    run_bloom_filter_tab(API_URL, disable_ssl_verification)

with tabs[5]:
    run_query_optimizer_tab(API_URL, disable_ssl_verification)
