import streamlit as st

st.set_page_config(
    page_title="Scientific Recommendation System - USTHB",
    page_icon="magnifying_glass",
    layout="wide"
)

st.title("Scientific Article Recommendation System")
st.subheader("PFE Master Bioinformatics - USTHB 2025/2026")
st.info("Interface under development - Phase 5")

st.markdown("""
### Infrastructure Status

| Service | Status |
|---------|--------|
| Kafka | Running |
| Spark | Running |
| Elasticsearch | Running |
| PostgreSQL | Running |
| MongoDB | Running |
| Redis | Running |
| API FastAPI | Running |
""")
