# Retail Analytics DLT Pipeline
**End-to-end Databricks Lakehouse** - Bronze→Silver→Gold processing sales records.

## 🎯 Business Value Delivered

💰 Revenue trends by price tier (Budget→Luxury)
🏪 Store productivity KPIs (revenue/txns/customer)
📦 Top 10 products per store (assortment analysis)
👤 Customer Lifetime Value + recency scoring

## 🏗️ Architecture
![Pipeline Graph]
<img width="1725" height="812" alt="Screenshot 2026-01-04 144826" src="https://github.com/user-attachments/assets/0a7e285b-70d9-42d9-a1bd-b05b7c8fe5bc" />

**Bronze (4 raw tables) → Silver (4 streaming dims) → Gold (5 materialized views)**

## 📋 Tables Overview
Bronze: Raw landing zone (Auto Loader)
Silver: Cleaned+typed streaming tables (dropDuplicates)
Gold: Analytics-ready MVs (window functions, expectations)

## 🛠️ Tech Stack
Databricks Lakehouse | DLT Pipeline | PySpark Streaming
Delta Lake | Window Functions | Data Quality Expectations
Auto Loader | Batch Materialized Views

## 🚀 Key Engineering Patterns
✅ Streaming deduplication (dropDuplicates)
✅ Silver→Gold materialized views
✅ Multi-table dimensional modeling
✅ Business logic validation (expectations)
✅ Production deployment ready

