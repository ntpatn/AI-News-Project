# AI-News-Project

A fully modular **News ETL + ML Classification Pipeline** designed for scalability, automation, and production‑grade processing.  
This project demonstrates end‑to‑end capability across **Data Engineering, Machine Learning Engineering, and MLOps**, featuring Airflow orchestration, Dockerized services, modular registries, and automated ML pipelines.

---

## ✨ Key Features
- **Automated ETL Pipeline** (Bronze → Silver → Gold)
- **Airflow-based Orchestration** with modular TaskGroups
- **Robust Data Loading** (database extractors, Snowflake‑style ID generator, schema validation)
- **Modular Machine Learning Pipeline**
  - Feature extractors (SBERT)
  - Reducers / Selectors
  - Model registry (LogReg, XGB, SVM, etc.)
- **Dockerized full environment** (Airflow, Postgres, MinIO)
- **Config‑driven architecture** for reproducibility
- **Production-ready patterns** (Factory, Registry, Strategy)

---

## 📦 Tech Stack
**Core:** Python, SQL, Docker, Airflow  
**ML:** Scikit‑Learn, SentenceTransformers, XGBoost  
**Storage:** Postgres, MinIO  
**Orchestration:** Airflow 2.10  
**Others:** SQLAlchemy, Pandas, MLflow (optional)

---

## 📁 Project Structure
```
AI-News-Project/
│
├── dags/
├── src/
│   ├── etl/
│   ├── function/
│   ├── registry/
│   └── interface/
│
├── docker-compose.yaml
├── requirements_airflow.txt
└── README.md
```

---

## 🚀 How to Run
### 1. Clone
```
git clone https://github.com/<your-user>/AI-News-Project.git
cd AI-News-Project
```

### 2. Build + Start Services
```
docker compose up -d --build
```

Airflow UI → http://localhost:8090  
MinIO → http://localhost:9001  

---

## 🧠 ML Pipeline Overview
```
Feature Extractor → Reducer → Selector → Model
```

Examples:
- `sbert` → `pca` → `kbest` → `logreg`
- `sbert` → `linearsvc`
- `tfidf` → `xgb`

Add new models via registry with just 3 lines.

---

## 🔥 Inference Interface
`src/interface/predict.py`  
- Loads SBERT once  
- Loads trained model  
- Returns news category from raw text

Ready for CLI, scripts, or future API deployment.

---

## 📊 Sample Use Case
1. Scrape raw news → Bronze  
2. Clean & normalize → Silver  
3. Deduplicate & enrich → Gold  
4. Train classification model  
5. Run inference to categorize new incoming news

---

<!-- ## 📌 Flow Diagram
*(Placeholder — will be added once final flow is confirmed.)*

--- -->

## 🌟 Why This Project Matters
- Real enterprise‑grade architecture  
- Scalability for **100+ models**  
- Airflow + Docker orchestration  
- Clean modular design for long‑term maintainability  
- Strong portfolio project for ML/DE/MLOps roles

---

## 👤 Author
**Nithispat Jitrdetakjorn**  
AI/ML Engineer • Data Engineer • Software Developer

---
