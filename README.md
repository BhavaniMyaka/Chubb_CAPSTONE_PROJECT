🌍 CO₂ Emissions Monitoring & Environmental Impact Analytics System

An end-to-end data engineering and analytics capstone project that tracks global CO₂ emissions, analyzes environmental impact, and provides decision-ready insights through automated ETL pipelines, big-data processing, and interactive dashboards.

📌 Project Objective

To build a real-world analytics system that:

Collects and processes CO₂ emissions data

Transforms raw data into meaningful environmental insights

Automates workflows using Airflow + Databricks

Visualizes trends, risks, and policy impacts using Power BI

This system is designed for policy makers, environmental analysts, and sustainability teams to support data-driven climate decisions.

🏗️ High-Level Architecture
Raw Dataset (CSV)
        ↓
Bronze Layer – Raw ingestion (Databricks)
        ↓
Silver Layer – Cleaning & standardization
        ↓
Gold Layer – Analytics & KPIs
        ↓
Power BI Dashboards
        ↓
Airflow – Orchestrates the entire pipeline

🧰 Technology Stack
Data Processing

Python

PySpark

Pandas (for validation & exploration)

Big Data & Analytics

Databricks (Delta Lake, Serverless Jobs)

Apache Spark

Workflow Automation

Apache Airflow (Dockerized setup)

Visualization

Power BI

Version Control

Git & GitHub

📂 Project Folder Structure
Capstone_Project/
│
├── Airflow/
│   ├── airflow-dags/
│   │   └── co2_databricks_etl_dag.py
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── requirements.txt
│
├── Dataset/
│   └── co2_emissions.csv
│
├── Notebooks-ETL/
│   ├── CO2_BRONZE_INGESTION.ipynb
│   ├── CO2_SILVER_TRANSFORMATION.ipynb
│   └── CO2_GOLD_ANALYTICS.ipynb
│
├── Implementation-Pictures/
│   ├── Airflow-Screenshots/
│   ├── DataBricksUI/
│   └── PowerBi-Screenshots/
│
├── CO2_EMISSIONS_VISUALIZATION.pbix
├── CO-Emissions-Monitoring-and-Environmental-Impact-Analytics-System.pptx
└── README.md

📊 Dataset Overview

The dataset contains country-wise and year-wise CO₂ emission records along with supporting indicators:

Column	Description
country	Country name
region	Continent / region
year	Reporting year
total_co2_emissions	Total emissions (million metric tons)
avg_population	Average population
avg_gdp_billion	GDP in billion USD
sector	Emission sector (Energy, Industry, Transport, etc.)
total_sector_emissions	Sector-wise emissions
scenario	Climate scenario (Baseline, Policy Reduction, Renewable Transition, High Growth)
avg_co2_per_capita	Per-capita emissions
impact_level	Environmental impact level
🔁 ETL Pipeline Design
1️⃣ Bronze Layer – Raw Ingestion

Notebook: CO2_BRONZE_INGESTION.ipynb

Loads CSV into Databricks

Stores raw data in Delta format

No transformations — preserves original data

2️⃣ Silver Layer – Cleaning & Transformation

Notebook: CO2_SILVER_TRANSFORMATION.ipynb

Handles missing values

Removes duplicates

Standardizes:

Country names

Year formats

Creates derived columns:

Emissions per capita

Emission growth indicators

3️⃣ Gold Layer – Analytics & KPIs

Notebook: CO2_GOLD_ANALYTICS.ipynb

Creates 8 analytical tables, including:

yearly_emissions_trend

country_emissions_summary

regional_emissions_summary

high_emission_regions

population_emission_correlation

scenario_environment_impact

sector_emissions_analysis

policy_impact_summary

These tables power dashboards and decision metrics.

⚙️ Workflow Automation – Apache Airflow
What is automated?

Triggers Databricks job

Executes Bronze → Silver → Gold pipeline

Monitors execution status

Handles retries on failure

DAG: co2_databricks_etl_dag.py

Key features:

Retry mechanism

Failure alerts in UI

Execution logs

Job status tracking

📈 Monitoring & Logging
Airflow Monitoring

Task logs stored in airflow-logs/

Each DAG run shows:

Start time

End time

Status (Success / Retry / Failed)

Databricks Monitoring

Job Run Logs

Spark UI

Delta table history

This ensures end-to-end observability of the pipeline.

📊 Power BI Dashboards

File: CO2_EMISSIONS_VISUALIZATION.pbix

Dashboard Pages
1️⃣ Executive CO₂ Overview

Total emissions KPIs

High-emission regions

Sector contribution

2️⃣ Trends & Growth Analysis

Year-over-year emission trends

Population vs emissions correlation

GDP impact analysis

3️⃣ Regional & Sector Analysis

World map visualization

Sector-wise emissions over time

Country-level drilldowns

4️⃣ Scenario & Policy Impact

Emissions by climate scenario

Per-capita impact

AI visuals: Key Influencers, Decomposition Tree

🤖 Advanced Analytics Features

Scenario Modeling
Compare emissions under:

Baseline

Policy Reduction

Renewable Transition

High Growth

Correlation Analysis
Emissions vs population growth

Policy Impact Estimation
Visual proof that sustainable policies reduce emissions

📸 Implementation Evidence

All execution proof is stored in:

Implementation-Pictures/


Includes:

Airflow DAG runs

Retry logs

Databricks notebook executions

Delta table creation

Power BI dashboards

Relationship models

This makes the project audit-ready and evaluator-friendly.

🎯 Evaluation Alignment
Evaluation Area	How this project satisfies
Data Processing & ETL (30%)	Multi-layer ETL using Databricks
Environmental Analytics (25%)	Scenario modeling, impact analysis
Workflow Automation (20%)	Airflow DAG orchestration
Visualization (15%)	Advanced Power BI dashboards
Documentation (10%)	Detailed README + screenshots
🚀 How to Run This Project
1️⃣ Clone Repository
git clone https://github.com/BhavaniMyaka/Chubb_CAPSTONE_PROJECT.git

2️⃣ Run Airflow
cd Airflow
docker compose up


Access UI:
👉 http://localhost:8080

Trigger DAG:
co2_emissions_databricks_etl

3️⃣ Databricks

Upload notebooks

Create job

Connect with Airflow

4️⃣ Power BI

Open CO2_EMISSIONS_VISUALIZATION.pbix

Refresh data

Explore dashboards

🏁 Final Outcome

This project demonstrates:

Real-world data engineering architecture

Enterprise-grade ETL automation

Scalable big-data analytics

Business-ready environmental insights

Professional documentation & presentation
