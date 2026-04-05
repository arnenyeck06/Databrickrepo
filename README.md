# 🚀 End-to-End Data Lakehouse on Databricks

A production-ready data engineering project implementing the **Medallion Architecture** to transform raw bike sales data into analytics-ready insights using **Databricks** and **Delta Lake**.

## 📊 Project Overview

This project processes **Sales, CRM, and Product data** through a multi-layered pipeline designed for **scalability, reliability, and data quality**.

Built as part of a Data Engineering Bootcamp, it demonstrates **real-world data engineering practices** used in modern lakehouse architectures.

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

| Layer   | Purpose              | Transformations |
|--------|---------------------|---------------|
| Bronze | Raw data ingestion  | Minimal processing, schema-on-read, full historical data |
| Silver | Cleaned & enriched  | Deduplication, validation, joins, type casting, business rules |
| Gold   | Analytics-ready     | Aggregations, KPIs, dimensional models for BI |

## 🛠️ Tech Stack

- Platform: Databricks (Community Edition)  
- Languages: PySpark, SQL  
- Storage: Delta Lake (ACID transactions, time travel)  
- Orchestration: Databricks Workflows  
- Version Control: Git + GitHub  

## 📂 Repository Structure

bike_lakehouse/
├── Bronze/
├── Silver/
│   ├── crm/
│   └── erp/
├── Gold/
│   ├── dim_products/
│   ├── dim_customers/
│   └── fact_sales/
├── init_lakehouse.ipynb
└── datasets/

## ✨ Key Features

- ACID Transactions with Delta Lake  
- Schema Evolution & Enforcement  
- Automated Workflows  
- Data Quality Checks  
- Incremental Processing  
- Query Optimization  

## 🚀 Getting Started

### Prerequisites
- Databricks workspace  
- GitHub account  

### Setup

git clone https://github.com/arnenyeck06/Databrickrepo.git

Run:
init_lakehouse.ipynb

Then execute:
Bronze → Silver → Gold

## 👤 Author

Arne Nyeck

## 📄 License

MIT License
