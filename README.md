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

Copyright (c) 2025 Arne Nyeck

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including, without limitation, the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
