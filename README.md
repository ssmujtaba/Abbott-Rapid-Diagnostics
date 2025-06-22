# ⚕️ Abbott Diagnostics Sales & Marketing Dashboard

This repository contains a full-scale data analytics project focused on the healthcare diagnostics industry. The project simulates a real-world business intelligence scenario, starting with the creation of a messy, realistic dataset, followed by a complete **ETL (Extract, Transform, Load)** process, and culminating in an interactive, multi-page dashboard built with **Microsoft Power BI**.

---

## 🎯 Project Goal

The objective of this project was to analyze three years of sales data (2021-2023) for Abbott's rapid diagnostics division. The goal was to build a powerful analytical tool to help business leaders understand sales trends, identify top-performing products and customers, evaluate sales team performance, and uncover actionable insights to drive strategic decisions.

---

## 🛠️ Technical Skills and Process

- **ETL Pipeline (Python & SQL)**: A Python script was developed using `pandas` and `numpy` to generate, clean, and transform 15,000 rows of synthetic data, including realistic seasonality for different product lines (e.g., COVID-19 vs. Influenza). The script then connected to a MySQL database to create a normalized **Star Schema** and load the cleaned data.

- **DAX (Data Analysis Expressions)**: A robust data model was built in Power BI, enhanced with key DAX measures to analyze performance. This included:
  - **Net Sales calculations** to properly account for product returns.
  - **Time intelligence measures** like Year-over-Year (YoY) Sales Growth using `SAMEPERIODLASTYEAR`.
  - **Advanced KPIs** such as `Average Sale Value` to measure transaction efficiency.

- **Data Visualization & Storytelling**: A three-page report was designed with a logical narrative flow, moving from a high-level summary to granular deep-dives. The design prioritizes interactivity and clear communication of key findings.

---

## 📖 The Dashboard: A Page-by-Page Story

The report consists of three pages, each providing a unique layer of analysis.

### 1. 📈 Executive Sales Summary

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/a572ca07-ec1b-431f-9403-d0b3a9416251" />

**Purpose**: To provide a high-level, "at-a-glance" summary of the business's overall health for senior leadership.

**Key Insights & Visuals**:
- **Core KPIs**: The dashboard immediately presents the most critical top-line numbers for the entire three-year period: **$79.15M in Net Sales** from over **3 Million units sold**.
- **Growth & Efficiency**: A key dynamic KPI shows a strong **31.72% Year-over-Year (YoY) Sales Growth**, indicating significant expansion. The **Average Sale Value of $5.28K** provides a baseline for customer purchasing power.
- **Sales Trend Analysis**: The `Net Sales and Quantity Sold Over Time` chart clearly illustrates the market trend. Sales peaked in **2021 ($38M)**, likely driven by the high demand for COVID-19 tests, and then normalized in **2022 ($24M)** and **2023 ($19M)** as the pandemic evolved.
- **Product Line Dominance**: The donut chart reveals that the **COVID-19** product line is the primary revenue driver, accounting for **60.4%** of all net sales.

### 2. 📊 Product & Customer Analysis

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/3a550156-5571-4331-990a-30e37c6c3a2f" />

**Purpose**: To perform a deep-dive analysis to answer the questions, "WHAT are our top-selling products?" and "WHO are our most valuable customers?"

**Key Insights & Visuals**:
- **Top Performing Products**: The bar chart pinpoints the "hero" products. The **"Id Now Covid-19"** test is the single most valuable product, generating **$26.7M** in net sales, followed by the **"Id Now Influenza A&B 2"** test at **$20.1M**.
- **Customer Type Contribution**: The `Sales Contribution by Customer Type` treemap shows a very well-diversified customer base. Sales are almost evenly split between **Distributors, Clinics, Governments, and Hospitals**, with each segment contributing ~$19-20M. This indicates a low-risk, balanced sales strategy.
- **Top Customer Leaderboard**: The table identifies the most valuable individual accounts. **Prime Diagnostics Inc.** is the top customer with **$10.14M** in net sales across 1,877 transactions.

### 3. 👥 Sales Team Performance

<img width="1280" alt="1" src="https://github.com/user-attachments/assets/5bd92c73-808b-4055-9b73-749ccf3128c0" />

**Purpose**: To analyze the performance of individual sales representatives and regional teams to identify top performers and understand regional strengths.

**Key Insights & Visuals**:
- **Sales Rep Leaderboard**: The table provides a clear ranking of sales performance. **David Chen ($20.08M)** and **Emily White ($20.07M)** are identified as the elite top performers, significantly outpacing the rest of the team. This highlights a key opportunity for knowledge sharing from top reps to the rest of the team.
- **Regional Performance**: The leaderboard shows that the **West (David Chen)** and **Midwest (Emily White)** are the top-performing regions, driven by their star sales reps. The Northeast and South regions have multiple reps but lower overall sales per rep, suggesting a different market dynamic or a need for regional strategy review.
- **Performance by Product Line (Matrix View)**: A matrix visual (not pictured) breaks down each representative's sales by product line. This would allow a sales manager to see, for example, that David Chen's success is driven by high-volume sales of COVID-19 tests, while another rep might be a specialist in the Influenza product line. This insight is crucial for targeted coaching and account assignment.

---

## ✨ Conclusion

This Power BI dashboard successfully transforms raw transactional data into a powerful analytical tool. It provides a clear, 360-degree view of business performance and delivers actionable insights that can help stakeholders optimize marketing strategies, identify top-performing products and sales personnel, and drive profitable growth in a competitive diagnostics market.
