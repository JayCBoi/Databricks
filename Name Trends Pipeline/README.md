# Baby Names Analysis Pipeline

## Overview
An end-to-end data pipeline built on Databricks that processes New York baby name data, performs trend analysis, and generates business insights.

## Architecture
**Medallion Architecture Implementation:**
- **Bronze Layer:** Raw data ingestion from public API
- **Silver Layer:** Data validation, cleaning, and quality checks  
- **Gold Layer:** Trend analysis and aggregation
- **Report Layer:** Business insights and visualizations

## Pipeline Steps
1. **Ingestion:** Download from NY health data API
2. **Cleaning:** Data quality checks, null handling, duplicate removal, proper column naming
<img width="649" height="144" alt="obraz" src="https://github.com/user-attachments/assets/7019a73f-9584-4f4c-b374-cff6851d7a10" />

3. **Analysis:** Trend identification, popularity rankings
<img width="904" height="476" alt="obraz" src="https://github.com/user-attachments/assets/9f2ebc1e-f52c-4cd0-8da8-4080dfe5a675" />
<img width="906" height="472" alt="obraz" src="https://github.com/user-attachments/assets/7aad81a5-8450-4d17-8aad-0a2780cc1951" />

4. **Reporting:** Visualization generation and business insights
<img width="463" height="162" alt="obraz" src="https://github.com/user-attachments/assets/b5c62270-3f30-45c7-bac6-854bda7afd11" />


## 📸 Pipeline
Set up as 4 tasks that can be run on a schedule, modular and parameterized.
<img width="809" height="565" alt="obraz" src="https://github.com/user-attachments/assets/a397e0fe-d216-4e38-a5da-99269525f0ca" />
<img width="444" height="268" alt="obraz" src="https://github.com/user-attachments/assets/f7a9a2fc-1736-44de-8ef8-1e719521518f" />

## Key Features
- Parameterized notebooks
- Automated data quality checks
- Multi-task dependency management
- Unity Catalog integration
