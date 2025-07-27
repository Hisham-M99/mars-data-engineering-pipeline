# Mars Data Engineering Pipeline

## Project Pipeline Overview

Here’s a visual summary of the ETL workflow:

![Project Workflow](pipeline_diagram.png)

## Project Description

This end-to-end data engineering project demonstrates the full lifecycle of handling real-world data from NASA’s open APIs, transforming and cleaning it, and loading it into a PostgreSQL database. The entire workflow is orchestrated using Apache Airflow and designed to simulate a production-grade pipeline.

The data focuses on **Mars exploration**:
- *NASA Mars Rover Photos*
- *NASA Insight Weather Data*

---

## Technologies Used

| Tool            | Purpose                                 |
|------------------|-------------------------------------------|
| **Python 3.11**       | Data extraction, transformation, format conversion |
| **PostgreSQL**        | Relational data storage                |
| **Talend Open Studio** | ETL jobs for transformation and loading processed data into PostgreSQL |
| **Apache Airflow 2.9**| Workflow orchestration & monitoring    |
| **JSON, Parquet, CSV**| Data serialization & format conversion |
| **NASA APIs**         | Source of public Mars data             |

---

## Workflow Overview

### Step 1: Data Extraction with Python
- Fetch Mars Rover photos metadata from [NASA Open APIs](https://api.nasa.gov/)
- Extract weather records from the [NASA InSight: Mars Weather Service API](https://api.nasa.gov/assets/insight/InSight%20Weather%20API%20Documentation.pdf)
- Store as raw `.json`

### Step 2: Data Cleaning & Transformation
- Remove duplicates and nulls
- Normalize nested fields
- Convert JSON → Parquet → CSV

### Step 3: Data Loading with Talend
- Two ETL jobs designed in **Talend Open Studio 7.3.1**
    - `Job_Load_Rover_Data`
    - `Job_Load_Weather_Data`
- Load processed CSVs into PostgreSQL tables
- Tables created via a SQL DDL script

### Step 4: Orchestration with Apache Airflow
- One DAG (`mars_pipeline_dag.py`) controls the entire pipeline
- Scheduled for daily execution
- Tracks status, retries on failure, logs everything

---

## Folder Structure

```
mars-data-engineering-pipeline/
├── dags/                       # Airflow DAG scripts
├── scripts/                    # Python scripts for extraction & transformation
├── talend_executables/        # Exported Talend jobs as .sh/.bat scripts
├── raw_data/                  # Original API JSON files
├── processed_data/            # Parquet and CSV outputs
├── db/                        # SQL scripts (table creation)
├── .gitignore
├── README.md
```

---

## How to Run

1. **Set up Airflow environment** and PostgreSQL
2. Place exported Talend `.sh` jobs inside `talend_executables/`
3. Modify paths or connections in DAG and scripts if needed
4. Trigger the DAG via Airflow UI or CLI:
---

## Outcome

- Successfully loaded and transformed **Mars Rover photo metadata** and **weather data**
- Cleaned and formatted for SQL queries and dashboards
- Simulates production pipelines with real monitoring and reprocessing logic

---

## Future Improvements

- Add email alerting to Airflow DAG
- Dockerize the full pipeline
- Deploy on cloud

---
