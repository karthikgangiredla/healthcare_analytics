A comprehensive, interactive dashboard built using Streamlit, BigQuery, and Plotly, powered by Synthea-generated synthetic healthcare data. This tool provides visual insights into patient demographics, provider performance, healthcare trends, and more.

 Features:
Sidebar navigation for:
Provider Dashboard
Patient Dashboard
Organization Dashboard
Advanced Analytics
Insights (Trends & Demographics)
Table Explorer for BigQuery previews
Interactive charts using Plotly
Age & gender-based filtering
Monthly encounter trend analysis
Dynamic SQL queries with BigQuery

Technologies Used:
Streamlit
Google BigQuery
Plotly Express
Synthea
Python (pandas, os)

Sample KPIs:
Avg conditions per age group
Encounter trends over time
Procedure volume vs encounter duration
Gender comparison of conditions

PROJECT STRUCTURE
project-root/
│
├── .venv/                  # Virtual environment directory
│   ├── etc/
│   ├── Include/
│   ├── Lib/
│   ├── Scripts/
│   ├── share/
│   ├── .gitignore
│   └── pyvenv.cfg
│
├── dashboard/              # Dashboard logic (views, schemas, KPIs)
│   ├── aggregations.py
│   ├── dashboards.py
│   ├── datamarts.py
│   ├── kpi_views.py
│   └── starschema.py
│
├── data/                   #  raw and processed data files
│
├── etl/                    # ETL pipeline code
│   ├── __pycache__/
│   ├── dbconfig.cpyt...
│   ├── extract.py
│   ├── load.py
│   └── transform.py
│
├── .env                    # Environment variables file
├── .gitignore              # Git ignore rules
├── app.py                  # Main application entry point -- streamlit
├── dbconfig.py             # DB config (duplicate/clean-up needed with cpyt...)
├── README.md               # Project documentation
└── requirements.txt        # Python dependencies
