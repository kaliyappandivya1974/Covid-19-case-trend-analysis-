🦠 COVID-19 Case Trend Analysis Engine
A Big Data Analytics Application designed to process large-scale epidemiological datasets. This project utilizes Apache Spark for high-performance distributed data processing and Flask to serve real-time insights via an interactive web dashboard.

🚀 Features
Big Data Processing: Ingests and processes massive CSV datasets using Apache Spark (PySpark).

Performance Optimization: Implements In-Memory Caching to reduce query latency from seconds to milliseconds.

Interactive Dashboard: A responsive frontend built with Bootstrap 5 and Chart.js for dynamic trend visualization.

Real-Time Aggregation: Calculates total cases (Confirmed, Deaths, Recovered) by country and date on-the-fly.

System Monitoring: Direct integration with the Spark UI to monitor cluster health and job execution stages.

🏗️ Tech Stack
Backend
Language: Python 3.11 (Required for PySpark compatibility on Windows)

Engine: Apache Spark (PySpark SQL & DataFrames)

Server: Flask (REST API)

Frontend
Structure: HTML5, Jinja2 Templates

Styling: Bootstrap 5 (Responsive Design)

Visualization: Chart.js (Line Charts)

Data
Format: CSV (Comma Separated Values)

Source: Aggregated time-series data (e.g., JHU CSSE, WHO).
