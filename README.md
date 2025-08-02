**Multinationa Retail Data Centralisation**

**Project Description**

A multinational company sells various goods across the globe. Its sales data is fragmented across different sources and formats, making analysis difficult and error-prone. This project focuses on building a robust ETL pipeline to ingest, transform and load diverse datasets into a centralized PostgreSQL database enabling efficient analytics and reporting.

**Technologies**

- Python
- PostgreSQL
- AWS S3
- Pandas, Numpy
- Git
- SQL

**Architecture & Data Flow**

The ETL process follows this pipeline:

1. **Data Extraction**
   -From PostgreSQL files and AWS S3 (csv files)
2. **Data Transformation**
   -Cleaned and standardized using python 
   -Handled inconsisten formats and missing values
3. **Data Loading**
   -Loaded clean datasets into a centralized PostgreSQL database for analysis

<img width="785" height="428" alt="image" src="https://github.com/user-attachments/assets/6149628b-6284-4e60-b699-d3ebe2356067" />

**Data Sources**

Raw Data Sources
  - PostgreSQL dump (.tar): https://cdn.theaicore.com/content/projects/MRDC_Data/mrdc_raw_data_db.tar
  - S3 Bucket CSV: s3://data-handling-public/products.csv
-Shcema
Includes tables like: Orders, users, card details, store details etc.

**Setup & Storage**

To run this project locally, follow these steps:

1. Clone the repository
git clone https://github.com/DulceF/mrdc-etl-pipeline.git
cd mrdc-etl-pipeline

2. Install required packages
   pip install -r requirements.txt

3. 

**Metrics and Results**
-Performance
-Data quality
-Insights

**Challenges & Lessons Learned**

-Challenge
-Solution
-Lesson

**License**
This project is licensed under MIT License. 
Feel free to use and modify for educational and professional purposes.

**Contact**
-Linkedin
-Github
-Email



