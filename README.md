# 123_Electricity_Data
CS 12300 Spring Project - Big Data - University of Chicago - Detecting Anomalies and Trends in Electricity Consumption Data

## Folders
- ElasticSearch
    - Elasticsearch ingestion files (python)
- Exploration
    - Initial data exploration and graphs
- KNN
    - Code for KNN algorithm
    - PostgreSQL ingestion files (python and SQL)
- Tarzan
    - Code for Tarzan algorithm. Main files are Tarzan.py, which contains the primary code for running the algorithm.  tarzan-pipeline.py contains code for running a pipeline to take a csv and create a json file containing discretizations or discretizations and computed scores. The json files can be used to re-ingest into ElasticSearch.
    - The multithreading and multiprocessing files are test files for running tarzan.py concurrently.
    - Txt files containing logs of outputs for trials of basic, multithreading, and multiprocessing, and a folder containing old trials

## Reports and Presentation
- Presentation
- Report
