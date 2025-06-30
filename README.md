**End to End Databricks Project**
The objective of this project is to build an end-to-end data engineering project with Databricks community edition following medallion architecture, which involves data ingestion from GitHub(https) and ADLSGen2 covering incremental loading using autoloader serving as bronze layer with unity catalog. Then silver layer contains the transformations using pyspark function, leveraging oops concept for data enrichment and functions in unity catalog for reusability. In gold layer, Integrating slowly changing dimension types 1and 2 with star schema and building delta live tables. Finally serving the data to warehouse for analytical reporting.

Architecture Diagram:
![ Diagram]( https://github.com/NisanthTumu/Databricks_EndToEnd_Project/blob/main/Architecture%20Diagram.jpg)

**🎯 Project Coverage:**
•	Data Understanding
•	Creating Azure Resources
•	Databricks Overview
•	Databricks Unity Catalog
•	Data Ingestion in Databricks
•	Databricks Autoloader with parquet files
•	Spark Structured Streaming
•	Databricks ETL Jobs
•	PySpark Functions
•	Python OOP with PySpark
•	PySpark Advanced Functions
•	Slowly Changing Dimension 
•	Databricks Delta Live Tables
•	Star Schema 
•	Databricks End-To-End Pipeline


✅ Key Goals: • Automate Data Ingestion: Seamlessly ingest data from sources into ADLS Gen2 storage using incremental loading with unity catalog
• Implement Medallion Architecture: Organize and transform data through bronze, silver, and gold layers to improve data quality and accessibility.
 • Enable Data Transformation & Enrichment: Leverage Azure Databricks for data processing with pyspark functions and oop concepts.
 • Optimize Analytics : Using dimensional data modelling with star schema and delta live tables for efficient querying, analytics, and reporting on large datasets. 
• Create Interactive Visualizations: Build dynamic dashboards with Power BI/Tableau/Fabric to provide meaningful business insights. 
• Ensure Production-Readiness: Apply best practices for performance optimization, error handling, monitoring, and maintenance.
