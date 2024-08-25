# Real-Time-Market-Data-Analysis
This project delivers real-time global market insights by streaming and transforming data using AWS Kinesis, Snowflake, and Power BI. We leverage AWS services for efficient data management, with Snowflake as the data warehouse and Power BI for visualization.


## Architecture

![image](https://github.com/user-attachments/assets/b632a7d0-a6d4-4ecf-8b9c-60484caefc03)





The architecture of this project leverages a series of AWS services to build an efficient data pipeline:

1. **Data Ingestion**:
   - **Amazon EC2**: An API on an EC2 instance is used to extract real-time market data in CSV format. The data is then sent to Amazon Kinesis Firehose.
  
2. **Data Streaming**:
   - **Amazon Kinesis Firehose**: Acts as the data stream that reliably captures and loads data into Amazon S3. Additionally, AWS Lambda is integrated with Kinesis for data transformation.
  
3. **Data Transformation**:
   - **AWS Lambda**: Facilitates real-time data transformation before the data is stored in the target location.
  
4. **Data Storage**:
   - **Amazon S3**: Temporary storage for raw and transformed data. The data is then ingested into Snowflake using **Snowpipe**.
   - **Snowflake**: Serves as the data warehouse, where transformed data is stored and made available for querying and analysis.
  
5. **Data Loading**:
   - **Snowpipe**: Automates the data loading process from Amazon S3 to Snowflake, ensuring the data is always up-to-date.
  
6. **Data Visualization**:
   - **Power BI**: The final transformed and stored data in Snowflake is visualized through Power BI, offering powerful insights and analytics to stakeholders.

### Key Components

- **Amazon EC2**: Handles the data extraction from market data APIs.
- **AWS Lambda**: Executes the real-time transformation of incoming data.
- **Amazon Kinesis Firehose**: Streams the data efficiently into the data lake and for further processing.
- **Amazon S3**: Provides scalable storage for data before it is loaded into Snowflake.
- **Snowflake**: A powerful data warehouse where data is stored, queried, and analyzed.
- **Power BI**: Used for creating dynamic and interactive dashboards based on the processed data.

### Goals

- **Real-Time Data Processing**: Ensure that market data is processed in real-time with minimal latency.
- **Scalable Architecture**: Utilize AWS services to build a scalable and efficient data pipeline.
- **Insightful Visualization**: Provide stakeholders with actionable insights through Power BI dashboards.

### Future Enhancements

- **Automated Monitoring**: Implement automated monitoring for the data pipeline to ensure smooth operations and quick error resolution.
- **Advanced Analytics**: Integrate machine learning models for predictive analysis on market data.
