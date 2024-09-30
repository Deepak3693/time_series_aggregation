# Time Series Aggregation with Pyspark

## Overview

This project implements a batch job that aggregates time series data based on metrics and time buckets. It calculates the average value of each metric within specified time intervals.

## Folder Structure

```bash
time_series_aggregation/
│
├── data/
│   ├── input/
│       ├── input.csv       # Example input CSV file containing metrics, values, and timestamps
│   ├── output/             # output folder to save the results
│
├── src/
│   ├── main.py  # The main PySpark script for aggregating the data
│
├── README.md                  # This README file
│
└── requirements.txt           # Python dependencies (if needed)
```

## Files Description

- **data/input/input.csv**: This is the input CSV file that contains the time series data with the following columns: `Metric`, `Value`, and `Timestamp`.

- **data/output/**: This directory will contain the output CSV file generated after running the aggregation job.

- **src/main.py**: This is the main script that performs the aggregation. It reads the input data, processes it, and writes the output to a CSV file.

- **requirements.txt**: This file can list any Python dependencies required to run the script (e.g., PySpark).

## Prerequisites

1. **Python**: Ensure you have Python installed (preferably Python 3).

2. **PySpark**: You can install PySpark via pip if it’s not already installed:

   ```bash
   pip install -r requirements.txt
   ```

## Running the job

1. **Navigate to the Project Directory:**

    ```bash
   cd path/to/time_series_aggregation
   ```

2. **Prepare Input Data:** Place your input CSV file in the data/input/ directory, ensuring it follows the format:

    ```bash
    Metric,Value,Timestamp
    temperature,88,2022-06-04T12:01:00.000Z
    temperature,89,2022-06-04T12:01:30.000Z
    precipitation,0.5,2022-06-04T14:23:32.000Z
    ```
3. **Arguments of glue job:**

    ```bash
    --input_path : Path to input CSV file
    --output_path : Directory to save the output CSV
    --bucket_duration : Duration for the time buckets (default: "24 hours")
    ```bash

4. **Run the PySpark Job:** Use the ```spark-submit command``` to run the aggregation job. Specify the input and output file paths:

    Sample command:

    ```bash
    spark-submit src/main.py --input_path data/input/input.csv --output_path data/output/ --bucket_duration "24 hours"
    ```
    You can modify the ```bucket_duration``` in the script to adjust the time window for averaging (default is 24 hours).

5. **Check Output:** After running the job, the output CSV file will be generated in the data/output/ directory with the averaged results.