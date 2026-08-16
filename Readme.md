# Lab 13 - ETL vs ELT with PySpark

## 1. Overview

This lab compares two common data engineering approaches: **ETL (Extract, Transform, Load)** and **ELT (Extract, Load, Transform)**. Both pipelines use the same e-commerce order dataset and produce the same cleaned result, but the order of operations is different.

The main goal of the lab is to understand when data is transformed in each approach and how that decision affects flexibility, scalability, data preservation, and maintenance.

---

## 2. Learning Objectives

- Build an ETL pipeline using PySpark.
- Build an ELT pipeline using PySpark SQL.
- Clean and validate raw e-commerce data.
- Store data in Parquet format.
- Compare ETL and ELT outputs.
- Understand the advantages and limitations of each approach.
- Explain why ELT is commonly used in modern cloud and big-data platforms.

---

## 3. Environment and Tools

The lab was completed using the following environment:

- **Operating System:** Ubuntu 22.04 on WSL
- **Python:** 3.10.12
- **Java:** OpenJDK 11
- **PySpark:** 3.5.8
- **Editor:** Visual Studio Code
- **Storage Format:** Parquet

The project used a Python virtual environment to isolate the required dependencies.

---

## 4. Project Structure

```text
Lab13/
├── venv/
├── requirements.txt
├── etl_elt_lab.py
└── data/
    ├── etl_output/
    │   └── orders_clean/
    └── elt_output/
        └── orders_raw/
```

The `data` folder was populated when the PySpark script was executed.

---

## 5. Setup and Execution

### Create and activate the virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Install PySpark

```bash
python -m pip install pyspark==3.5.8
```

### Save the dependencies

```bash
python -m pip freeze > requirements.txt
```

### Run the lab

```bash
python etl_elt_lab.py
```

The script successfully started a Spark session, processed the dataset using both ETL and ELT, compared the results, and created the required Parquet output folders.

---

## 6. Dataset

The dataset contains 10 sample e-commerce orders. It intentionally includes several data-quality issues such as:

- Customer names with inconsistent capitalization.
- Product categories written using different letter cases.
- A missing order amount.
- A cancelled order.
- Dates stored as strings.

Examples include values such as:

- `bob` and `alice`
- `CLOTHING`, `Electronics`, and `electronics`
- A `NULL` amount for one order
- A cancelled order that should not be included in the final clean dataset

These problems were used to demonstrate the transformation stage.

---

## 7. ETL Pipeline

ETL means **Extract, Transform, Load**.

In this approach, the raw data is first extracted, then cleaned and transformed before it is written to the final storage location.

### Extract

The raw e-commerce dataset was loaded into a Spark DataFrame.

The script also checked the number of rows and the number of missing values.

### Transform

The ETL pipeline applied the following transformations:

- Customer names were standardized using `INITCAP`.
- Categories were converted to lowercase.
- Order dates were converted from strings to date values.
- Missing amounts were replaced with `0.0`.
- A new `order_month` column was created.
- Cancelled orders were removed.

For example:

```text
bob          -> Bob
CLOTHING     -> clothing
ELECTRONICS  -> electronics
NULL amount  -> 0.0
```

### Load

After the data was cleaned, it was written to Parquet:

```text
data/etl_output/orders_clean/
```

This means the ETL destination contains the already transformed version of the dataset.

---

## 8. ELT Pipeline

ELT means **Extract, Load, Transform**.

The main difference is that the raw data is stored first. Transformation happens after the data has already been loaded.

### Extract and Load

The raw dataset was written directly to:

```text
data/elt_output/orders_raw/
```

No cleaning was performed before this step.

### Register Raw Data

The raw Parquet data was loaded again and registered as a Spark SQL temporary view:

```text
orders_raw
```

### Transform with Spark SQL

Spark SQL was then used to perform transformations similar to the ETL pipeline:

- Standardize customer names.
- Convert categories to lowercase.
- Convert the order date to a date type.
- Replace missing amounts with `0.0`.
- Create the `order_month` column.
- Remove cancelled orders.

The ELT pipeline therefore reached the same cleaned result as ETL, but the raw data remained available.

### Category Summary Mart

The ELT section also created a second analytical result called the category summary.

It calculated:

- Total completed orders by category.
- Total revenue.
- Average order value.

This was created directly from the raw table without changing the ingestion process.

---

## 9. ETL vs ELT Comparison

| Feature | ETL | ELT |
|---|---|---|
| Transformation timing | Before loading | After loading |
| Raw data preserved | Usually not in target | Yes |
| Main transformation tool | Pipeline processing layer | Target compute engine |
| Data quality before storage | High | Raw data may contain issues |
| Flexibility | Lower | Higher |
| Reprocessing | May require extracting data again | Raw data can be reused |
| Modern cloud use | Common in controlled workflows | Common in data lakes and cloud analytics |

In this lab, both approaches produced matching cleaned outputs.

The comparison showed the same cleaned order records in both pipelines (screenshot):



The cancelled order was removed, and the missing amount was replaced with `0.0`.

---

## 10. Results

The lab completed successfully.

The final console output confirmed:

- The Spark session started.
- The raw dataset was displayed.
- The ETL pipeline completed successfully.
- Clean ETL data was written to Parquet.
- Raw ELT data was written to Parquet.
- Spark SQL transformations completed successfully.
- The category summary was generated.
- ETL and ELT produced matching cleaned results.
- Both output schemas were displayed.
- The script reached the final `Lab complete` message.

---

## 11. Discussion Questions

### Question 1: Architecture and Approach

The main architectural difference between ETL and ELT is **when the transformation occurs**.

With ETL, the data is transformed before it is loaded into the destination. This means the destination receives only cleaned and validated data.

With ELT, the raw data is loaded first and stored in the target platform. Transformations are performed later using the processing capabilities of the target system.

In this lab, the ETL pipeline cleaned the Spark DataFrame before writing it to `orders_clean`. The ELT pipeline first stored the original data in `orders_raw` and then used Spark SQL to clean it.

---

### Question 2: Data Preservation in ELT

Preserving raw data is useful because business requirements can change over time.

For example, a business analyst may later request a new calculation that was not included in the original pipeline. Because ELT keeps the original data, the engineering team can create a new transformation without extracting the data again from the original source.

Raw data is also useful if a transformation rule was incorrect. The data can be reprocessed using the corrected rules.

In this lab, `orders_raw` remained available even after the cleaned result was produced.

---

### Question 3: Flexibility and Secondary Analytics

The `category_summary` demonstrates the flexibility of ELT because a new analytical dataset was created from the existing raw data without changing the original loading process.

The Spark SQL query grouped completed orders by category and calculated total orders, total revenue, and average order value.

This shows that once raw data is stored, different teams can create multiple transformations or data marts for different business needs.

For example, another team could later create a monthly sales report or customer-level summary using the same raw table.

---

### Question 4: Scalability on Large Data

For a very large dataset such as 100 GB, ELT can be a strong choice when the target platform provides distributed processing such as Spark.

The data can be loaded into scalable storage first and then processed in parallel across multiple worker nodes. This allows the system to use distributed compute only when transformations are required.

ELT can also reduce the need to move large datasets through an external transformation server before loading them.

However, the best approach still depends on the system architecture. ETL may be more appropriate when only a small cleaned subset should be sent to the target system, especially when network bandwidth or storage cost is limited.

For a modern distributed Spark environment, ELT would generally provide more flexibility for large-scale analytics because the raw data can be processed and reprocessed in parallel.

---

### Question 5: Real-World Trade-offs

A real-world situation where I would prefer ETL is when sensitive or regulated data must be cleaned, validated, or removed before it enters the target system.

For example, a company may receive customer records containing personally identifiable information. If the analytics platform is not allowed to store certain sensitive fields, the ETL pipeline can remove or mask those fields before the data is loaded.

ETL is also useful when downstream applications require a strict schema and only validated records are allowed.

In these situations, ensuring data quality and compliance before loading is more important than keeping the complete raw dataset in the target platform.

---


## 12. Lessons Learned

This lab helped me understand that ETL and ELT can produce the same final result, but they use different data-processing architectures.

The most important lessons were:

- ETL transforms data before loading it.
- ELT preserves raw data and transforms it after loading.
- Preserving raw data makes it easier to support changing business requirements.
- Spark SQL can be used to build multiple analytical views from the same raw dataset.
- Distributed platforms such as Spark make ELT attractive for large datasets.
- ETL is still valuable when data must be validated, masked, or cleaned before it reaches the target system.
- Software compatibility, especially between Java and PySpark versions, is important when configuring Spark locally.

---

## Conclusion

Both ETL and ELT are useful data engineering patterns.

ETL is appropriate when the destination must receive clean and validated data immediately. ELT is more flexible because it stores raw data first and allows different transformations to be performed later.

In this lab, both pipelines produced the same final cleaned dataset, but the ELT approach demonstrated an additional advantage by preserving the raw data and using it to create a separate category summary.

The choice between ETL and ELT depends on factors such as data volume, compliance requirements, available compute resources, storage capacity, and how often business requirements change.
