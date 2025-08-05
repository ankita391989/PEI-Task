Answers corresponding to the notes provided.
1. Every task has a respective code file and Unit Test file to it.
2. This solution has used a test-driven development (TDD) approach as per following: 
    > - For each data processing step, corresponding pytest unit tests are defined immediately after the implementation.
    > - These tests validate data loading, transformation, joins, aggregations, schema, data integrity, and error handling.
    > - The tests are run to ensure correctness at every stage of the pipeline.
    > - Example: After loading and transforming data, tests like test_orders_json_load, test_products_csv_load, test_customers_excel_load, etc. are defined.
    > - After Spark transformations and joins, tests like test_enriched_orders_inner_join, test_enriched_orders_schema, test_customer_metrics_columns, etc. are defined.
    > - Edge cases and error handling are also covered with tests that expect exceptions or validate behavior with empty/malformed data.

3. This solution addresses data quality issues and implements robust error-handling mechanisms as follows:

 a. Data Quality Handling:
    > - Nulls are dropped from key DataFrames using dropna().
    > - Data types are validated and enforced (e.g., date parsing, numeric checks, string checks).
    > - Special characters and digits are removed from customer names.
    > - Columns are renamed to remove spaces for compatibility.
    > - The 'phone' column with bad data is dropped from customers_df.
    > - Data integrity is checked via unit tests (e.g., no nulls in key columns, correct types, unique keys).

 b. Error Handling:
    > - Unit tests use pytest to check for expected errors (e.g., loading files with wrong structure, missing headers, empty files).
    > - Joins are tested to ensure missing keys do not appear in results.
    > - Aggregations and joins on non-existent columns are tested to raise AnalysisException.
    > - Edge cases (empty DataFrames, null keys) are explicitly tested.
    > - Rounding and schema integrity are validated.
    
4. The solution has documented the code and assumptions clearly to make it understandable for others.
  
5. This is how the performance implications and optimization of the code has been done for efficiency and scalability.

  a. Use of Spark DataFrames:
    > - All heavy data processing, joins, and aggregations are performed using Spark DataFrames, which are distributed and optimized for large-scale data.

  b. Column Pruning and Projection:
    > - Only necessary columns are selected during joins (e.g., select('Customer_ID', 'Customer_Name', 'Country')), reducing data shuffling and memory usage.

  c. Data Cleaning Before Joins:
    > - Nulls are dropped from key DataFrames (dropna), ensuring efficient joins and reducing unnecessary data movement.

  d. Efficient Aggregations:
    > - GroupBy and aggregation operations are performed using Spark's built-in functions, which are optimized for distributed computation.

  e. Avoidance of UDFs:
    > - All transformations use Spark SQL functions instead of Python UDFs, leveraging Spark's Catalyst optimizer for better performance.

  f. Table Caching and Reuse:
    > - Intermediate results are saved as tables (saveAsTable), allowing for reuse and avoiding recomputation.

  g. Minimal Data Movement:
    > - Joins are performed on indexed keys (Customer_ID, Product_ID), minimizing shuffles.

  h. Rounding and Data Type Handling:
    > - Numeric columns are rounded after aggregation, reducing data size and improving performance for downstream analytics.

  i. Error Handling and Edge Cases:
    > - Edge cases are handled in unit tests, ensuring robustness and preventing expensive failures during large-scale runs.

  k. No Collect or toPandas on Large Data:
    > - Data is not collected to the driver or converted to pandas except for small DataFrames in tests, preventing driver memory overload.

 In summary, the solution is optimized for efficiency and scalability by leveraging Spark's distributed processing, minimizing data movement, and using efficient transformations and aggregations.
