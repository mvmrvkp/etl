### Spark ETL
- Spark ETL read data from multiple sources, process and write to multiple sources

#### Input

- Input support different types
     ```
    HDFS
    HIVE
    Any other input sources with respected development
    ```

- Output support different types
     ```
    HDFS
    HIVE
    Any other input sources with respected development
    ```

#### Run Spark ETL
- Checkout from git
- Update the data paths in config w.r.t checkout dir
- run the following command
    ```
    sbt clean compile test
    ```