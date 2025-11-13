from pyspark.sql import SparkSession
from pyspark.sql import Row

def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("Students HDFS Parquet + SQL Example")
        .master("spark://pyspark-master:7077")  # Connects to Spark master container
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")  # HDFS NameNode
        .getOrCreate()
    )
    print("SparkSession created successfully.")
    return spark

def write_parquet_to_hdfs(spark):
    # Automatically generate student data (10 rows)
    students = [
        Row(RollNo=1, Name="Ayesha", Age=20, Department="CS", CGPA=3.7),
        Row(RollNo=2, Name="Bilal", Age=22, Department="IT", CGPA=3.4),
        Row(RollNo=3, Name="Sara", Age=21, Department="SE", CGPA=3.9),
        Row(RollNo=4, Name="Ali", Age=23, Department="AI", CGPA=3.2),
        Row(RollNo=5, Name="Noor", Age=20, Department="DS", CGPA=3.8),
        Row(RollNo=6, Name="Hassan", Age=22, Department="CS", CGPA=3.6),
        Row(RollNo=7, Name="Laiba", Age=21, Department="IT", CGPA=3.5),
        Row(RollNo=8, Name="Usman", Age=23, Department="AI", CGPA=3.1),
        Row(RollNo=9, Name="Zara", Age=20, Department="SE", CGPA=3.9),
        Row(RollNo=10, Name="Imran", Age=22, Department="DS", CGPA=3.4),
    ]

    df = spark.createDataFrame(students)
    print("Student DataFrame created:")
    df.show()

    output_path = "hdfs://namenode:9000/user/data/students_parquet"
    print(f"Writing Parquet data to HDFS at: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print("Parquet file written to HDFS.")

def read_parquet_and_query(spark):
    input_path = "hdfs://namenode:9000/user/data/students_parquet"
    print(f"Reading Parquet data from HDFS at: {input_path}")
    df = spark.read.parquet(input_path)

    print("Data read successfully. Registering temporary SQL table...")
    df.createOrReplaceTempView("students")

    print("All student data:")
    spark.sql("SELECT * FROM students").show()

    print("Students in CS department:")
    spark.sql("SELECT Name, Age, CGPA FROM students WHERE Department = 'CS'").show()

    print(" Average CGPA by Department:")
    spark.sql("SELECT Department, AVG(CGPA) as Avg_CGPA FROM students GROUP BY Department").show()

if __name__ == "__main__":
    spark = get_spark_session()
    write_parquet_to_hdfs(spark)
    read_parquet_and_query(spark)
    spark.stop()
    print("Spark session stopped.")
