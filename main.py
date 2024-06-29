from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark session
spark = SparkSession.builder.appName("Hospital Resource Management").getOrCreate()

# Load datasets
patient_admissions = spark.read.csv("patient_admissions.csv", header=True, inferSchema=True)
resource_usage = spark.read.csv("resource_usage.csv", header=True, inferSchema=True)
staff_efficiency = spark.read.csv("staff_efficiency.csv", header=True, inferSchema=True)

# Data cleaning and transformations
patient_admissions = patient_admissions.na.drop()
resource_usage = resource_usage.na.drop()
staff_efficiency = staff_efficiency.na.drop()

# Task 2a: Total number of admissions per department
total_admissions = patient_admissions.groupBy("department").count().withColumnRenamed("count", "total_admissions")

# Task 2b: Average resource usage per department
average_resource_usage = resource_usage.groupBy("department").agg(avg("usage_hours").alias("average_resource_usage"))

# Task 2c: Average staff efficiency per department
average_staff_efficiency = staff_efficiency.groupBy("department").agg(avg("efficiency_rating").alias("average_staff_efficiency"))

# Task 3: Join the datasets
final_report = total_admissions.join(average_resource_usage, "department").join(average_staff_efficiency, "department")

# Task 4: Save the final report to results/final_report.csv
final_report_output_path = "results"
final_report.coalesce(1).write.csv(final_report_output_path, header=True)

# Stop the Spark session
spark.stop()
