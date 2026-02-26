
import sys

from pyspark.sql import SparkSession
 
def main():

    if len(sys.argv) != 3:

        print("Usage: wordcount_health <input> <output>")

        sys.exit(-1)
 
    spark = SparkSession.builder.appName("HealthcareAnalysis").getOrCreate()

    input_path = sys.argv[1]

    output_path = sys.argv[2]

    try:

        # We use header=True; if the CSV has no header, these columns become _c0, _c1, etc.

        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df.show(5) # Debug: Print first 5 rows to logs

        # CHANGED: Using 'tweets' based on Spark's suggestion from your logs

        target_column = "tweets" 

        if target_column in df.columns:

            counts = df.groupBy(target_column).count()

            counts.write.mode("overwrite").csv(output_path)

            print(f"Job completed successfully grouping by {target_column}!")

        else:

            print(f"Error: Column {target_column} not found. Available: {df.columns}")

            sys.exit(1)

    except Exception as e:

        print(f"Error: {e}")

        sys.exit(1)

    finally:

        spark.stop()
 
if __name__ == "__main__":

    main()

