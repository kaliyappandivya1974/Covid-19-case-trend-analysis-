# spark_processor.py
import sys
import os
import socketserver

# --- PATCH: Fix for PySpark on Windows with Python 3.13 ---
# Python 3.13 on Windows doesn't have UnixStreamServer, but PySpark tries to use it.
if sys.platform == 'win32' and not hasattr(socketserver, 'UnixStreamServer'):
    # Define a dummy class or alias TCPServer to allow import to proceed
    socketserver.UnixStreamServer = socketserver.TCPServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

class CovidTrendProcessor:
    def __init__(self, filepath):
        self.filepath = filepath
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("CovidTrendAnalysis") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()
            
        self.df = None
        print(f"\n✅ SPARK STATUS: Running at {self.spark.sparkContext.uiWebUrl}\n")
        
        # --- FIX 1: Run a 'Warmup' Job ---
        # This ensures at least one completed job appears in the Spark UI immediately on startup
        self.spark.sparkContext.setJobDescription("Startup Warmup Checks")
        self.spark.range(1).collect()
        self.spark.sparkContext.setJobDescription(None)

    def get_spark_ui_url(self):
        # Retrieve the original URL (can be http://hostname:4040 or http://10.x.x.x:4040)
        web_url = self.spark.sparkContext.uiWebUrl
        
        # Robustly force 127.0.0.1 to ensure it works on local Windows machines
        # This handles cases where Spark binds to a specific hostname/IP that might not be clickable
        try:
            port = web_url.split(':')[-1]
            return f"http://127.0.0.1:{port}"
        except:
            return web_url.replace("host.docker.internal", "127.0.0.1")

    def load_data(self):
        if not os.path.exists(self.filepath):
            print(f"❌ Error: File not found at {self.filepath}")
            return
        
        # Set description for the loading job
        self.spark.sparkContext.setJobDescription(f"Loading Data: {os.path.basename(self.filepath)}")
            
        self.df = self.spark.read.csv(self.filepath, header=True, inferSchema=True)

        # --- FIX: Handle the specific column names from your CSV ---
        # Your CSV has 'date' (lowercase), so we rename it to 'Date' for consistency
        if 'date' in self.df.columns:
            self.df = self.df.withColumnRenamed("date", "Date")
        
        # Ensure Date is actually a DateType
        self.df = self.df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
            
        self.df.cache()
        
        # --- FIX 2: Force Materialization for Storage Tab ---
        # cache() is lazy. Running count() forces Spark to cache it NOW, making it show up in the 'Storage' tab.
        row_count = self.df.count()
        print(f">> Data Loaded, Cached, and Counted ({row_count} rows)")
        
        self.spark.sparkContext.setJobDescription(None)

    def get_trends(self, country, metric):
        if self.df is None:
            self.load_data()
            
        # --- FIX: Use 'country' (lowercase) instead of 'Country/Region' ---
        if country == "Global":
            filtered_df = self.df
        else:
            # Matches the 'country' column in your CSV 
            filtered_df = self.df.filter(col("country") == country)

        # Aggregation Logic
        trend_df = filtered_df.groupBy("Date") \
            .agg(_sum(metric).alias("Total")) \
            .orderBy("Date")

        # --- FIX 3: Name the Job in UI ---
        self.spark.sparkContext.setJobDescription(f"Trend Analysis: {metric} in {country}")
        results = trend_df.collect()
        self.spark.sparkContext.setJobDescription(None)
        
        # Format output
        labels = [row["Date"].strftime("%Y-%m-%d") for row in results if row["Date"]]
        data = [row["Total"] for row in results if row["Total"] is not None]

        return {"labels": labels, "data": data}