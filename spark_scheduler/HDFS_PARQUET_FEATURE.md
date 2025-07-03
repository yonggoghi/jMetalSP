# HDFS/Parquet Auto-Detection Feature

## Overview
The Campaign Scheduling Optimizer now automatically detects Hadoop/HDFS availability and saves optimization results in the most appropriate format:
- **Parquet files on HDFS** when Hadoop is available (ideal for big data processing)
- **CSV files locally** when Hadoop is not available (fallback for development/testing)

## Features

### 🔍 **Automatic Environment Detection**
- Detects Hadoop environment variables (`HADOOP_HOME`, `HADOOP_CONF_DIR`, `YARN_CONF_DIR`)
- Tests HDFS connectivity and configuration (`fs.defaultFS`)
- Graceful fallback to local CSV when HDFS is not available

### 📊 **Rich Data Formats**

#### **Schedule Records (Parquet/CSV)**
Each customer assignment contains:
```scala
case class ScheduleRecord(
  customerId: Long,
  timeSlot: Int,
  channel: Int,
  channelName: String,           // "Email", "SMS", "Push", "In-app"
  expectedResponseRate: Double,
  cost: Double,
  priority: Double,
  solutionMetrics: SolutionMetrics
)
```

#### **Solution Summary (Parquet/CSV)**
Comprehensive metrics for all Pareto solutions:
```scala
case class SolutionSummary(
  solutionId: Int,
  timestamp: String,
  expectedResponses: Double,
  totalCost: Double,
  customerSatisfaction: Double,
  totalAssignments: Int,
  maxHourlyLoad: Int,
  utilizationRate: Double,
  emailAssignments: Int,
  smsAssignments: Int,
  pushAssignments: Int,
  inAppAssignments: Int,
  avgResponseRate: Double,
  costPerResponse: Double
)
```

### 🗂️ **Optimized Storage**

#### **HDFS Parquet Files**
- **Compression**: Snappy compression for optimal storage/query performance
- **Partitioning**: Partitioned by channel for efficient querying
- **Location**: `/campaign_optimization/` directory on HDFS
- **Naming**: Timestamped files for easy identification

#### **Local CSV Files**
- **Headers**: Self-documenting with clear column names
- **Sorting**: Customer assignments sorted by customer ID
- **Compatibility**: Standard CSV format for Excel/analysis tools

## File Structure

### HDFS Output (when available)
```
/campaign_optimization/
├── schedule_best_response_20250701_103200/
│   ├── channel=0/           # Email assignments
│   ├── channel=1/           # SMS assignments  
│   ├── channel=2/           # Push assignments
│   └── channel=3/           # In-app assignments
├── schedule_lowest_cost_20250701_103200/
├── schedule_best_satisfaction_20250701_103200/
└── solutions_summary_20250701_103200/
```

### Local Output (fallback)
```
spark_scheduler/
├── best_response_20250701_103200.csv
├── lowest_cost_20250701_103200.csv
├── best_satisfaction_20250701_103200.csv
├── solutions_summary_20250701_103200.csv
├── VAR_campaign_20250701_103200.tsv      # Decision variables
└── FUN_campaign_20250701_103200.tsv      # Objective values
```

## Usage Examples

### 🔧 **Reading Parquet Files with Spark**

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Campaign Analysis")
  .getOrCreate()

// Load schedule data
val scheduleDF = spark.read.parquet("/campaign_optimization/schedule_best_response_20250701_103200")

// Analyze by channel
scheduleDF
  .groupBy("channelName")
  .agg(
    count("customerId").alias("assignments"),
    avg("expectedResponseRate").alias("avg_response_rate"),
    sum("cost").alias("total_cost")
  )
  .show()

// Find high-value customers
scheduleDF
  .filter($"expectedResponseRate" > 0.2)
  .orderBy($"expectedResponseRate".desc)
  .show(20)

// Load solutions summary
val summaryDF = spark.read.parquet("/campaign_optimization/solutions_summary_20250701_103200")

// Find Pareto-optimal solutions
summaryDF
  .select("solutionId", "expectedResponses", "totalCost", "customerSatisfaction")
  .orderBy($"expectedResponses".desc)
  .show()
```

### 📈 **SQL Analytics**

```sql
-- Register as temporary view
CREATE OR REPLACE TEMPORARY VIEW campaign_schedule 
USING PARQUET 
LOCATION '/campaign_optimization/schedule_best_response_20250701_103200';

-- Channel performance analysis
SELECT 
  channelName,
  COUNT(*) as assignments,
  AVG(expectedResponseRate) as avg_response_rate,
  SUM(cost) as total_cost,
  SUM(cost) / SUM(expectedResponseRate) as cost_per_response
FROM campaign_schedule
GROUP BY channelName
ORDER BY avg_response_rate DESC;

-- Time slot utilization
SELECT 
  timeSlot,
  COUNT(*) as customer_count,
  AVG(expectedResponseRate) as avg_response_rate
FROM campaign_schedule
GROUP BY timeSlot
ORDER BY timeSlot;

-- High-value customer segments
SELECT 
  CASE 
    WHEN expectedResponseRate >= 0.25 THEN 'High'
    WHEN expectedResponseRate >= 0.15 THEN 'Medium'
    ELSE 'Low'
  END as response_segment,
  channelName,
  COUNT(*) as customers,
  AVG(cost) as avg_cost
FROM campaign_schedule
GROUP BY response_segment, channelName
ORDER BY response_segment DESC, avg_cost ASC;
```

### 🐍 **Python/PySpark Integration**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Campaign Analysis").getOrCreate()

# Load and analyze schedule
schedule_df = spark.read.parquet("/campaign_optimization/schedule_best_response_20250701_103200")

# Channel performance
channel_stats = schedule_df.groupBy("channelName").agg(
    count("customerId").alias("assignments"),
    avg("expectedResponseRate").alias("avg_response_rate"),
    sum("cost").alias("total_cost")
).collect()

for row in channel_stats:
    print(f"{row.channelName}: {row.assignments} assignments, "
          f"{row.avg_response_rate:.3f} avg response rate, "
          f"${row.total_cost:.2f} total cost")

# Export to Pandas for further analysis
schedule_pandas = schedule_df.toPandas()
import matplotlib.pyplot as plt

# Response rate distribution by channel
schedule_pandas.boxplot(column='expectedResponseRate', by='channelName')
plt.title('Response Rate Distribution by Channel')
plt.show()
```

## Detection Logic

### Environment Detection Flow
1. **Check Environment Variables**
   - `HADOOP_HOME`
   - `HADOOP_CONF_DIR` 
   - `YARN_CONF_DIR`

2. **Test HDFS Configuration**
   - Verify `fs.defaultFS` starts with `hdfs://`
   - Test filesystem connectivity

3. **Create Directory Structure**
   - Auto-create `/campaign_optimization/` if needed
   - Set appropriate permissions

4. **Fallback Handling**
   - Graceful degradation to local CSV
   - Clear logging of detection results

### Manual Override Options

```bash
# Force local mode (skip HDFS detection)
export FORCE_LOCAL_STORAGE=true
./build.sh test

# Set specific HDFS configuration
export HADOOP_CONF_DIR=/etc/hadoop/conf
./build.sh test

# Use specific Spark master with HDFS
SPARK_MASTER=yarn ./build.sh test
```

## Benefits

### 🚀 **Performance**
- **Parquet**: Columnar storage for fast analytics queries
- **Compression**: 60-80% storage savings with Snappy
- **Partitioning**: Channel-based partitioning for selective queries

### 📊 **Analytics Ready**
- **Spark SQL**: Direct querying without data transformation
- **DataFrame API**: Full Spark DataFrame functionality
- **Integration**: Works with Jupyter, Zeppelin, Databricks

### 🔄 **Scalability**
- **Distributed**: HDFS handles petabyte-scale data
- **Fault Tolerance**: Built-in replication and recovery
- **Parallel Processing**: Efficient parallel reads/writes

### 🛠️ **Development Friendly**
- **Auto-Detection**: Works in any environment
- **Fallback**: Local CSV for development/testing
- **Debugging**: Clear logging of storage decisions

## Production Recommendations

### HDFS Configuration
```xml
<!-- core-site.xml -->
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://namenode:9000</value>
</property>

<!-- hdfs-site.xml -->
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
```

### Spark Configuration
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar
```

### Monitoring
- Monitor HDFS disk usage: `/campaign_optimization/` directory
- Set up retention policies for old optimization results
- Use Spark History Server for job monitoring

## Troubleshooting

### Common Issues

**HDFS Not Detected**
```
Hadoop environment variables not found - saving as local CSV
```
- Solution: Set `HADOOP_HOME` or `HADOOP_CONF_DIR`

**Permission Denied**
```
Failed to save to HDFS: Permission denied
```
- Solution: Check HDFS permissions for `/campaign_optimization/`

**Out of Memory**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```
- Solution: Increase driver memory or use SimpleCampaignOptimizer for testing

### Debug Commands

```bash
# Test HDFS connectivity
hdfs dfs -ls /

# Check campaign directory
hdfs dfs -ls /campaign_optimization/

# View Parquet schema
spark-shell --packages org.apache.spark:spark-sql_2.12:3.1.3
scala> spark.read.parquet("/campaign_optimization/schedule_best_response_20250701_103200").printSchema()

# Manual cleanup
hdfs dfs -rm -r /campaign_optimization/old_results_*
```

## Migration Guide

### From CSV to Parquet
```scala
// Read existing CSV results
val csvDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("SCHEDULE_simple_campaign_20250701_103200.csv")

// Save as Parquet with partitioning
csvDF.write
  .mode("overwrite")
  .option("compression", "snappy")
  .partitionBy("channel")
  .parquet("/campaign_optimization/migrated_schedule")
```

### Batch Processing
```bash
#!/bin/bash
# Migrate all CSV files to Parquet
find . -name "SCHEDULE_*.csv" | while read file; do
  timestamp=$(echo $file | grep -o '[0-9]\{8\}_[0-9]\{6\}')
  spark-submit --class MigrationScript \
    --conf spark.sql.adaptive.enabled=true \
    migration.jar $file "/campaign_optimization/migrated_$timestamp"
done
```

---

**Next Steps:**
1. Set up Hadoop cluster for production use
2. Configure retention policies for optimization results  
3. Integrate with BI tools (Tableau, Power BI) for visualization
4. Set up automated reporting dashboards 