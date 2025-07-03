#!/bin/bash

echo "=== Minimal Test - Ultra Conservative Settings ==="
echo "Testing with absolute minimum resources to isolate core issues"
echo ""

# Extremely small settings
NUM_CUSTOMERS=${1:-5000}   # Very small
POPULATION_SIZE=${2:-20}   # Tiny population
MAX_EVALUATIONS=${3:-500}  # Few evaluations
BUSINESS_THRESHOLD=${4:-0.3}

echo "Minimal Configuration:"
echo "  Customers: $NUM_CUSTOMERS (minimal)"
echo "  Population: $POPULATION_SIZE (tiny)"
echo "  Evaluations: $MAX_EVALUATIONS (few)"
echo "  Business Threshold: $BUSINESS_THRESHOLD"
echo ""

# Build if needed
JAR_FILE="target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Building project (JAR not found)..."
    mvn clean package -DskipTests -q
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
    
    if [ ! -f "$JAR_FILE" ]; then
        echo "ERROR: JAR file was not created: $JAR_FILE"
        exit 1
    fi
    echo "JAR built successfully: $JAR_FILE"
else
    echo "JAR already exists: $JAR_FILE"
fi

# Create timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="minimal_test_${TIMESTAMP}.log"

echo "Starting minimal test (logs: $LOG_FILE)..."
echo "This should complete in 2-5 minutes if working..."

# Ultra-conservative Spark settings - absolute minimum
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "MinimalTest-${NUM_CUSTOMERS}-${TIMESTAMP}" \
  --queue default \
  \
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 4g \
  --driver-memory 2g \
  --driver-cores 1 \
  \
  --conf spark.executor.memoryOverhead=1g \
  --conf spark.driver.memoryOverhead=512m \
  --conf spark.executor.memoryFraction=0.8 \
  --conf spark.driver.maxResultSize=1g \
  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrationRequired=false \
  --conf spark.kryo.unsafe=true \
  --conf spark.kryo.referenceTracking=false \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.kryoserializer.buffer=64m \
  --conf spark.serializer.objectStreamReset=25 \
  \
  --conf spark.default.parallelism=20 \
  --conf spark.sql.shuffle.partitions=20 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=20s \
  --conf spark.rpc.askTimeout=120s \
  --conf spark.rpc.lookupTimeout=120s \
  --conf spark.rpc.message.maxSize=128 \
  \
  --conf spark.task.maxDirectResultSize=262144 \
  --conf spark.checkpoint.compress=false \
  --conf spark.rdd.compress=false \
  --conf spark.broadcast.compress=false \
  --conf spark.io.compression.codec=lz4 \
  \
  --conf spark.storage.level=MEMORY_ONLY \
  --conf spark.task.maxFailures=1 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=false \
  \
  --conf spark.executor.instances=5 \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
  \
  --class org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer \
  $JAR_FILE \
  \
  --num-customers $NUM_CUSTOMERS \
  --population-size $POPULATION_SIZE \
  --max-evaluations $MAX_EVALUATIONS \
  --business-priority-threshold $BUSINESS_THRESHOLD \
  --max-customers-per-hour $(($NUM_CUSTOMERS / 5)) \
  --campaign-budget $(($NUM_CUSTOMERS * 10)) \
  --customer-batch-size 1000 \
  --max-concurrent-tasks 20 \
  --checkpoint-interval 25 \
  > $LOG_FILE 2>&1

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Minimal test PASSED!"
    echo "  Log file: $LOG_FILE"
    echo ""
    echo "Success! The core system works. Scale up gradually:"
    echo "  1. ./minimal_test.sh 10000 50 1000"
    echo "  2. ./debug_memory_issues.sh 25000 100 2500"
    echo "  3. ./run_large_scale_optimization.sh 100000 200 10000"
else
    echo "✗ Minimal test FAILED (exit code: $EXIT_CODE)"
    echo "  Log file: $LOG_FILE"
    echo ""
    echo "Last 40 lines of error log:"
    tail -40 $LOG_FILE
    echo ""
    echo "Core issue analysis:"
    echo "  • If RejectedExecutionException: YARN resource allocation problem"
    echo "  • If OutOfMemoryError: JVM heap issues even with minimal settings"
    echo "  • If Kryo errors: Serialization issues with customer objects"
    echo "  • If ClassNotFoundException: JAR dependency problems"
    echo ""
    echo "Emergency fallback - try local mode:"
    echo "  spark-submit --master local[4] --driver-memory 8g ..."
fi

echo ""
echo "Minimal test completed." 