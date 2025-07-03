#!/bin/bash

echo "=== Memory Issues Debug Script ==="
echo "Testing with minimal resources to isolate serialization problems"
echo ""

# Very conservative settings for debugging
NUM_CUSTOMERS=${1:-10000}  # Start very small
POPULATION_SIZE=${2:-50}
MAX_EVALUATIONS=${3:-1000}
BUSINESS_THRESHOLD=${4:-0.3}

echo "Debug Configuration:"
echo "  Customers: $NUM_CUSTOMERS (very small for testing)"
echo "  Population: $POPULATION_SIZE"
echo "  Evaluations: $MAX_EVALUATIONS"
echo "  Business Threshold: $BUSINESS_THRESHOLD"
echo ""

# Build if needed
if [ ! -f "target/classes" ]; then
    echo "Building project..."
    mvn clean compile -q
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
fi

# Create timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="debug_memory_${TIMESTAMP}.log"

echo "Starting debug run (logs: $LOG_FILE)..."
echo "This should complete in 5-10 minutes if working correctly..."

# Very conservative Spark settings to avoid all memory issues
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "DebugMemory-${NUM_CUSTOMERS}-${TIMESTAMP}" \
  --queue default \
  \
  --num-executors 10 \
  --executor-cores 2 \
  --executor-memory 8g \
  --driver-memory 4g \
  --driver-cores 1 \
  \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.driver.memoryOverhead=1g \
  --conf spark.executor.memoryFraction=0.8 \
  --conf spark.driver.maxResultSize=2g \
  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrationRequired=false \
  --conf spark.kryo.unsafe=true \
  --conf spark.kryo.referenceTracking=false \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.kryoserializer.buffer=128m \
  --conf spark.serializer.objectStreamReset=50 \
  \
  --conf spark.default.parallelism=40 \
  --conf spark.sql.shuffle.partitions=40 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.rpc.askTimeout=300s \
  --conf spark.rpc.lookupTimeout=300s \
  --conf spark.rpc.message.maxSize=256 \
  \
  --conf spark.task.maxDirectResultSize=524288 \
  --conf spark.checkpoint.compress=true \
  --conf spark.rdd.compress=true \
  --conf spark.broadcast.compress=true \
  --conf spark.io.compression.codec=snappy \
  \
  --conf spark.storage.level=MEMORY_AND_DISK_SER \
  --conf spark.task.maxFailures=1 \
  --conf spark.dynamicAllocation.enabled=false \
  \
  --class org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer \
  target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar \
  \
  --num-customers $NUM_CUSTOMERS \
  --population-size $POPULATION_SIZE \
  --max-evaluations $MAX_EVALUATIONS \
  --business-priority-threshold $BUSINESS_THRESHOLD \
  --max-customers-per-hour $(($NUM_CUSTOMERS / 10)) \
  --campaign-budget $(($NUM_CUSTOMERS * 25)) \
  --customer-batch-size 2500 \
  --max-concurrent-tasks 40 \
  --checkpoint-interval 50 \
  > $LOG_FILE 2>&1

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Debug run completed successfully!"
    echo "  Log file: $LOG_FILE"
    echo ""
    echo "Memory settings are working. You can now scale up:"
    echo "  • Next test: ./debug_memory_issues.sh 50000 100 5000"
    echo "  • Then: ./run_large_scale_optimization.sh 100000 200 10000"
    echo "  • Finally: ./run_large_scale_optimization.sh 1000000 500 50000"
else
    echo "✗ Debug run failed (exit code: $EXIT_CODE)"
    echo "  Check log file: $LOG_FILE"
    echo ""
    echo "Last 30 lines of log:"
    tail -30 $LOG_FILE
    echo ""
    echo "If still failing:"
    echo "  1. Try even smaller: ./debug_memory_issues.sh 5000 20 500"
    echo "  2. Check YARN resources: yarn application -list"
    echo "  3. Try local mode: --master local[4]"
fi

echo ""
echo "Debug completed. Check the log for detailed error information." 