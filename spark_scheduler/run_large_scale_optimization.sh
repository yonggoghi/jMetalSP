#!/bin/bash

echo "=== Large-Scale Campaign Optimization Runner ==="
echo "Optimized for 100K+ customers with memory management"
echo ""

# Configuration
NUM_CUSTOMERS=${1:-100000}
POPULATION_SIZE=${2:-200}
MAX_EVALUATIONS=${3:-20000}
BUSINESS_THRESHOLD=${4:-0.3}

# Cluster configuration (adjust for your 100-node cluster)
NUM_EXECUTORS=200
EXECUTOR_CORES=4
EXECUTOR_MEMORY="28g"
EXECUTOR_OVERHEAD="4g"
DRIVER_MEMORY="8g"
DRIVER_OVERHEAD="2g"

echo "Configuration:"
echo "  Customers: $NUM_CUSTOMERS"
echo "  Population: $POPULATION_SIZE"
echo "  Evaluations: $MAX_EVALUATIONS"
echo "  Business Threshold: $BUSINESS_THRESHOLD"
echo "  Executors: $NUM_EXECUTORS"
echo "  Executor Memory: $EXECUTOR_MEMORY + $EXECUTOR_OVERHEAD overhead"
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
LOG_FILE="large_optimization_${TIMESTAMP}.log"

echo "Starting optimization (logs: $LOG_FILE)..."
echo "This may take 1-3 hours for large datasets..."

# Run optimization with comprehensive memory and serialization settings
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "CampaignOptimizer-${NUM_CUSTOMERS}-${TIMESTAMP}" \
  --queue default \
  \
  --num-executors $NUM_EXECUTORS \
  --executor-cores $EXECUTOR_CORES \
  --executor-memory $EXECUTOR_MEMORY \
  --driver-memory $DRIVER_MEMORY \
  --driver-cores 2 \
  \
  --conf spark.executor.memoryOverhead=$EXECUTOR_OVERHEAD \
  --conf spark.driver.memoryOverhead=$DRIVER_OVERHEAD \
  --conf spark.executor.memoryFraction=0.8 \
  --conf spark.driver.maxResultSize=4g \
  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrationRequired=false \
  --conf spark.kryo.unsafe=true \
  --conf spark.kryo.referenceTracking=false \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.kryoserializer.buffer=256m \
  --conf spark.serializer.objectStreamReset=100 \
  \
  --conf spark.default.parallelism=800 \
  --conf spark.sql.shuffle.partitions=800 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  \
  --conf spark.network.timeout=1200s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.rpc.askTimeout=600s \
  --conf spark.rpc.lookupTimeout=600s \
  --conf spark.rpc.message.maxSize=512 \
  \
  --conf spark.task.maxDirectResultSize=1048576 \
  --conf spark.checkpoint.compress=true \
  --conf spark.rdd.compress=true \
  --conf spark.broadcast.compress=true \
  --conf spark.io.compression.codec=snappy \
  \
  --conf spark.storage.level=MEMORY_AND_DISK_SER \
  --conf spark.persistence.storageFraction=0.3 \
  \
  --conf spark.task.maxFailures=3 \
  --conf spark.stage.maxConsecutiveAttempts=8 \
  --conf spark.blacklist.enabled=true \
  \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=true \
  \
  --class org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer \
  target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar \
  \
  --num-customers $NUM_CUSTOMERS \
  --population-size $POPULATION_SIZE \
  --max-evaluations $MAX_EVALUATIONS \
  --business-priority-threshold $BUSINESS_THRESHOLD \
  --max-customers-per-hour $(($NUM_CUSTOMERS / 20)) \
  --campaign-budget $(($NUM_CUSTOMERS * 50)) \
  --customer-batch-size 5000 \
  --max-concurrent-tasks 800 \
  --checkpoint-interval 100 \
  > $LOG_FILE 2>&1

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Optimization completed successfully!"
    echo "  Log file: $LOG_FILE"
    echo "  Results: *_campaign_${TIMESTAMP}.* files"
    
    # Show summary if available
    if [ -f "solutions_summary_${TIMESTAMP}.csv" ]; then
        echo ""
        echo "Quick Summary:"
        head -5 "solutions_summary_${TIMESTAMP}.csv" | column -t -s','
    fi
else
    echo "✗ Optimization failed (exit code: $EXIT_CODE)"
    echo "  Check log file: $LOG_FILE"
    echo ""
    echo "Last 20 lines of log:"
    tail -20 $LOG_FILE
    echo ""
    echo "Common issues and solutions:"
    echo "  • OutOfMemoryError: Reduce --num-customers or increase cluster memory"
    echo "  • Task too large: Reduce --customer-batch-size (current: 5000)"
    echo "  • Serialization errors: Check Kryo buffer settings"
    echo "  • Network timeouts: Increase --network-timeout values"
fi

echo ""
echo "Resource usage recommendations for next run:"
echo "  • For 1M customers: --num-customers 1000000 --population-size 500"
echo "  • For memory issues: --customer-batch-size 5000"
echo "  • For speed: --max-evaluations 10000 (faster but less optimal)"
echo "  • For quality: --max-evaluations 50000 (slower but better results)" 