#!/bin/bash

echo "=== Local Mode Test - Bypass YARN Completely ==="
echo "Testing with local Spark mode to isolate YARN vs application issues"
echo ""

# Small settings for local testing
NUM_CUSTOMERS=${1:-5000}
POPULATION_SIZE=${2:-20}
MAX_EVALUATIONS=${3:-500}
BUSINESS_THRESHOLD=${4:-0.3}

echo "Local Test Configuration:"
echo "  Customers: $NUM_CUSTOMERS"
echo "  Population: $POPULATION_SIZE"
echo "  Evaluations: $MAX_EVALUATIONS"
echo "  Business Threshold: $BUSINESS_THRESHOLD"
echo "  Mode: local[4] (no YARN)"
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
LOG_FILE="local_test_${TIMESTAMP}.log"

echo "Starting local mode test (logs: $LOG_FILE)..."
echo "This bypasses YARN completely and runs on local machine..."

# Local mode - no YARN, no cluster complexity
spark-submit \
  --master local[4] \
  --name "LocalTest-${NUM_CUSTOMERS}-${TIMESTAMP}" \
  \
  --driver-memory 8g \
  \
  --conf spark.driver.maxResultSize=4g \
  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrationRequired=false \
  --conf spark.kryo.unsafe=true \
  --conf spark.kryo.referenceTracking=false \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.kryoserializer.buffer=128m \
  --conf spark.serializer.objectStreamReset=50 \
  \
  --conf spark.default.parallelism=8 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=false \
  \
  --conf spark.network.timeout=600s \
  --conf spark.rpc.askTimeout=300s \
  --conf spark.rpc.lookupTimeout=300s \
  --conf spark.rpc.message.maxSize=256 \
  \
  --conf spark.task.maxDirectResultSize=524288 \
  --conf spark.storage.level=MEMORY_ONLY \
  --conf spark.task.maxFailures=2 \
  \
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
  --max-concurrent-tasks 8 \
  --checkpoint-interval 25 \
  --disable-checkpointing \
  > $LOG_FILE 2>&1

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Local mode test PASSED!"
    echo "  Log file: $LOG_FILE"
    echo ""
    echo "SUCCESS: Application works in local mode!"
    echo "This means the issue is likely YARN/cluster configuration."
    echo ""
    echo "Next steps:"
    echo "  1. Check YARN resource availability: yarn node -list"
    echo "  2. Check YARN queue limits: yarn queue -status default"
    echo "  3. Try smaller YARN test: ./minimal_test.sh 2500 10 250"
    echo "  4. Contact cluster admin about YARN executor allocation"
else
    echo "✗ Local mode test FAILED (exit code: $EXIT_CODE)"
    echo "  Log file: $LOG_FILE"
    echo ""
    echo "Last 30 lines of error log:"
    tail -30 $LOG_FILE
    echo ""
    echo "CRITICAL: If local mode fails, the issue is in the application code itself."
    echo ""
    echo "Possible application issues:"
    echo "  • Customer object serialization problems"
    echo "  • jMetal library compatibility issues"
    echo "  • Scala version conflicts"
    echo "  • Memory allocation even with small datasets"
    echo ""
    echo "Debug steps:"
    echo "  1. Check Java version: java -version"
    echo "  2. Check Scala version in pom.xml"
    echo "  3. Try even smaller: ./local_test.sh 1000 10 100"
fi

echo ""
echo "Local mode test completed." 