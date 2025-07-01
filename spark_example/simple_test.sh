#!/bin/bash

# Simple test script with optimized memory settings
echo "Running Campaign Scheduling Optimizer - Simple Test"
echo "===================================================="

# Check if JAR exists
if [ ! -f "target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar" ]; then
    echo "Building project first..."
    ./build.sh build
fi

echo "Starting simple test with optimized memory settings..."

# Run with optimized memory configuration
spark-submit \
    --class "org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer" \
    --master "local[2]" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.driver.memory=4g \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.executor.memory=4g \
    --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000 \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.rdd.compress=true \
    --conf spark.serializer.objectStreamReset=100 \
    target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar

echo ""
echo "Simple test completed!"
echo "Check the output files: VAR_campaign_*.tsv and FUN_campaign_*.tsv" 