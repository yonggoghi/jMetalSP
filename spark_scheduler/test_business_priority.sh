#!/bin/bash

echo "=== Business Priority Threshold Comparison Test ==="
echo "Testing different thresholds to show the impact on customer utilization"
echo ""

# Maven compile first
echo "Building the project..."
mvn clean compile -q

if [ $? -eq 0 ]; then
    echo "Build successful!"
else
    echo "Build failed. Please check your code."
    exit 1
fi

# Test parameters
NUM_CUSTOMERS=1000
MAX_EVALUATIONS=1000
POPULATION_SIZE=50
MAX_CUSTOMERS_PER_HOUR=500
CAMPAIGN_BUDGET=50000

echo ""
echo "Test Parameters:"
echo "  Customers: $NUM_CUSTOMERS"
echo "  Max Evaluations: $MAX_EVALUATIONS"
echo "  Population Size: $POPULATION_SIZE"
echo "  Max Customers/Hour: $MAX_CUSTOMERS_PER_HOUR"
echo "  Campaign Budget: \$$CAMPAIGN_BUDGET"
echo ""

# Function to extract utilization from output
extract_utilization() {
    local log_file=$1
    local threshold=$2
    echo "Analyzing results for threshold $threshold..."
    
    # Look for key metrics in the log file
    local assigned=$(grep -i "total assignments" "$log_file" | head -1 | grep -o '[0-9]\+' | head -1)
    local business_filtered=$(grep -i "business.*filtered" "$log_file" | head -1 | grep -o '[0-9]\+' | head -1)
    local total_processed=$(grep -i "total.*processed" "$log_file" | head -1 | grep -o '[0-9]\+' | head -1)
    
    if [[ -n "$assigned" && -n "$total_processed" ]]; then
        local utilization=$(echo "scale=1; $assigned * 100 / $total_processed" | bc -l)
        echo "  Threshold $threshold: $assigned/$total_processed customers (${utilization}% utilization)"
        
        if [[ -n "$business_filtered" ]]; then
            local business_filter_rate=$(echo "scale=1; $business_filtered * 100 / $total_processed" | bc -l)
            echo "    Business priority filtered: $business_filtered (${business_filter_rate}%)"
        fi
    else
        echo "  Threshold $threshold: Could not extract metrics from log"
    fi
    echo ""
}

# Test different thresholds
thresholds=(0.2 0.3 0.5 0.7)

echo "Running tests with different business priority thresholds..."
echo "This will take several minutes..."
echo ""

for threshold in "${thresholds[@]}"; do
    echo "=== Testing threshold: $threshold ==="
    
    # Run optimization with current threshold
    log_file="test_threshold_${threshold}.log"
    
    java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
        org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer \
        --num-customers $NUM_CUSTOMERS \
        --max-evaluations $MAX_EVALUATIONS \
        --population-size $POPULATION_SIZE \
        --max-customers-per-hour $MAX_CUSTOMERS_PER_HOUR \
        --campaign-budget $CAMPAIGN_BUDGET \
        --business-priority-threshold $threshold \
        > "$log_file" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✓ Optimization completed for threshold $threshold"
        extract_utilization "$log_file" "$threshold"
    else
        echo "✗ Optimization failed for threshold $threshold"
        echo "Check $log_file for details"
        echo ""
    fi
done

echo "=== SUMMARY ==="
echo "Results comparison:"

for threshold in "${thresholds[@]}"; do
    log_file="test_threshold_${threshold}.log"
    if [ -f "$log_file" ]; then
        extract_utilization "$log_file" "$threshold"
    fi
done

echo "=== RECOMMENDATIONS ==="
echo "Based on these results:"
echo "• For maximum customer reach: Use threshold 0.1-0.2"
echo "• For balanced ARPU/volume: Use threshold 0.3-0.4 (current default)"
echo "• For premium customer focus: Use threshold 0.5-0.7"
echo "• For ultra-premium only: Use threshold 0.8+"
echo ""
echo "Logs saved to: test_threshold_*.log"
echo "Latest results saved to: *_campaign_*.tsv files" 