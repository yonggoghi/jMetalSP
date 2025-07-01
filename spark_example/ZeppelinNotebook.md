# Campaign Scheduling Optimization - Zeppelin Notebook

This notebook demonstrates how to use the Campaign Message Scheduling Optimizer in Apache Zeppelin with Spark 3.1.x.

## Setup

```scala
%spark.conf
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.driver.memory 4g
spark.executor.memory 8g
```

## Import Dependencies

```scala
%spark
import org.uma.jmetalsp.spark.examples.campaign._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Verify Spark context
println(s"Spark Version: ${spark.version}")
println(s"Master: ${spark.sparkContext.master}")
println(s"Application Name: ${spark.sparkContext.appName}")
```

## 1. Data Exploration

### Generate Sample Customers

```scala
%spark
// Generate sample customers for analysis
val numCustomers = 1000
val customers = Customer.generateRandomCustomers(numCustomers, seed = 42L)

println(s"Generated ${customers.length} customers")

// Show customer statistics
val stats = Customer.getStatistics(customers)
println(stats)
```

### Analyze Customer Response Patterns

```scala
%spark
import scala.collection.mutable.ArrayBuffer

// Analyze response rate patterns
val responseData = ArrayBuffer[(Int, Int, Double, String)]()

for {
  customer <- customers.take(10) // Sample first 10 customers
  timeSlot <- 0 until 60
  channel <- 0 until 4
} {
  val responseRate = customer.getResponseRate(timeSlot, channel)
  val channelName = channel match {
    case 0 => "Email"
    case 1 => "SMS" 
    case 2 => "Push"
    case 3 => "In-app"
  }
  responseData += ((timeSlot, channel, responseRate, channelName))
}

// Convert to DataFrame for analysis
import spark.implicits._
val responseDF = responseData.toDF("timeSlot", "channel", "responseRate", "channelName")

responseDF.show(20)
```

### Visualize Response Patterns

```scala
%spark
// Calculate average response rates by time slot and channel
val avgResponseRates = responseDF
  .groupBy("timeSlot", "channelName")
  .agg(avg("responseRate").alias("avgResponseRate"))
  .orderBy("timeSlot", "channelName")

avgResponseRates.show(50)

// Calculate peak hours for each channel
val peakHours = responseDF
  .groupBy("channelName", "timeSlot")
  .agg(avg("responseRate").alias("avgResponseRate"))
  .groupBy("channelName")
  .agg(
    max("avgResponseRate").alias("maxResponseRate"),
    first("timeSlot").alias("peakHour")
  )

peakHours.show()
```

## 2. Small Scale Optimization

### Quick Test with 100 Customers

```scala
%spark
val quickOptimizer = new CampaignSchedulingOptimizer()

val quickResults = quickOptimizer.optimizeForZeppelin(
  numCustomers = 100,
  populationSize = 20,
  maxEvaluations = 500
)

quickOptimizer.printResults(quickResults)
```

### Analyze Quick Results

```scala
%spark
// Extract metrics from quick test
val quickMetrics = quickResults.metrics

if (quickMetrics.nonEmpty) {
  val bestSolution = quickMetrics.head
  
  println("=== QUICK TEST ANALYSIS ===")
  println(f"Response Rate: ${bestSolution.totalResponseRate}%.2f")
  println(f"Cost: $$${bestSolution.totalCost}%.2f")
  println(f"Satisfaction: ${bestSolution.customerSatisfaction}%.3f")
  println(f"Efficiency: ${bestSolution.utilizationEfficiency * 100}%.1f%%")
  
  // Calculate cost per response
  val costPerResponse = if (bestSolution.totalResponseRate > 0) {
    bestSolution.totalCost / bestSolution.totalResponseRate
  } else 0.0
  
  println(f"Cost per Response: $$${costPerResponse}%.2f")
}
```

## 3. Medium Scale Optimization

### Optimize for 1000 Customers

```scala
%spark
val mediumOptimizer = new CampaignSchedulingOptimizer()

val mediumResults = mediumOptimizer.optimizeForZeppelin(
  numCustomers = 1000,
  populationSize = 50,
  maxEvaluations = 2000
)

mediumOptimizer.printResults(mediumResults)
```

## 4. Parameter Sensitivity Analysis

### Test Different Population Sizes

```scala
%spark
val populationSizes = Array(20, 50, 100)
val results = ArrayBuffer[(Int, Double, Double, Long)]()

for (popSize <- populationSizes) {
  println(s"Testing population size: $popSize")
  
  val testOptimizer = new CampaignSchedulingOptimizer()
  val testResults = testOptimizer.optimizeForZeppelin(
    numCustomers = 500,
    populationSize = popSize,
    maxEvaluations = 1000
  )
  
  if (testResults.metrics.nonEmpty) {
    val bestMetric = testResults.metrics.head
    results += ((
      popSize,
      bestMetric.totalResponseRate,
      bestMetric.totalCost,
      testResults.executionTime
    ))
  }
}

// Show sensitivity analysis results
import spark.implicits._
val sensitivityDF = results.toDF("populationSize", "responseRate", "cost", "executionTime")
sensitivityDF.show()
```

## 5. Advanced Configuration

### Custom Optimization with Business Constraints

```scala
%spark
import org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer.OptimizationConfig

// Create custom configuration for specific business scenario
val businessConfig = OptimizationConfig(
  populationSize = 75,
  maxEvaluations = 3000,
  numCustomersDemo = 1500,
  maxCustomersPerHour = 200,    // Limited capacity
  campaignBudget = 25000.0,     // Tight budget
  sparkMaster = "local[*]",
  enableCheckpointing = true
)

val businessOptimizer = new CampaignSchedulingOptimizer()
val businessResults = businessOptimizer.optimize(businessConfig)

businessOptimizer.printResults(businessResults)
```

## 6. Result Visualization

### Compare Multiple Solutions

```scala
%spark
// Extract objective values for visualization
val solutions = businessResults.bestSolutions
val objectiveData = ArrayBuffer[(Int, Double, Double, Double)]()

solutions.zipWithIndex.foreach { case (solution, index) =>
  val responseRate = -solution.getObjective(0) // Negated for maximization
  val cost = solution.getObjective(1)
  val satisfaction = -solution.getObjective(2) // Negated for maximization
  
  objectiveData += ((index, responseRate, cost, satisfaction))
}

import spark.implicits._
val objectiveDF = objectiveData.toDF("solutionIndex", "responseRate", "cost", "satisfaction")

objectiveDF.show(10)

// Find trade-offs
println("\n=== TRADE-OFF ANALYSIS ===")
val maxResponseSolution = objectiveDF.orderBy(desc("responseRate")).first()
val minCostSolution = objectiveDF.orderBy("cost").first()
val maxSatisfactionSolution = objectiveDF.orderBy(desc("satisfaction")).first()

println("Best Response Rate Solution:")
println(s"  Index: ${maxResponseSolution.getAs[Int]("solutionIndex")}")
println(f"  Response Rate: ${maxResponseSolution.getAs[Double]("responseRate")}%.2f")
println(f"  Cost: $$${maxResponseSolution.getAs[Double]("cost")}%.2f")

println("Lowest Cost Solution:")
println(s"  Index: ${minCostSolution.getAs[Int]("solutionIndex")}")
println(f"  Response Rate: ${minCostSolution.getAs[Double]("responseRate")}%.2f")
println(f"  Cost: $$${minCostSolution.getAs[Double]("cost")}%.2f")
```

## 7. Production Readiness Check

### Scale Test (Large Configuration)

```scala
%spark
// Test with larger scale (be careful with memory)
val scaleConfig = OptimizationConfig(
  populationSize = 100,
  maxEvaluations = 5000,
  numCustomersDemo = 5000,      // 5K customers
  maxCustomersPerHour = 1000,
  campaignBudget = 100000.0,
  sparkMaster = "local[*]",
  enableCheckpointing = true
)

println("=== SCALE TEST ===")
println("WARNING: This test may take several minutes...")
println(s"Configuration: $scaleConfig")

// Uncomment to run scale test
// val scaleOptimizer = new CampaignSchedulingOptimizer()
// val scaleResults = scaleOptimizer.optimize(scaleConfig)
// scaleOptimizer.printResults(scaleResults)

println("Scale test configuration ready. Uncomment to execute.")
```

## 8. Export Results

### Save Results for Production Use

```scala
%spark
// Export best solution for implementation
if (businessResults.bestSolutions.nonEmpty) {
  val bestSolution = businessResults.bestSolutions.head
  
  // In production, you would:
  // 1. Decode the solution to get actual customer assignments
  // 2. Save to database or file system
  // 3. Create execution timeline
  // 4. Set up monitoring
  
  println("=== IMPLEMENTATION READY ===")
  println("Best solution selected for implementation:")
  println(f"  Response Rate: ${-bestSolution.getObjective(0)}%.2f")
  println(f"  Cost: $$${bestSolution.getObjective(1)}%.2f")
  println(f"  Satisfaction: ${-bestSolution.getObjective(2)}%.3f")
  
  // Export decision variables
  val variables = (0 until bestSolution.getNumberOfVariables)
    .map(i => bestSolution.getVariable(i))
    .toArray
  
  println(s"Decision variables: ${variables.length} values")
  println(s"First 10 variables: ${variables.take(10).mkString(", ")}")
}
```

## 9. Performance Monitoring

### Monitor Spark Performance

```scala
%spark
// Check Spark application metrics
val sparkContext = spark.sparkContext
val statusTracker = sparkContext.statusTracker

println("=== SPARK PERFORMANCE METRICS ===")
println(s"Application ID: ${sparkContext.applicationId}")
println(s"Default Parallelism: ${sparkContext.defaultParallelism}")

val executorInfos = statusTracker.getExecutorInfos
executorInfos.foreach { executor =>
  println(s"Executor ${executor.executorId}:")
  println(s"  Host: ${executor.executorHost}")
  println(s"  Total Cores: ${executor.totalCores}")
  println(s"  Max Memory: ${executor.maxMemory / (1024 * 1024)} MB")
}
```

## Summary

This notebook demonstrated:

1. **Data Exploration**: Understanding customer response patterns
2. **Progressive Testing**: Starting small and scaling up
3. **Parameter Tuning**: Finding optimal algorithm settings
4. **Business Constraints**: Applying real-world limitations
5. **Result Analysis**: Interpreting multi-objective trade-offs
6. **Production Readiness**: Preparing for large-scale deployment

### Next Steps for Production:

1. **Data Integration**: Connect to real customer databases
2. **Real-time Updates**: Implement streaming response data
3. **A/B Testing**: Compare optimized vs. baseline campaigns
4. **Automated Reoptimization**: Schedule regular optimization runs
5. **Monitoring Dashboard**: Track actual vs. predicted performance

### Recommended Resources:

- [jMetalSP Documentation](http://jmetalsp.uma.es/)
- [Apache Spark Tuning Guide](https://spark.apache.org/docs/3.1.3/tuning.html)
- [Multi-objective Optimization Best Practices](https://doi.org/10.1016/j.asoc.2018.02.015) 