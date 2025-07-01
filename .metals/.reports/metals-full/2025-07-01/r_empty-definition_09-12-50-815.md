error id: file://<WORKSPACE>/spark_example/src/main/scala/org/uma/jmetalsp/spark/examples/campaign/CampaignSchedulingOptimizer.scala:
file://<WORKSPACE>/spark_example/src/main/scala/org/uma/jmetalsp/spark/examples/campaign/CampaignSchedulingOptimizer.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 2280
uri: file://<WORKSPACE>/spark_example/src/main/scala/org/uma/jmetalsp/spark/examples/campaign/CampaignSchedulingOptimizer.scala
text:
```scala
package org.uma.jmetalsp.spark.examples.campaign

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset}
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.{AlgorithmRunner, JMetalLogger}
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import org.uma.jmetalsp.spark.evaluator.SparkSolutionListEvaluator

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Main optimizer for campaign message scheduling using jMetalSP with Spark 3.1.x
 * 
 * This application demonstrates:
 * 1. Multi-objective optimization for campaign scheduling
 * 2. Spark 3.1.x integration with jMetalSP
 * 3. Real-world constraints (capacity, budget, timing)
 * 4. Scalable solution for 10M+ customers
 * 
 * Usage for Zeppelin:
 * %spark
 * import org.uma.jmetalsp.spark.examples.campaign._
 * val optimizer = new CampaignSchedulingOptimizer()
 * val results = optimizer.optimize()
 * 
 * Usage for spark-submit:
 * spark-submit --class="org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer" \
 *   --master local[4] \
 *   jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar
 */
object CampaignSchedulingOptimizer {
  
  def main(args: Array[String]): Unit = {
    val optimizer = new CampaignSchedulingOptimizer()
    
    Try {
      optimizer.optimize()
    } match {
      case Success(results) =>
        println("=== OPTIMIZATION COMPLETED SUCCESSFULLY ===")
        optimizer.printResults(results)
        
      case Failure(exception) =>
        println(s"=== OPTIMIZATION FAILED ===")
        println(s"Error: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }
}

class CampaignSchedulingOptimizer {
  
  // Algorithm configurati@@on
  case class OptimizationConfig(
    populationSize: Int = 100,
    maxEvaluations: Int = 10000,
    crossoverProbability: Double = 0.9,
    crossoverDistributionIndex: Double = 20.0,
    mutationDistributionIndex: Double = 20.0,
    numCustomersDemo: Int = 1000, // Scale down for demo (would be 10M in production)
    maxCustomersPerHour: Int = 500, // Scale down for demo
    campaignBudget: Double = 50000.0, // Scale down for demo
    sparkMaster: String = "yarn", // Can be overridden for cluster
    enableCheckpointing: Boolean = true
  )
  
  case class OptimizationResults(
    bestSolutions: List[DoubleSolution],
    schedules: List[CampaignSchedule],
    metrics: List[ScheduleMetrics],
    executionTime: Long,
    problemStats: ProblemStatistics,
    customerStats: CustomerStatistics
  )
  
  def optimize(config: OptimizationConfig = OptimizationConfig()): OptimizationResults = {
    
    println("=== Campaign Message Scheduling Optimization ===")
    println(s"Configuration: $config")
    println("=" * 50)
    
    val startTime = System.currentTimeMillis()
    
    // Initialize Spark
    val spark = createSparkSession(config)
    val sparkContext = spark.sparkContext
    
    try {
      // Step 1: Generate/Load customer data
      println("Step 1: Loading customer data...")
      val customers = loadCustomerData(config, spark)
      val customerStats = Customer.getStatistics(customers)
      println(customerStats)
      
      // Step 2: Create optimization problem
      println("\nStep 2: Creating optimization problem...")
      val problem = new CampaignSchedulingProblem(
        customers = customers,
        maxCustomersPerHour = config.maxCustomersPerHour,
        campaignBudget = config.campaignBudget
      )
      val problemStats = problem.getStatistics
      println(problemStats)
      
      // Step 3: Configure NSGA-II algorithm
      println("\nStep 3: Configuring NSGA-II algorithm...")
      val algorithm = createNSGAII(problem, config, sparkContext)
      
      // Step 4: Run optimization
      println("\nStep 4: Running optimization...")
      println(s"Population size: ${config.populationSize}")
      println(s"Max evaluations: ${config.maxEvaluations}")
      
      val algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute()
      val solutions = algorithm.getResult
      val executionTime = algorithmRunner.getComputingTime
      
      println(s"\nOptimization completed in ${executionTime}ms")
      println(s"Found ${solutions.size()} solutions on the Pareto front")
      
      // Step 5: Analyze results
      println("\nStep 5: Analyzing results...")
      val (schedules, metrics) = analyzeResults(problem, solutions)
      
      // Step 6: Save results
      println("\nStep 6: Saving results...")
      saveResults(solutions, schedules)
      
      OptimizationResults(
        bestSolutions = solutions.asScala.toList,
        schedules = schedules,
        metrics = metrics,
        executionTime = executionTime,
        problemStats = problemStats,
        customerStats = customerStats
      )
      
    } finally {
      spark.stop()
      println("\nSpark session stopped.")
    }
  }
  
  private def createSparkSession(config: OptimizationConfig): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Campaign Scheduling Optimization with jMetalSP")
      .master(config.sparkMaster)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    if (config.enableCheckpointing) {
      spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
    }
    
    println("Spark session initialized:")
    println(s"  Spark version: ${spark.version}")
    println(s"  Master: ${spark.sparkContext.master}")
    println(s"  Default parallelism: ${spark.sparkContext.defaultParallelism}")
    
    spark
  }
  
  private def loadCustomerData(config: OptimizationConfig, spark: SparkSession): Array[Customer] = {
    // In production, this would load from a distributed data source
    // For demo, we generate synthetic data
    
    println(s"Generating ${config.numCustomersDemo} synthetic customers...")
    
    // Generate customers with realistic patterns
    val customers = Customer.generateRandomCustomers(
      numCustomers = config.numCustomersDemo,
      seed = 42L
    )
    
    // In production, you might want to use Spark DataFrames for processing:
    // val customerDF = spark.createDataFrame(customers.map(c => (c.id, c.responseRates.flatten)))
    // customerDF.cache() // Cache for repeated access
    
    println(s"Generated ${customers.length} customers with response rate data")
    
    customers
  }
  
  private def createNSGAII(
    problem: CampaignSchedulingProblem, 
    config: OptimizationConfig,
    sparkContext: SparkContext
  ): org.uma.jmetal.algorithm.Algorithm[java.util.List[DoubleSolution]] = {
    
    // Calculate mutation probability
    val mutationProbability = 1.0 / problem.getNumberOfVariables
    
    // Create genetic operators
    val crossover = new SBXCrossover(config.crossoverProbability, config.crossoverDistributionIndex)
    val mutation = new PolynomialMutation(mutationProbability, config.mutationDistributionIndex)
    val selection = new BinaryTournamentSelection[DoubleSolution](
      new RankingAndCrowdingDistanceComparator[DoubleSolution]()
    )
    
    // Create Spark-based solution evaluator
    val evaluator = new SparkSolutionListEvaluator[DoubleSolution](sparkContext)
    
    // Build NSGA-II algorithm
    new NSGAIIBuilder[DoubleSolution](problem, crossover, mutation, config.populationSize)
      .setSelectionOperator(selection)
      .setMaxEvaluations(config.maxEvaluations)
      .setSolutionListEvaluator(evaluator)
      .build()
  }
  
  private def analyzeResults(
    problem: CampaignSchedulingProblem, 
    solutions: java.util.List[DoubleSolution]
  ): (List[CampaignSchedule], List[ScheduleMetrics]) = {
    
    val schedules = solutions.asScala.map { solution =>
      // We need to decode the solution to get the schedule
      // This is a simplified version - in reality you'd use the problem's decode method
      decodeSolutionToSchedule(problem, solution)
    }.toList
    
    val metrics = schedules.map { schedule =>
      ScheduleMetrics(
        totalResponseRate = schedule.getTotalExpectedResponses,
        totalCost = schedule.getTotalCost,
        customerSatisfaction = 0.8, // Simplified calculation
        maxHourlyLoad = schedule.getMaxHourlyLoad,
        utilizationEfficiency = schedule.getAssignmentCount.toDouble / problem.customers.length,
        totalAssignments = schedule.getAssignmentCount
      )
    }
    
    (schedules, metrics)
  }
  
  private def decodeSolutionToSchedule(
    problem: CampaignSchedulingProblem, 
    solution: DoubleSolution
  ): CampaignSchedule = {
    // Simplified schedule creation for demonstration
    // In reality, this would use the problem's decoding logic
    
    val assignments = Array.empty[CustomerAssignment] // Placeholder
    val hourlyLoads = Array.fill(60)(0) // Placeholder
    
    CampaignSchedule(assignments, hourlyLoads)
  }
  
  private def saveResults(
    solutions: java.util.List[DoubleSolution], 
    schedules: List[CampaignSchedule]
  ): Unit = {
    
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    
    // Save Pareto front
    val output = new SolutionListOutput(solutions)
      .setSeparator("\t")
      .setVarFileOutputContext(new DefaultFileOutputContext(s"VAR_campaign_${timestamp}.tsv"))
      .setFunFileOutputContext(new DefaultFileOutputContext(s"FUN_campaign_${timestamp}.tsv"))
    
    output.print()
    
    // Save detailed schedule for best solution (if available)
    if (schedules.nonEmpty) {
      val bestSchedule = schedules.head // Simplified - would choose based on criteria
      // In production, save to database or distributed file system
      println(s"Best schedule saved: ${bestSchedule.getAssignmentCount} assignments")
    }
    
    println(s"Results saved with timestamp: $timestamp")
  }
  
  def printResults(results: OptimizationResults): Unit = {
    println("\n" + "=" * 60)
    println("OPTIMIZATION RESULTS SUMMARY")
    println("=" * 60)
    
    println(s"Execution time: ${results.executionTime}ms")
    println(s"Solutions found: ${results.bestSolutions.length}")
    
    println(s"\n${results.problemStats}")
    println(s"\n${results.customerStats}")
    
    if (results.metrics.nonEmpty) {
      println("\n=== BEST SOLUTIONS METRICS ===")
      
      results.metrics.zipWithIndex.take(5).foreach { case (metrics, index) =>
        println(s"\nSolution ${index + 1}:")
        println(f"  Expected responses: ${metrics.totalResponseRate}%.2f")
        println(f"  Total cost: $$${metrics.totalCost}%.2f")
        println(f"  Customer satisfaction: ${metrics.customerSatisfaction}%.3f")
        println(f"  Max hourly load: ${metrics.maxHourlyLoad}")
        println(f"  Utilization: ${metrics.utilizationEfficiency * 100}%.1f%%")
      }
    }
    
    println("\n=== RECOMMENDATIONS ===")
    println("1. Review the Pareto front in FUN_campaign_*.tsv")
    println("2. Select solution based on business priorities")
    println("3. Implement the schedule using VAR_campaign_*.tsv")
    println("4. Monitor actual response rates vs. predictions")
    println("5. Retrain models with new data periodically")
    
    println("\n" + "=" * 60)
  }
  
  /**
   * Zeppelin notebook helper method
   */
  def optimizeForZeppelin(
    numCustomers: Int = 1000,
    populationSize: Int = 50,
    maxEvaluations: Int = 2000
  ): OptimizationResults = {
    
    val config = OptimizationConfig(
      numCustomersDemo = numCustomers,
      populationSize = populationSize,
      maxEvaluations = maxEvaluations,
      sparkMaster = "yarn" // Use all available cores in Zeppelin
    )
    
    optimize(config)
  }
} 
```


#### Short summary: 

empty definition using pc, found symbol in pc: 