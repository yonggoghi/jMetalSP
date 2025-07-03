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
import java.net.{Socket, ConnectException}
import java.io.IOException

/**
 * Main optimizer for campaign message scheduling using jMetalSP with Spark 3.1.x
 * 
 * This application demonstrates:
 * 1. Multi-objective optimization for campaign scheduling
 * 2. Spark 3.1.x integration with jMetalSP
 * 3. Real-world constraints (capacity, budget, timing)
 * 4. Scalable solution for 10M+ customers
 * 5. Automatic YARN detection for cluster vs local execution
 * 
 * Usage for Zeppelin:
 * %spark
 * import org.uma.jmetalsp.spark.examples.campaign._
 * val optimizer = new CampaignSchedulingOptimizer()
 * val results = optimizer.optimizeWithSpark(spark)
 * 
 * Usage for spark-submit:
 * spark-submit --class="org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer" \
 *   --master yarn \
 *   jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar
 */
object CampaignSchedulingOptimizer {
  
  def main(args: Array[String]): Unit = {
    val optimizer = new CampaignSchedulingOptimizer()
    
    // Parse command line arguments
    val config = parseArgs(args, optimizer)
    
    println(s"Starting optimization with configuration: $config")
    
    Try {
      // Create Spark session for standalone usage only
      val spark = optimizer.createSparkSession(config)
      try {
        val results = optimizer.optimizeWithSpark(spark, config)
        optimizer.printResults(results)
      } finally {
        spark.stop()
        println("\nSpark session stopped.")
      }
    } match {
      case Success(results) =>
        println("=== OPTIMIZATION COMPLETED SUCCESSFULLY ===")
        
      case Failure(exception) =>
        println(s"=== OPTIMIZATION FAILED ===")
        println(s"Error: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }
  
  private def parseArgs(args: Array[String], optimizer: CampaignSchedulingOptimizer): optimizer.OptimizationConfig = {
    var populationSize = 100
    var maxEvaluations = 10000
    var crossoverProbability = 0.9
    var crossoverDistributionIndex = 20.0
    var mutationDistributionIndex = 20.0
    var numCustomersDemo = 1000
    var maxCustomersPerHour = 500
    var campaignBudget = 50000.0
    var businessPriorityThreshold = 0.3 // Lowered from 0.5 for better utilization

    var enableCheckpointing = true
    var customerBatchSize = 50000
    var solutionCacheLevel = "MEMORY_AND_DISK_SER"
    var checkpointInterval = 100
    var maxConcurrentTasks = 800
    var saveResults = true // Default to saving results
    
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--population-size" =>
          i += 1
          if (i < args.length) populationSize = args(i).toInt
        case "--max-evaluations" =>
          i += 1
          if (i < args.length) maxEvaluations = args(i).toInt
        case "--crossover-probability" =>
          i += 1
          if (i < args.length) crossoverProbability = args(i).toDouble
        case "--crossover-distribution-index" =>
          i += 1
          if (i < args.length) crossoverDistributionIndex = args(i).toDouble
        case "--mutation-distribution-index" =>
          i += 1
          if (i < args.length) mutationDistributionIndex = args(i).toDouble
        case "--num-customers" =>
          i += 1
          if (i < args.length) numCustomersDemo = args(i).toInt
        case "--max-customers-per-hour" =>
          i += 1
          if (i < args.length) maxCustomersPerHour = args(i).toInt
        case "--campaign-budget" =>
          i += 1
          if (i < args.length) campaignBudget = args(i).toDouble
        case "--business-priority-threshold" =>
          i += 1
          if (i < args.length) businessPriorityThreshold = args(i).toDouble

        case "--disable-checkpointing" =>
          enableCheckpointing = false
        case "--customer-batch-size" =>
          i += 1
          if (i < args.length) customerBatchSize = args(i).toInt
        case "--solution-cache-level" =>
          i += 1
          if (i < args.length) solutionCacheLevel = args(i)
        case "--checkpoint-interval" =>
          i += 1
          if (i < args.length) checkpointInterval = args(i).toInt
        case "--max-concurrent-tasks" =>
          i += 1
          if (i < args.length) maxConcurrentTasks = args(i).toInt
        case "--save-results" =>
          i += 1
          if (i < args.length) saveResults = args(i).toBoolean
        case "--no-save-results" =>
          saveResults = false
        case "--help" =>
          printUsage()
          System.exit(0)
        case unknown =>
          println(s"Unknown argument: $unknown")
          printUsage()
          System.exit(1)
      }
      i += 1
    }
    
    optimizer.OptimizationConfig(
      populationSize = populationSize,
      maxEvaluations = maxEvaluations,
      crossoverProbability = crossoverProbability,
      crossoverDistributionIndex = crossoverDistributionIndex,
      mutationDistributionIndex = mutationDistributionIndex,
      numCustomersDemo = numCustomersDemo,
      maxCustomersPerHour = maxCustomersPerHour,
      campaignBudget = campaignBudget,
      businessPriorityThreshold = businessPriorityThreshold,

      enableCheckpointing = enableCheckpointing,
      customerBatchSize = customerBatchSize,
      solutionCacheLevel = solutionCacheLevel,
      checkpointInterval = checkpointInterval,
      maxConcurrentTasks = maxConcurrentTasks,
      saveResults = saveResults
    )
  }
  
  private def printUsage(): Unit = {
    println("Usage: CampaignSchedulingOptimizer [options]")
    println()
    println("Options:")
    println("  --population-size <int>              Population size for NSGA-II (default: 100)")
    println("  --max-evaluations <int>              Maximum number of evaluations (default: 10000)")
    println("  --crossover-probability <double>     Crossover probability (default: 0.9)")
    println("  --crossover-distribution-index <double> Crossover distribution index (default: 20.0)")
    println("  --mutation-distribution-index <double>  Mutation distribution index (default: 20.0)")
    println("  --num-customers <int>                Number of customers for demo (default: 1000)")
    println("  --max-customers-per-hour <int>       Max customers per hour capacity (default: 500)")
    println("  --campaign-budget <double>           Campaign budget (default: 50000.0)")
    println("  --business-priority-threshold <double> Business priority threshold (default: 0.3)")
    println("  --disable-checkpointing              Disable Spark checkpointing")
    println("  --customer-batch-size <int>           Customer batch size (default: 50000)")
    println("  --solution-cache-level <string>       Solution cache level (default: MEMORY_AND_DISK_SER)")
    println("  --checkpoint-interval <int>          Checkpoint interval (default: 100)")
    println("  --max-concurrent-tasks <int>         Maximum concurrent tasks (default: 800)")
    println("  --save-results <true|false>          Save optimization results to files (default: true)")
    println("  --no-save-results                    Disable saving results (same as --save-results false)")
    println("  --help                               Show this help message")
    println()
    println("Examples:")
    println("  # Basic usage with default parameters")
    println("  spark-submit --class CampaignSchedulingOptimizer app.jar")
    println()
    println("  # Custom configuration")
    println("  spark-submit --class CampaignSchedulingOptimizer app.jar \\")
    println("    --population-size 200 \\")
    println("    --max-evaluations 20000 \\")
    println("    --num-customers 5000 \\")
    println("    --campaign-budget 100000")
    println()
    println("  # Run without saving results")
    println("  spark-submit --class CampaignSchedulingOptimizer app.jar \\")
    println("    --no-save-results")
    println()
    println("  # YARN cluster mode")
    println("  spark-submit --master yarn --deploy-mode cluster \\")
    println("    --class CampaignSchedulingOptimizer app.jar \\")
    println("    --num-customers 10000 \\")
    println("    --max-evaluations 50000")
    println()
    println("  # Optimize business priority threshold for better utilization")
    println("  spark-submit --class CampaignSchedulingOptimizer app.jar \\")
    println("    --num-customers 1000 \\")
    println("    --business-priority-threshold 0.2 \\")
    println("    --max-customers-per-hour 800")
    println()
    println("Business Priority Thresholds:")
    println("  0.1-0.2: High utilization (70-90% customers), lower ARPU focus")
    println("  0.3-0.4: Balanced utilization (50-70% customers), good ARPU/volume trade-off")
    println("  0.5-0.7: Premium focus (30-50% customers), highest ARPU customers only")
    println("  0.8+   : Ultra-premium (10-30% customers), top-tier customers only")
  }
}

class CampaignSchedulingOptimizer {
  
  // Algorithm configuration
  case class OptimizationConfig(
    populationSize: Int = 100,
    maxEvaluations: Int = 10000,
    crossoverProbability: Double = 0.9,
    crossoverDistributionIndex: Double = 20.0,
    mutationDistributionIndex: Double = 20.0,
    numCustomersDemo: Int = 1000, // Scale down for demo (would be 10M in production)
    maxCustomersPerHour: Int = 500, // Scale down for demo
    campaignBudget: Double = 50000.0, // Scale down for demo
    businessPriorityThreshold: Double = 0.3, // ARPU-based business priority threshold

    enableCheckpointing: Boolean = true,
    customerBatchSize: Int = 50000,        // Process customers in batches
    solutionCacheLevel: String = "MEMORY_AND_DISK_SER",
    checkpointInterval: Int = 100,         // Checkpoint every 100 generations
    maxConcurrentTasks: Int = 800,         // Match parallelism
    saveResults: Boolean = true            // Whether to save results to files
  )
  
  case class OptimizationResults(
    bestSolutions: List[DoubleSolution],
    schedules: List[CampaignSchedule],
    metrics: List[ScheduleMetrics],
    executionTime: Long,
    problemStats: ProblemStatistics,
    customerStats: CustomerStatistics
  )
  
  // Instead of sending full Customer objects, send only essential data
  case class CustomerEssentials(
    id: Long,
    arpu: Double,
    tier: Int,
    lifetimeValue: Double
  )
  
  /**
   * Main optimization method that accepts an existing SparkSession
   * This is the preferred method for Zeppelin or when you already have a Spark session
   */
  def optimizeWithSpark(spark: SparkSession, config: OptimizationConfig = OptimizationConfig()): OptimizationResults = {
    
    println("=== Campaign Message Scheduling Optimization ===")
    println(s"Configuration: $config")
    println("=" * 50)
    
    val startTime = System.currentTimeMillis()
    val sparkContext = spark.sparkContext
    
    // Step 1: Generate/Load customer data
    println("Step 1: Loading customer data...")
    val customers = loadCustomerDataOptimized(config, spark)
    val customerStats = Customer.getStatistics(customers)
    println(customerStats)
    
    // Broadcast customer data to all executors once
    val broadcastCustomers = sparkContext.broadcast(customers)
    
    // Step 2: Create optimization problem
    println("\nStep 2: Creating optimization problem...")
    val problem = new CampaignSchedulingProblem(
      customers = customers,
      maxCustomersPerHour = config.maxCustomersPerHour,
      campaignBudget = config.campaignBudget,
      businessPriorityThreshold = config.businessPriorityThreshold
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
    
    // Step 6: Save results (conditional)
    if (config.saveResults) {
      println("\nStep 6: Saving results...")
      saveResults(solutions, schedules, problem, spark)
    } else {
      println("\nStep 6: Skipping result saving (disabled by --no-save-results)")
    }
    
    OptimizationResults(
      bestSolutions = solutions.asScala.toList,
      schedules = schedules,
      metrics = metrics,
      executionTime = executionTime,
      problemStats = problemStats,
      customerStats = customerStats
    )
  }
  

  
  private def createSparkSession(config: OptimizationConfig): SparkSession = {
    val spark: SparkSession = {
      SparkSession.builder()
        .appName("CampaignScheduler")
        .config("spark.hadoop.metastore.catalog.default","hive")
        .enableHiveSupport()
        .getOrCreate()
    }

    spark.sparkContext.setLogLevel("WARN")
    
    if (config.enableCheckpointing) {
      spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
    }
    
    println("Spark session initialized:")
    println(s"  Spark version: ${spark.version}")
    println(s"  Master: ${spark.sparkContext.master}")
    println(s"  Default parallelism: ${spark.sparkContext.defaultParallelism}")
    println(s"  Deploy mode: ${spark.sparkContext.deployMode}")
    
    spark
  }
  
  private def loadCustomerDataOptimized(config: OptimizationConfig, spark: SparkSession): Array[Customer] = {
    import spark.implicits._
    
    println(s"Generating ${config.numCustomersDemo} customers with memory-optimized processing...")
    
    // CRITICAL: Use much smaller batch sizes to prevent large task serialization
    val safeBatchSize = Math.min(config.customerBatchSize, 10000) // Max 10K per batch
    val numBatches = (config.numCustomersDemo + safeBatchSize - 1) / safeBatchSize
    
    println(s"Processing in ${numBatches} batches of ${safeBatchSize} customers each")
    
    // Generate customers in sequential batches to control memory
    val customers = (0 until numBatches).flatMap { batchIndex =>
      val startId = batchIndex * safeBatchSize
      val endId = Math.min(startId + safeBatchSize, config.numCustomersDemo)
      
      println(s"Generating batch ${batchIndex + 1}/${numBatches}: customers ${startId} to ${endId-1}")
      
      val batchCustomers = Customer.generateRandomCustomers(
        numCustomers = endId - startId,
        seed = 42L + batchIndex
      )
      
      // Process batch immediately to avoid memory accumulation
      batchCustomers
    }.toArray
    
    println(s"Generated ${customers.length} customers successfully")
    
    // Store customer metadata in Spark for distributed access (but not the full objects)
    storeCustomerMetadata(customers, spark)
    
    customers
  }
  
  private def storeCustomerMetadata(customers: Array[Customer], spark: SparkSession): Unit = {
    import spark.implicits._
    
    try {
      // Store only essential metadata, not full customer objects
      val customerMetadata = customers.map { c =>
        (c.id, c.arpu, c.tier, c.lifetimeValue, c.getBusinessPriority)
      }.toSeq.toDF("id", "arpu", "tier", "ltv", "businessPriority")
      
      // Cache metadata for fast lookups
      customerMetadata.cache()
      customerMetadata.count() // Force caching
      customerMetadata.createOrReplaceTempView("customer_metadata")
      
      println(s"Cached metadata for ${customerMetadata.count()} customers")
      
    } catch {
      case e: Exception =>
        println(s"Warning: Failed to cache customer metadata: ${e.getMessage}")
        // Continue without caching - not critical for operation
    }
  }
  
  private def optimizeCustomerData(customers: Array[Customer], spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Convert to more memory-efficient format
    val customerDF = customers.toSeq.toDF()
    customerDF.cache()
    customerDF.createOrReplaceTempView("customers")
    
    // Pre-compute business priorities to avoid repeated calculations
    spark.sql("""
      SELECT id, arpu, tier, lifetimeValue,
             (LEAST(2.0, arpu / 50.0) * 0.5 + 
              CASE tier WHEN 3 THEN 1.5 WHEN 2 THEN 1.0 ELSE 0.7 END * 0.3 + 
              LEAST(2.0, lifetimeValue / 600.0) * 0.2) as businessPriority
      FROM customers
    """).createOrReplaceTempView("customer_priorities")
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
    
    if (solutions.isEmpty) {
      println("No solutions found!")
      return (List.empty, List.empty)
    }
    
    println(s"Analyzing ${solutions.size()} solutions...")
    
    // Analyze best solution for each objective
    val solutionsList = solutions.asScala.toList
    
    // Find solution with best response rate (lowest value since we negate it)
    val bestResponseSolution = solutionsList.minBy(_.getObjective(0))
    
    // Find solution with best ARPU-weighted value (lowest value since we negate it)
    val bestValueSolution = solutionsList.minBy(_.getObjective(1))
    
    println("\n=== BEST SOLUTIONS ANALYSIS ===")
    println("Best Response Rate Solution:")
    printSolutionDetails(bestResponseSolution)
    printScheduleDetails(bestResponseSolution, problem, "BEST RESPONSE RATE")
    
    println("\nBest ARPU-Weighted Value Solution:")
    printSolutionDetails(bestValueSolution)
    printScheduleDetails(bestValueSolution, problem, "BEST ARPU-WEIGHTED VALUE")
    
    // Print summary statistics
    printSolutionStatistics(solutionsList)
    
    // Return empty lists for now since we're focusing on the analysis output
    (List.empty, List.empty)
  }
  
  private def printSolutionDetails(solution: DoubleSolution): Unit = {
    val responseRate = -solution.getObjective(0) // Un-negate
    val arpuWeightedValue = -solution.getObjective(1) // Un-negate
    val totalCost = Option(solution.getAttribute("TotalCost")).map(_.toString.toDouble).getOrElse(0.0)
    
    println(f"  Expected responses: ${responseRate}%.2f")
    println(f"  ARPU-weighted value: $$${arpuWeightedValue}%.2f")
    println(f"  Total cost: $$${totalCost}%.2f")
    
    // Check constraint violations
    val capacityViolation = Option(solution.getAttribute("CapacityViolation")).map(_.toString.toDouble).getOrElse(0.0)
    val budgetViolation = Option(solution.getAttribute("BudgetViolation")).map(_.toString.toDouble).getOrElse(0.0)
    
    if (capacityViolation > 0 || budgetViolation > 0) {
      println(f"  Constraint Violations:")
      if (capacityViolation > 0) println(f"    Capacity: ${capacityViolation}%.2f")
      if (budgetViolation > 0) println(f"    Budget: $$${budgetViolation}%.2f")
    } else {
      println("  All constraints satisfied")
    }
    
    // Calculate utilization
    val totalVariables = solution.getNumberOfVariables
    val assignmentCount = (0 until totalVariables by 3).count { i =>
      solution.getVariableValue(i + 2) > 0.3 // Priority threshold
    }
    val utilization = assignmentCount.toDouble / (totalVariables / 3) * 100
    println(f"  Utilization: ${utilization}%.1f%%")
    
    // Calculate max hourly load
    val hourlyLoads = Array.fill(60)(0)
    for (i <- 0 until totalVariables by 3) {
      val priority = solution.getVariableValue(i + 2)
      if (priority > 0.3) {
        val timeSlot = Math.floor(solution.getVariableValue(i)).toInt
        if (timeSlot >= 0 && timeSlot < 60) {
          hourlyLoads(timeSlot) += 1
        }
      }
    }
    val maxHourlyLoad = hourlyLoads.max
    println(f"  Max hourly load: ${maxHourlyLoad}")
  }
  
  private def printScheduleDetails(solution: DoubleSolution, problem: CampaignSchedulingProblem, label: String): Unit = {
    println(s"\n=== $label SCHEDULE ===")
    
    // Decode the solution into a schedule
    val schedule = decodeScheduleFromSolution(solution, problem)
    
    // DIAGNOSTIC: Analyze why customers are not assigned
    println("\n=== ASSIGNMENT ANALYSIS ===")
    val businessPriorityFilteredOut = Option(solution.getAttribute("BusinessPriorityFilteredOut")).map(_.toString.toInt).getOrElse(0)
    val contactConstraintFilteredOut = Option(solution.getAttribute("ContactConstraintFilteredOut")).map(_.toString.toInt).getOrElse(0)
    val capacityFilteredOut = Option(solution.getAttribute("CapacityFilteredOut")).map(_.toString.toInt).getOrElse(0)
    val totalProcessed = Option(solution.getAttribute("TotalProcessed")).map(_.toString.toInt).getOrElse(0)
    val totalAssigned = totalProcessed - businessPriorityFilteredOut - contactConstraintFilteredOut - capacityFilteredOut
    
    println(f"Total customers: ${problem.customers.length}")
    println(f"Business priority threshold filtered out: $businessPriorityFilteredOut")
    println(f"Contact constraint (48h) filtered out: $contactConstraintFilteredOut")
    println(f"Hourly capacity constraint filtered out: $capacityFilteredOut")
    println(f"Successfully assigned: $totalAssigned")
    println(f"Max customers per hour setting: ${problem.maxCustomersPerHour}")
    println(f"Campaign budget setting: ${problem.campaignBudget}")
    
    // Show business priority distribution
    val customerPriorities = problem.customers.map(_.getBusinessPriority)
    val avgBusinessPriority = customerPriorities.sum / customerPriorities.length
    val maxBusinessPriority = customerPriorities.max
    val minBusinessPriority = customerPriorities.min
    val highPriorityCustomers = customerPriorities.count(_ > 1.0)
    
    println(f"\n=== BUSINESS PRIORITY DISTRIBUTION ===")
    println(f"Average customer business priority: ${avgBusinessPriority}%.3f")
    println(f"Min business priority: ${minBusinessPriority}%.3f")
    println(f"Max business priority: ${maxBusinessPriority}%.3f")
    println(f"High-value customers (priority > 1.0): $highPriorityCustomers")
    // Get threshold from problem statistics 
    val problemStats = problem.getStatistics
    println(f"Business priority threshold: ${problemStats.businessPriorityThreshold}%.2f")
    
    // Show actual schedule details
    println(f"\nSchedule contains ${schedule.assignments.length} customer assignments:")
    println("CustomerID\tTimeSlot\tChannel\tExpectedResponse\tCost\tPriority")
    println("-" * 80)
    
    // Sort assignments by customer ID for easier reading
    val sortedAssignments = schedule.assignments.sortBy(_.customerId)
    
    // Show first 20 assignments as sample
    sortedAssignments.take(20).foreach { assignment =>
      val channelName = assignment.channel match {
        case 0 => "Email"
        case 1 => "SMS"
        case 2 => "Push"
        case 3 => "In-app"
        case _ => s"Ch${assignment.channel}"
      }
      
      println(f"${assignment.customerId}%10d\t${assignment.timeSlot}%8d\t${channelName}%7s\t${assignment.expectedResponseRate}%15.3f\t$$${assignment.cost}%4.2f\t${assignment.priority}%8.3f")
    }
    
    if (sortedAssignments.length > 20) {
      println(f"... and ${sortedAssignments.length - 20} more assignments")
    }
    
    // Show hourly distribution
    println(f"\nHourly Load Distribution (first 24 hours):")
    println("Hour\tAssignments\tCapacity")
    println("-" * 30)
    schedule.hourlyLoads.take(24).zipWithIndex.foreach { case (load, hour) =>
      if (load > 0) {
        println(f"${hour}%4d\t${load}%11d\t${problem.maxCustomersPerHour}%8d")
      }
    }
    
    // Channel distribution
    val channelCounts = schedule.assignments.groupBy(_.channel).mapValues(_.length)
    println(f"\nChannel Distribution:")
    println("Channel\tAssignments")
    println("-" * 20)
    channelCounts.toSeq.sortBy(_._1).foreach { case (channel, count) =>
      val channelName = channel match {
        case 0 => "Email"
        case 1 => "SMS"
        case 2 => "Push"
        case 3 => "In-app"
        case _ => s"Channel $channel"
      }
      println(f"${channelName}%7s\t${count}%11d")
    }
    
    println(f"\n=== RECOMMENDATIONS ===")
    if (businessPriorityFilteredOut > totalAssigned) {
      println("• MAIN ISSUE: Business priority threshold is filtering out most customers")
      println(f"• Current threshold: ${problemStats.businessPriorityThreshold}%.2f")
      println("• Consider lowering the business priority threshold in CampaignSchedulingProblem.scala")
      println("• Or increase population size and max evaluations to evolve better ARPU-based priorities")
    }
    if (capacityFilteredOut > 0) {
      println(f"• Hourly capacity constraint blocked $capacityFilteredOut assignments")
      println(f"• Consider increasing --max-customers-per-hour (current: ${problem.maxCustomersPerHour})")
    }
    if (contactConstraintFilteredOut > 0) {
      println(f"• 48-hour contact constraint blocked $contactConstraintFilteredOut assignments")
    }
    
    // ARPU-specific recommendations
    val lowArpuCustomers = problem.customers.count(_.arpu < 30.0)
    val highArpuCustomers = problem.customers.count(_.arpu > 80.0)
    if (lowArpuCustomers > highArpuCustomers && businessPriorityFilteredOut > totalAssigned) {
      println(f"• Many low-ARPU customers (${lowArpuCustomers}) may need lower priority threshold")
      println(f"• High-ARPU customers (${highArpuCustomers}) are being prioritized")
    }
  }
  
  private def decodeScheduleFromSolution(solution: DoubleSolution, problem: CampaignSchedulingProblem): CampaignSchedule = {
    problem.decodeSchedule(solution)
  }
  
  private def printSolutionStatistics(solutions: List[DoubleSolution]): Unit = {
    val responseRates = solutions.map(-_.getObjective(0))
    val arpuWeightedValues = solutions.map(-_.getObjective(1))
    val costs = solutions.map(sol => Option(sol.getAttribute("TotalCost")).map(_.toString.toDouble).getOrElse(0.0))
    
    println(f"\n=== SOLUTION STATISTICS ===")
    println(f"Response Rate Range: ${responseRates.min}%.2f - ${responseRates.max}%.2f")
    println(f"ARPU-Weighted Value Range: $$${arpuWeightedValues.min}%.2f - $$${arpuWeightedValues.max}%.2f")
    if (costs.nonEmpty && costs.max > 0) {
      println(f"Cost Range: $$${costs.min}%.2f - $$${costs.max}%.2f")
    }
    
    // Calculate trade-offs
    val avgResponseRate = responseRates.sum / responseRates.length
    val avgArpuWeightedValue = arpuWeightedValues.sum / arpuWeightedValues.length
    val avgCost = if (costs.nonEmpty) costs.sum / costs.length else 0.0
    
    println(f"\n=== AVERAGE VALUES ===")
    println(f"Average Response Rate: ${avgResponseRate}%.2f")
    println(f"Average ARPU-Weighted Value: $$${avgArpuWeightedValue}%.2f")
    if (avgCost > 0) {
      println(f"Average Cost: $$${avgCost}%.2f")
    }
    
    // Business efficiency metrics
    if (avgCost > 0 && avgResponseRate > 0) {
      println(f"Cost per Response: $$${avgCost/avgResponseRate}%.2f")
      println(f"Value per Response: $$${avgArpuWeightedValue/avgResponseRate}%.2f")
      println(f"ROI (Value/Cost): ${avgArpuWeightedValue/avgCost}%.2fx")
    }
    
    // Pareto front analysis
    println(f"\n=== PARETO FRONT ANALYSIS ===")
    println(f"Solutions span ${solutions.length} different trade-offs")
    val responseDiversity = (responseRates.max - responseRates.min) / responseRates.max * 100
    val valueDiversity = (arpuWeightedValues.max - arpuWeightedValues.min) / arpuWeightedValues.max * 100
    println(f"Response rate diversity: ${responseDiversity}%.1f%%")
    println(f"ARPU-weighted value diversity: ${valueDiversity}%.1f%%")
  }
  
  private def saveResults(
    solutions: java.util.List[DoubleSolution], 
    schedules: List[CampaignSchedule],
    problem: CampaignSchedulingProblem,
    spark: SparkSession
  ): Unit = {

    val hdfsDirPath = s"hdfs://scluster/user/g1110566/campaign_optimization"
    
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    
    // Save Pareto front (traditional format)
    val output = new SolutionListOutput(solutions)
      .setSeparator("\t")
      .setVarFileOutputContext(new DefaultFileOutputContext(s"VAR_campaign_${timestamp}.tsv"))
      .setFunFileOutputContext(new DefaultFileOutputContext(s"FUN_campaign_${timestamp}.tsv"))
    
    output.print()
    
    // Save detailed schedules for best solutions
    if (!solutions.isEmpty) {
      val solutionsList = solutions.asScala.toList
      
      // Get best solution for each objective
      val bestResponseSolution = solutionsList.minBy(_.getObjective(0))
      val bestValueSolution = solutionsList.minBy(_.getObjective(1))
      
      // Save schedules in appropriate format (Parquet on HDFS or CSV locally)
      saveScheduleData(bestResponseSolution, problem, spark, hdfsDirPath, s"best_response_${timestamp}")
      saveScheduleData(bestValueSolution, problem, spark, hdfsDirPath, s"best_arpu_value_${timestamp}")
      
      // Also save all solutions summary
      saveAllSolutionsSummary(solutionsList, problem, spark, hdfsDirPath, timestamp)
    }
    
    println(s"Results saved with timestamp: $timestamp")
  }
  
  private def saveScheduleData(
    solution: DoubleSolution,
    problem: CampaignSchedulingProblem,
    spark: SparkSession,
    hdfsDirPath: String,
    filename: String
  ): Unit = {
    
    val schedule = problem.decodeSchedule(solution)
    
    if (isHadoopAvailable(spark, hdfsDirPath)) {
      saveScheduleAsParquet(schedule, solution, spark, hdfsDirPath, filename)
    } else {
      saveScheduleAsCSV(schedule, s"${filename}.csv")
    }
  }
  
  private def saveAllSolutionsSummary(
    solutions: List[DoubleSolution],
    problem: CampaignSchedulingProblem,
    spark: SparkSession,
    hdfsDirPath: String,
    timestamp: String
  ): Unit = {
    
    import spark.implicits._
    
    // Create summary data for all solutions
    val summaryData = solutions.zipWithIndex.map { case (solution, index) =>
      val schedule = problem.decodeSchedule(solution)
      
      SolutionSummary(
        solutionId = index,
        timestamp = timestamp,
        expectedResponses = -solution.getObjective(0), // Un-negate
        totalCost = schedule.totalCost,
        customerValue = -solution.getObjective(1), // Un-negate ARPU-weighted value
        totalAssignments = schedule.assignments.length,
        maxHourlyLoad = schedule.hourlyLoads.max,
        utilizationRate = schedule.assignments.length.toDouble / problem.customers.length,
        emailAssignments = schedule.assignments.count(_.channel == 0),
        smsAssignments = schedule.assignments.count(_.channel == 1),
        pushAssignments = schedule.assignments.count(_.channel == 2),
        inAppAssignments = schedule.assignments.count(_.channel == 3),
        avgResponseRate = if (schedule.assignments.nonEmpty) schedule.assignments.map(_.expectedResponseRate).sum / schedule.assignments.length else 0.0,
        costPerResponse = if (schedule.totalCost > 0 && schedule.totalExpectedResponses > 0) schedule.totalCost / schedule.totalExpectedResponses else 0.0
      )
    }
    
    if (isHadoopAvailable(spark, hdfsDirPath)) {
      val df = summaryData.toList.toDF()
      val hdfsPath = s"${hdfsDirPath}/solutions_summary_${timestamp}"
      
      try {
        df.write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(hdfsPath)
        
        println(s"Solutions summary saved to HDFS: $hdfsPath")
      } catch {
        case e: Exception =>
          println(s"Failed to save to HDFS, falling back to local: ${e.getMessage}")
          saveSummaryAsCSV(summaryData, s"solutions_summary_${timestamp}.csv")
      }
    } else {
      saveSummaryAsCSV(summaryData, s"solutions_summary_${timestamp}.csv")
    }
  }
  
  private def saveScheduleAsParquet(
    schedule: CampaignSchedule,
    solution: DoubleSolution,
    spark: SparkSession,
    hdfsDirPath: String,
    filename: String
  ): Unit = {
    
    import spark.implicits._
    
    // Convert schedule to DataFrame-friendly format
    val scheduleData = schedule.assignments.map { assignment =>
      ScheduleRecord(
        customerId = assignment.customerId,
        timeSlot = assignment.timeSlot,
        channel = assignment.channel,
        channelName = assignment.channel match {
          case 0 => "Email"
          case 1 => "SMS"
          case 2 => "Push"
          case 3 => "In-app"
          case _ => s"Channel${assignment.channel}"
        },
        expectedResponseRate = assignment.expectedResponseRate,
        cost = assignment.cost,
        priority = assignment.priority,
        solutionMetrics = SolutionMetrics(
          expectedResponses = -solution.getObjective(0),
          totalCost = schedule.totalCost,
          customerValue = -solution.getObjective(1) // ARPU-weighted value
        )
      )
    }
    
    val hdfsPath = s"${hdfsDirPath}/schedule_${filename}"
    
    try {
      val df = scheduleData.toSeq.toDF()
      
      df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("channel")  // Partition by channel for efficient querying
        .parquet(hdfsPath)
      
      println(s"Schedule saved to HDFS as Parquet: $hdfsPath")
      
      // Show some statistics
      println(s"  Records: ${scheduleData.length}")
      println(s"  Partitions: ${df.rdd.getNumPartitions}")
      
    } catch {
      case e: Exception =>
        println(s"Failed to save to HDFS, falling back to local CSV: ${e.getMessage}")
        saveScheduleAsCSV(schedule, s"${filename}.csv")
    }
  }
  
  private def saveScheduleAsCSV(schedule: CampaignSchedule, filename: String): Unit = {
    import java.io.PrintWriter
    import java.io.File
    
    val writer = new PrintWriter(new File(filename))
    try {
      // Write CSV header
      writer.println("CustomerID,TimeSlot,Channel,ChannelName,ExpectedResponseRate,Cost,Priority")
      
      // Write assignments sorted by customer ID
      schedule.assignments.sortBy(_.customerId).foreach { assignment =>
        val channelName = assignment.channel match {
          case 0 => "Email"
          case 1 => "SMS"
          case 2 => "Push"
          case 3 => "In-app"
          case _ => s"Channel${assignment.channel}"
        }
        
        writer.println(s"${assignment.customerId},${assignment.timeSlot},${assignment.channel},${channelName},${assignment.expectedResponseRate},${assignment.cost},${assignment.priority}")
      }
      
      println(s"Schedule saved locally as CSV: $filename")
    } finally {
      writer.close()
    }
  }
  
  private def saveSummaryAsCSV(summaryData: List[SolutionSummary], filename: String): Unit = {
    import java.io.PrintWriter
    import java.io.File
    
    val writer = new PrintWriter(new File(filename))
    try {
      // Write CSV header
      writer.println("SolutionId,Timestamp,ExpectedResponses,TotalCost,CustomerValue,TotalAssignments,MaxHourlyLoad,UtilizationRate,EmailAssignments,SmsAssignments,PushAssignments,InAppAssignments,AvgResponseRate,CostPerResponse")
      
      // Write data
      summaryData.foreach { summary =>
        writer.println(s"${summary.solutionId},${summary.timestamp},${summary.expectedResponses},${summary.totalCost},${summary.customerValue},${summary.totalAssignments},${summary.maxHourlyLoad},${summary.utilizationRate},${summary.emailAssignments},${summary.smsAssignments},${summary.pushAssignments},${summary.inAppAssignments},${summary.avgResponseRate},${summary.costPerResponse}")
      }
      
      println(s"Solutions summary saved locally as CSV: $filename")
    } finally {
      writer.close()
    }
  }
  
  private def isHadoopAvailable(spark: SparkSession, hdfsDirPath: String): Boolean = {
    try {
      // Check if Hadoop configuration is available
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      
      // Check if HDFS is configured
      val defaultFS = hadoopConf.get("fs.defaultFS", "")
      val isHDFSConfigured = defaultFS.startsWith("hdfs://")
      
      if (!isHDFSConfigured) {
        println("HDFS not configured (fs.defaultFS not set to hdfs://)")
        return false
      }
      
      // Try to access HDFS
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val testPath = new org.apache.hadoop.fs.Path("/")
      val exists = fs.exists(testPath)
      
      if (exists) {
        println(s"HDFS available at: $defaultFS")
        
        // Try to create campaign optimization directory if it doesn't exist
        val campaignDir = new org.apache.hadoop.fs.Path(hdfsDirPath)
        if (!fs.exists(campaignDir)) {
          fs.mkdirs(campaignDir)
          println("Created /campaign_optimization directory on HDFS")
        }
        
        true
      } else {
        println("HDFS root directory not accessible")
        false
      }
      
    } catch {
      case e: Exception =>
        println(s"Hadoop/HDFS not available: ${e.getMessage}")
        false
    }
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
        println(f"  Customer value: $$${metrics.customerValue}%.2f")
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
   * Zeppelin notebook helper method that uses existing SparkSession
   */
  def optimizeForZeppelin(
    spark: SparkSession,
    numCustomers: Int = 1000,
    populationSize: Int = 50,
    maxEvaluations: Int = 2000
  ): OptimizationResults = {
    
    val config = OptimizationConfig(
      numCustomersDemo = numCustomers,
      populationSize = populationSize,
      maxEvaluations = maxEvaluations
    )
    
    optimizeWithSpark(spark, config)
  }
  

}

/**
 * Data structure for saving schedule records to Parquet
 */
case class ScheduleRecord(
  customerId: Long,
  timeSlot: Int,
  channel: Int,
  channelName: String,
  expectedResponseRate: Double,
  cost: Double,
  priority: Double,
  solutionMetrics: SolutionMetrics
)

/**
 * Metrics associated with a solution
 */
case class SolutionMetrics(
  expectedResponses: Double,
  totalCost: Double,
  customerValue: Double
)

/**
 * Summary data for all solutions
 */
case class SolutionSummary(
  solutionId: Int,
  timestamp: String,
  expectedResponses: Double,
  totalCost: Double,
  customerValue: Double,
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