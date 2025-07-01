error id: file://<WORKSPACE>/spark_example/src/main/scala/org/uma/jmetalsp/spark/examples/campaign/CampaignSchedulingOptimizer.scala:local161
file://<WORKSPACE>/spark_example/src/main/scala/org/uma/jmetalsp/spark/examples/campaign/CampaignSchedulingOptimizer.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb

found definition using fallback; symbol DoubleSolution
offset: 26162
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
    
    Try {
      // Create Spark session for standalone usage only
      val spark = optimizer.createSparkSession(optimizer.OptimizationConfig())
      try {
        val results = optimizer.optimizeWithSpark(spark)
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
    sparkMaster: Option[String] = None, // Auto-detect if None
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
  
  /**
   * Automatically detect if YARN is available
   */
  private def detectSparkMaster(): String = {
    // Method 1: Check if YARN ResourceManager is configured (don't rely on localhost connectivity)
    def isYarnConfigured: Boolean = {
      // Check if yarn.resourcemanager.hostname is configured
      val yarnRmHostname = sys.props.get("yarn.resourcemanager.hostname")
        .orElse(sys.env.get("YARN_RESOURCEMANAGER_HOSTNAME"))
      
      // Check if fs.defaultFS points to HDFS (indicates Hadoop cluster)
      val defaultFS = sys.props.get("fs.defaultFS")
        .orElse(sys.env.get("HADOOP_DEFAULT_FS"))
      
      if (yarnRmHostname.isDefined) {
        println(s"  YARN ResourceManager hostname configured: ${yarnRmHostname.get}")
        true
      } else if (defaultFS.exists(_.startsWith("hdfs://"))) {
        println(s"  HDFS configured as default filesystem: ${defaultFS.get}")
        true
      } else {
        // Try to read Hadoop configuration files if available
        val hadoopConfDir = sys.env.get("HADOOP_CONF_DIR")
        val yarnConfDir = sys.env.get("YARN_CONF_DIR")
        
        if (hadoopConfDir.isDefined || yarnConfDir.isDefined) {
          println(s"  Hadoop configuration directories found")
          // In a real cluster, configuration files would be present
          true
        } else {
          println("  No YARN configuration found")
          false
        }
      }
    }
    
    // Method 2: Check environment variables (but be conservative)
    def hasYarnEnvVars: Boolean = {
      val yarnEnvVars = List(
        "YARN_CONF_DIR",
        "HADOOP_CONF_DIR", 
        "HADOOP_HOME"
      )
      
      val foundVars = yarnEnvVars.filter(sys.env.contains)
      if (foundVars.nonEmpty) {
        println(s"  Found YARN environment variables: ${foundVars.mkString(", ")}")
        true
      } else {
        println("  No YARN environment variables found")
        false
      }
    }
    
    // Method 3: Check if running in a known cluster environment
    def isClusterEnvironment: Boolean = {
      val clusterIndicators = List(
        "KUBERNETES_SERVICE_HOST", // Kubernetes
        "MESOS_TASK_ID",           // Mesos
        "SLURM_JOB_ID"             // SLURM
      )
      
      val foundIndicators = clusterIndicators.filter(sys.env.contains)
      if (foundIndicators.nonEmpty) {
        println(s"  Found cluster indicators: ${foundIndicators.mkString(", ")}")
        true
      } else {
        false
      }
    }
    
    // Method 4: Check command line arguments or system properties
    def isYarnFromArgs: Boolean = {
      val sparkMasterProp = sys.props.get("spark.master")
      val sparkMasterEnv = sys.env.get("SPARK_MASTER")
      
      val yarnFromProps = sparkMasterProp.exists(_.contains("yarn"))
      val yarnFromEnv = sparkMasterEnv.exists(_.contains("yarn"))
      
      if (yarnFromProps) println(s"  Spark master from system property: ${sparkMasterProp.get}")
      if (yarnFromEnv) println(s"  Spark master from environment: ${sparkMasterEnv.get}")
      
      yarnFromProps || yarnFromEnv
    }
    
    // Method 5: Check for explicit local mode configuration
    def isLocalFromArgs: Boolean = {
      val sparkMasterProp = sys.props.get("spark.master")
      val sparkMasterEnv = sys.env.get("SPARK_MASTER")
      
      val localFromProps = sparkMasterProp.exists(_.startsWith("local"))
      val localFromEnv = sparkMasterEnv.exists(_.startsWith("local"))
      
      if (localFromProps) println(s"  Local mode from system property: ${sparkMasterProp.get}")
      if (localFromEnv) println(s"  Local mode from environment: ${sparkMasterEnv.get}")
      
      localFromProps || localFromEnv
    }
    
    println("Checking cluster environment...")
    
    val detectedMaster = if (isYarnFromArgs) {
      println("  Using YARN from explicit configuration")
      "yarn"
    } else if (isLocalFromArgs) {
      val sparkMasterProp = sys.props.get("spark.master")
      val sparkMasterEnv = sys.env.get("SPARK_MASTER")
      val explicitMaster = sparkMasterProp.orElse(sparkMasterEnv).getOrElse("local[*]")
      println(s"  Using explicit local configuration: $explicitMaster")
      explicitMaster
    } else if (hasYarnEnvVars || isYarnConfigured) {
      println("  YARN environment detected")
      "yarn"
    } else if (isClusterEnvironment) {
      println("  Cluster environment detected, using YARN")
      "yarn"
    } else {
      println("  No cluster environment detected, using local mode")
      "local[*]"
    }
    
    println(s"Auto-detected Spark master: $detectedMaster")
    if (detectedMaster == "yarn") {
      println("  YARN cluster environment detected")
    } else {
      println("  Local environment detected - using all available cores")
    }
    
    detectedMaster
  }
  
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
    saveResults(solutions, schedules, problem, spark)
    
    OptimizationResults(
      bestSolutions = solutions.asScala.toList,
      schedules = schedules,
      metrics = metrics,
      executionTime = executionTime,
      problemStats = problemStats,
      customerStats = customerStats
    )
  }
  
  /**
   * Legacy optimization method that creates its own Spark session
   * Only use this for standalone applications
   */
  def optimize(config: OptimizationConfig = OptimizationConfig()): OptimizationResults = {
    
    println("=== Campaign Message Scheduling Optimization ===")
    
    // Auto-detect Spark master if not specified
    val finalConfig = config.sparkMaster match {
      case Some(master) => 
        println(s"Using specified Spark master: $master")
        config
      case None => 
        println("Auto-detecting Spark master...")
        val detectedMaster = detectSparkMaster()
        config.copy(sparkMaster = Some(detectedMaster))
    }
    
    // Initialize Spark
    val spark = createSparkSession(finalConfig)
    
    try {
      optimizeWithSpark(spark, finalConfig)
    } finally {
      spark.stop()
      println("\nSpark session stopped.")
    }
  }
  
  private def createSparkSession(config: OptimizationConfig): SparkSession = {
    val masterUrl = config.sparkMaster.getOrElse("local[*]")
    
    val spark: SparkSession = {
      val builder = SparkSession.builder()
        .appName("Campaign Scheduling Optimization with jMetalSP")
        .master(masterUrl)
      
      // Try to enable Hive support if available, otherwise continue without it
      val finalBuilder = Try {
        builder
          .config("spark.hadoop.metastore.catalog.default","hive")
          .enableHiveSupport()
      } match {
        case Success(hiveBuilder) =>
          println("  Hive support enabled")
          hiveBuilder
        case Failure(exception) =>
          println(s"  Hive support not available: ${exception.getMessage}")
          println("  Continuing without Hive support...")
          builder
      }
      
      finalBuilder.getOrCreate()
    }

    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
    
    if (config.enableCheckpointing) {
      val checkpointDir = if (masterUrl.contains("yarn")) {
        "/tmp/spark-checkpoint-yarn"
      } else {
        "/tmp/spark-checkpoint-local"
      }
      spark.sparkContext.setCheckpointDir(checkpointDir)
    }
    
    println("Spark session initialized:")
    println(s"  Spark version: ${spark.version}")
    println(s"  Master: ${spark.sparkContext.master}")
    println(s"  Default parallelism: ${spark.sparkContext.defaultParallelism}")
    println(s"  Deploy mode: ${spark.sparkContext.deployMode}")
    
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
    
    if (solutions.isEmpty) {
      println("No solutions found!")
      return (List.empty, List.empty)
    }
    
    println(s"Analyzing ${solutions.size()} solutions...")
    
    // Analyze best solution for each objective
    val solutionsList = solutions.asScala.toList
    
    // Find solution with best response rate (lowest value since we negate it)
    val bestResponseSolution = solutionsList.minBy(_.getObjective(0))
    
    // Find solution with lowest cost
    val lowestCostSolution = solutionsList.minBy(_.getObjective(1))
    
    // Find solution with best satisfaction (lowest value since we negate it)
    val bestSatisfactionSolution = solutionsList.minBy(_.getObjective(2))
    
    println("\n=== BEST SOLUTIONS ANALYSIS ===")
    println("Best Response Rate Solution:")
    printSolutionDetails(bestResponseSolution)
    printScheduleDetails(bestResponseSolution, problem, "BEST RESPONSE RATE")
    
    println("\nLowest Cost Solution:")
    printSolutionDetails(lowestCostSolution)
    printScheduleDetails(lowestCostSolution, problem, "LOWEST COST")
    
    println("\nBest Satisfaction Solution:")
    printSolutionDetails(bestSatisfactionSolution)
    printScheduleDetails(bestSatisfactionSolution, problem, "BEST SATISFACTION")
    
    // Print summary statistics
    printSolutionStatistics(solutionsList)
    
    // Return empty lists for now since we're focusing on the analysis output
    (List.empty, List.empty)
  }
  
  private def printSolutionDetails(solution: DoubleSolution): Unit = {
    val responseRate = -solution.getObjective(0) // Un-negate
    val cost = solution.getObjective(1)
    val satisfaction = -solution.getObjective(2) // Un-negate
    
    println(f"  Expected responses: ${responseRate}%.2f")
    println(f"  Total cost: $$${cost}%.2f")
    println(f"  Customer satisfaction: ${satisfaction}%.3f")
    
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
    
    println(f"Schedule contains ${schedule.assignments.length} customer assignments:")
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
    println("Hour\tAssignments")
    println("-" * 20)
    schedule.hourlyLoads.take(24).zipWithIndex.foreach { case (load, hour) =>
      if (load > 0) {
        println(f"${hour}%4d\t${load}%11d")
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
  }
  
  private def decodeScheduleFromSolution(solution: DoubleSolution, problem: CampaignSchedulingProblem): CampaignSchedule = {
    problem.decodeSchedule(solution)
  }
  
  private def printSolutionStatistics(solutions: List[DoubleSolution]): Unit = {
    val responseRates = solutions.map(-_.getObjective(0))
    val costs = solutions.map(_.getObjective(1))
    val satisfactions = solutions.map(-_.getObjective(2))
    
    println(f"\n=== SOLUTION STATISTICS ===")
    println(f"Response Rate Range: ${responseRates.min}%.2f - ${responseRates.max}%.2f")
    println(f"Cost Range: $$${costs.min}%.2f - $$${costs.max}%.2f")
    println(f"Satisfaction Range: ${satisfactions.min}%.3f - ${satisfactions.max}%.3f")
    
    // Calculate trade-offs
    val avgResponseRate = responseRates.sum / responseRates.length
    val avgCost = costs.sum / costs.length
    val avgSatisfaction = satisfactions.sum / satisfactions.length
    
    println(f"\n=== AVERAGE VALUES ===")
    println(f"Average Response Rate: ${avgResponseRate}%.2f")
    println(f"Average Cost: $$${avgCost}%.2f")
    println(f"Average Satisfaction: ${avgSatisfaction}%.3f")
    
    // Cost efficiency metrics
    if (avgCost > 0) {
      println(f"Cost per Response: $$${avgCost/Math.max(avgResponseRate, 0.01)}%.2f")
    }
  }
  
  private def saveResults(
    solutions: java.util.List[DoubleSolution], 
    schedules: List[CampaignSchedule],
    problem: CampaignSchedulingProblem,
    spark: SparkSession
  ): Unit = {
    
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
      val lowestCostSolution = solutionsList.minBy(_.getObjective(1))
      val bestSatisfactionSolution = solutionsList.minBy(_.getObjective(2))
      
      // Save schedules in appropriate format (Parquet on HDFS or CSV locally)
      saveScheduleData(bestResponseSolution, problem, spark, s"best_response_${timestamp}")
      saveScheduleData(lowestCostSolution, problem, spark, s"lowest_cost_${timestamp}")
      saveScheduleData(bestSatisfactionSolution, problem, spark, s"best_satisfaction_${timestamp}")
      
      // Also save all solutions summary
      saveAllSolutionsSummary(solutionsList, problem, spark, timestamp)
    }
    
    println(s"Results saved with timestamp: $timestamp")
  }
  
  private def saveScheduleData(
    solution: DoubleSolution,
    problem: CampaignSchedulingProblem,
    spark: SparkSession,
    filename: String
  ): Unit = {
    
    val schedule = problem.decodeSchedule(solution)
    
    if (isHadoopAvailable(spark)) {
      saveScheduleAsParquet(schedule, solution, spark, filename)
    } else {
      saveScheduleAsCSV(schedule, s"${filename}.csv")
    }
  }
  
  private def saveAllSolutionsSummary(
    solutions: List[DoubleSolution],
    problem: CampaignSchedulingProblem,
    spark: SparkSession,
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
        totalCost = solution.getObjective(1),
        customerSatisfaction = -solution.getObjective(2), // Un-negate
        totalAssignments = schedule.assignments.length,
        maxHourlyLoad = schedule.hourlyLoads.max,
        utilizationRate = schedule.assignments.length.toDouble / problem.customers.length,
        emailAssignments = schedule.assignments.count(_.channel == 0),
        smsAssignments = schedule.assignments.count(_.channel == 1),
        pushAssignments = schedule.assignments.count(_.channel == 2),
        inAppAssignments = schedule.assignments.count(_.channel == 3),
        avgResponseRate = if (schedule.assignments.nonEmpty) schedule.assignments.map(_.expectedResponseRate).sum / schedule.assignments.length else 0.0,
        costPerResponse = if (schedule.assignments.nonEmpty) solution.getObjective(1) / (-solution.getObjective(0)) else 0.0
      )
    }
    
    if (isHadoopAvailable(spark)) {
      val df = summaryData.toList.toDF()
      val hdfsPath = s"hdfs://scluster/user/g1110566/campaign_optimization/solutions_summary_${timestamp}"
      
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
    solution: DoubleSoluti@@on,
    spark: SparkSession,
    hdfsPath: String,
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
          totalCost = solution.getObjective(1),
          customerSatisfaction = -solution.getObjective(2)
        )
      )
    }
    
    // val hdfsPath = s"hdfs://scluster/user/g1110566/campaign_optimization/schedule_${filename}"
    
    try {
      val df = scheduleData.toList.toDF()
      
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
      writer.println("SolutionId,Timestamp,ExpectedResponses,TotalCost,CustomerSatisfaction,TotalAssignments,MaxHourlyLoad,UtilizationRate,EmailAssignments,SmsAssignments,PushAssignments,InAppAssignments,AvgResponseRate,CostPerResponse")
      
      // Write data
      summaryData.foreach { summary =>
        writer.println(s"${summary.solutionId},${summary.timestamp},${summary.expectedResponses},${summary.totalCost},${summary.customerSatisfaction},${summary.totalAssignments},${summary.maxHourlyLoad},${summary.utilizationRate},${summary.emailAssignments},${summary.smsAssignments},${summary.pushAssignments},${summary.inAppAssignments},${summary.avgResponseRate},${summary.costPerResponse}")
      }
      
      println(s"Solutions summary saved locally as CSV: $filename")
    } finally {
      writer.close()
    }
  }
  
  private def isHadoopAvailable(spark: SparkSession): Boolean = {
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
        val campaignDir = new org.apache.hadoop.fs.Path("campaign_optimization")
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
  
  /**
   * Helper method to manually specify Spark master (for testing)
   * Creates its own Spark session
   */
  def optimizeWithMaster(
    sparkMaster: String,
    numCustomers: Int = 1000,
    populationSize: Int = 50,
    maxEvaluations: Int = 2000
  ): OptimizationResults = {
    
    val config = OptimizationConfig(
      numCustomersDemo = numCustomers,
      populationSize = populationSize,
      maxEvaluations = maxEvaluations,
      sparkMaster = Some(sparkMaster)
    )
    
    optimize(config)
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
  customerSatisfaction: Double
)

/**
 * Summary data for all solutions
 */
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


#### Short summary: 

empty definition using pc, found symbol in pc: 