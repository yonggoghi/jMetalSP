package org.uma.jmetalsp.spark.examples.campaign

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.{AlgorithmRunner, JMetalLogger}
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.net.{Socket, ConnectException}
import java.io.IOException

/**
 * Simplified Campaign Scheduling Optimizer for easier testing and demonstration
 * 
 * This version uses:
 * - Smaller problem sizes
 * - Sequential evaluation instead of Spark for initial testing
 * - Reduced memory footprint
 * - Simple constraint handling
 */
object SimpleCampaignOptimizer {
  
  def main(args: Array[String]): Unit = {
    println("=== Simple Campaign Scheduling Optimizer ===")
    
    val useSparkMode = args.contains("--spark") || args.contains("-s")
    
    Try {
      if (useSparkMode) {
        println("Running with Spark support...")
        runSparkOptimization()
      } else {
        println("Running without Spark (pure jMetal)...")
        runSimpleOptimization()
      }
    } match {
      case Success(_) =>
        println("=== SIMPLE OPTIMIZATION COMPLETED SUCCESSFULLY ===")
        
      case Failure(exception) =>
        println(s"=== SIMPLE OPTIMIZATION FAILED ===")
        println(s"Error: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }
  
  def runSparkOptimization(): Unit = {
    
    // Auto-detect Spark master
    println("Auto-detecting Spark master...")
    val detectedMaster = detectSparkMaster()
    
    // Create single Spark session for the entire optimization
    val spark: SparkSession = {
      val builder = SparkSession.builder()
        .master(detectedMaster)
        .appName("Simple Campaign Scheduling Optimizer")
        // Add Java compatibility configurations
        .config("spark.sql.adaptive.enabled", "false") // Disable adaptive query execution for compatibility
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") // Set warehouse directory
      
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
      
      Try {
        finalBuilder.getOrCreate()
      } match {
        case Success(sparkSession) =>
          println("  Spark session created successfully")
          sparkSession
        case Failure(exception) =>
          println(s"  Failed to create Spark session: ${exception.getMessage}")
          println("  This might be a Java version compatibility issue.")
          println("  Try running with Java 8 or 11 for better Spark 3.1.3 compatibility.")
          throw exception
      }
    }

    val sc = spark.sparkContext
    
    try {
      // Simple configuration for testing
      val numCustomers = 100      // Much smaller for testing
      val populationSize = 20
      val maxEvaluations = 500
      val maxCustomersPerHour = 50
      val campaignBudget = 5000.0
      
      println(s"Configuration:")
      println(s"  Customers: $numCustomers")
      println(s"  Population size: $populationSize")
      println(s"  Max evaluations: $maxEvaluations")
      println(s"  Max customers/hour: $maxCustomersPerHour")
      println(s"  Campaign budget: $$${campaignBudget}")
      println("=" * 50)
    
      // Step 1: Generate customer data
      println("Step 1: Generating customer data...")
      val customers = Customer.generateRandomCustomers(numCustomers, seed = 42L)
      val customerStats = Customer.getStatistics(customers)
      println(customerStats)
      
      // Step 2: Create optimization problem
      println("\nStep 2: Creating optimization problem...")
      val problem = new CampaignSchedulingProblem(
        customers = customers,
        maxCustomersPerHour = maxCustomersPerHour,
        campaignBudget = campaignBudget
      )
      val problemStats = problem.getStatistics
      println(problemStats)
      
      // Step 3: Configure NSGA-II algorithm (with Spark for evaluation)
      println("\nStep 3: Configuring NSGA-II algorithm with Spark...")
      val mutationProbability = 1.0 / problem.getNumberOfVariables
      
      val crossover = new SBXCrossover(0.9, 20.0)
      val mutation = new PolynomialMutation(mutationProbability, 20.0)
      val selection = new BinaryTournamentSelection[DoubleSolution](
        new RankingAndCrowdingDistanceComparator[DoubleSolution]()
      )
      
      // Use Spark evaluator
      import org.uma.jmetalsp.spark.evaluator.SparkSolutionListEvaluator
      val evaluator = new SparkSolutionListEvaluator[DoubleSolution](sc)
      
      // Build NSGA-II algorithm with Spark evaluator
      val algorithm = new NSGAIIBuilder[DoubleSolution](problem, crossover, mutation, populationSize)
        .setSelectionOperator(selection)
        .setMaxEvaluations(maxEvaluations)
        .setSolutionListEvaluator(evaluator)
        .build()
      
      // Step 4: Run optimization
      println("\nStep 4: Running optimization with Spark...")
      println(s"This may take a few minutes...")
      
      val algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute()
      val solutions = algorithm.getResult
      val executionTime = algorithmRunner.getComputingTime
      
      println(s"\nOptimization completed in ${executionTime}ms")
      println(s"Found ${solutions.size()} solutions on the Pareto front")
      
      // Step 5: Analyze results
      println("\nStep 5: Analyzing results...")
      analyzeSimpleResults(solutions, customers, problem)
      
      // Step 6: Save results
      println("\nStep 6: Saving results...")
      saveSparkResults(solutions, problem, spark)
      
      println("\n=== SPARK OPTIMIZATION SUMMARY ===")
      printSimpleSummary(solutions, executionTime, customerStats, problemStats)
      
    } finally {
      spark.stop()
    }
  }
  
  private def saveSparkResults(solutions: java.util.List[DoubleSolution], problem: CampaignSchedulingProblem, spark: SparkSession): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    
    // Save Pareto front
    val output = new SolutionListOutput(solutions)
      .setSeparator("\t")
      .setVarFileOutputContext(new DefaultFileOutputContext(s"VAR_spark_campaign_${timestamp}.tsv"))
      .setFunFileOutputContext(new DefaultFileOutputContext(s"FUN_spark_campaign_${timestamp}.tsv"))
    
    output.print()
    
    // Save detailed schedule for the best response rate solution
    if (!solutions.isEmpty) {
      val bestSolution = solutions.asScala.minBy(_.getObjective(0)) // Best response rate
      val schedule = problem.decodeSchedule(bestSolution)
      
      // Try to save as Parquet if Hadoop is available, otherwise CSV
      if (isHadoopAvailableSimple(spark)) {
        saveScheduleAsParquetSimple(schedule, bestSolution, s"spark_campaign_${timestamp}", spark)
      } else {
        saveScheduleToCSV(schedule, s"SCHEDULE_spark_campaign_${timestamp}.csv")
      }
    }
    
    println(s"Results saved:")
    println(s"  Variables: VAR_spark_campaign_${timestamp}.tsv")
    println(s"  Objectives: FUN_spark_campaign_${timestamp}.tsv")
  }
  
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
  
  def runSimpleOptimization(): Unit = {
    
    println("Running simple optimization without Spark overhead...")
    
    try {
      // Simple configuration for testing
      val numCustomers = 100      // Much smaller for testing
      val populationSize = 20
      val maxEvaluations = 500
      val maxCustomersPerHour = 50
      val campaignBudget = 5000.0
      
      println(s"Configuration:")
      println(s"  Customers: $numCustomers")
      println(s"  Population size: $populationSize")
      println(s"  Max evaluations: $maxEvaluations")
      println(s"  Max customers/hour: $maxCustomersPerHour")
      println(s"  Campaign budget: $$${campaignBudget}")
      println("=" * 50)
    
    // Step 1: Generate customer data
    println("Step 1: Generating customer data...")
    val customers = Customer.generateRandomCustomers(numCustomers, seed = 42L)
    val customerStats = Customer.getStatistics(customers)
    println(customerStats)
    
    // Step 2: Create optimization problem
    println("\nStep 2: Creating optimization problem...")
    val problem = new CampaignSchedulingProblem(
      customers = customers,
      maxCustomersPerHour = maxCustomersPerHour,
      campaignBudget = campaignBudget
    )
    val problemStats = problem.getStatistics
    println(problemStats)
    
    // Step 3: Configure NSGA-II algorithm (without Spark for simplicity)
    println("\nStep 3: Configuring NSGA-II algorithm...")
    val mutationProbability = 1.0 / problem.getNumberOfVariables
    
    val crossover = new SBXCrossover(0.9, 20.0)
    val mutation = new PolynomialMutation(mutationProbability, 20.0)
    val selection = new BinaryTournamentSelection[DoubleSolution](
      new RankingAndCrowdingDistanceComparator[DoubleSolution]()
    )
    
    // Build NSGA-II algorithm without Spark evaluator
    val algorithm = new NSGAIIBuilder[DoubleSolution](problem, crossover, mutation, populationSize)
      .setSelectionOperator(selection)
      .setMaxEvaluations(maxEvaluations)
      .build()
    
    // Step 4: Run optimization
    println("\nStep 4: Running optimization...")
    println(s"This may take a few minutes...")
    
    val algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute()
    val solutions = algorithm.getResult
    val executionTime = algorithmRunner.getComputingTime
    
    println(s"\nOptimization completed in ${executionTime}ms")
    println(s"Found ${solutions.size()} solutions on the Pareto front")
    
    // Step 5: Analyze results
    println("\nStep 5: Analyzing results...")
    analyzeSimpleResults(solutions, customers, problem)
    
    // Step 6: Save results
    println("\nStep 6: Saving results...")
    saveSimpleResults(solutions, problem)
    
    println("\n=== SIMPLE OPTIMIZATION SUMMARY ===")
    printSimpleSummary(solutions, executionTime, customerStats, problemStats)
    
    } catch {
      case e: Exception =>
        println(s"Error during optimization: ${e.getMessage}")
        throw e
    }
  }
  
  private def analyzeSimpleResults(
    solutions: java.util.List[DoubleSolution], 
    customers: Array[Customer],
    problem: CampaignSchedulingProblem
  ): Unit = {
    
    if (solutions.isEmpty) {
      println("No solutions found!")
      return
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
    printSolutionDetails(bestResponseSolution, "Best Response")
    printScheduleDetails(bestResponseSolution, problem, "BEST RESPONSE RATE")
    
    println("\nLowest Cost Solution:")
    printSolutionDetails(lowestCostSolution, "Lowest Cost")
    printScheduleDetails(lowestCostSolution, problem, "LOWEST COST")
    
    println("\nBest Satisfaction Solution:")
    printSolutionDetails(bestSatisfactionSolution, "Best Satisfaction")
    printScheduleDetails(bestSatisfactionSolution, problem, "BEST SATISFACTION")
  }
  
  private def printSolutionDetails(solution: DoubleSolution, label: String): Unit = {
    val responseRate = -solution.getObjective(0) // Un-negate
    val cost = solution.getObjective(1)
    val satisfaction = -solution.getObjective(2) // Un-negate
    
    println(f"  Response Rate: ${responseRate}%.2f")
    println(f"  Cost: $$${cost}%.2f")
    println(f"  Satisfaction: ${satisfaction}%.3f")
    
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
    
    // Show first 15 assignments as sample (smaller for simple optimizer)
    sortedAssignments.take(15).foreach { assignment =>
      val channelName = assignment.channel match {
        case 0 => "Email"
        case 1 => "SMS"
        case 2 => "Push"
        case 3 => "In-app"
        case _ => s"Ch${assignment.channel}"
      }
      
      println(f"${assignment.customerId}%10d\t${assignment.timeSlot}%8d\t${channelName}%7s\t${assignment.expectedResponseRate}%15.3f\t$$${assignment.cost}%4.2f\t${assignment.priority}%8.3f")
    }
    
    if (sortedAssignments.length > 15) {
      println(f"... and ${sortedAssignments.length - 15} more assignments")
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
  
  private def saveSimpleResults(solutions: java.util.List[DoubleSolution], problem: CampaignSchedulingProblem): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    
    // Save Pareto front
    val output = new SolutionListOutput(solutions)
      .setSeparator("\t")
      .setVarFileOutputContext(new DefaultFileOutputContext(s"VAR_simple_campaign_${timestamp}.tsv"))
      .setFunFileOutputContext(new DefaultFileOutputContext(s"FUN_simple_campaign_${timestamp}.tsv"))
    
    output.print()
    
    // Save detailed schedule for the best response rate solution
    if (!solutions.isEmpty) {
      val bestSolution = solutions.asScala.minBy(_.getObjective(0)) // Best response rate
      val schedule = problem.decodeSchedule(bestSolution)
      
      // Save as CSV (simple version doesn't use Spark/Parquet)
      saveScheduleToCSV(schedule, s"SCHEDULE_simple_campaign_${timestamp}.csv")
    }
    
    println(s"Results saved:")
    println(s"  Variables: VAR_simple_campaign_${timestamp}.tsv")
    println(s"  Objectives: FUN_simple_campaign_${timestamp}.tsv")
  }
  
  private def saveScheduleToCSV(schedule: CampaignSchedule, filename: String): Unit = {
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
    } finally {
      writer.close()
    }
  }
  
  private def printSimpleSummary(
    solutions: java.util.List[DoubleSolution],
    executionTime: Long,
    customerStats: CustomerStatistics,
    problemStats: ProblemStatistics
  ): Unit = {
    
    println(s"Execution time: ${executionTime}ms (${executionTime/1000.0}s)")
    println(s"Solutions found: ${solutions.size()}")
    
    if (!solutions.isEmpty) {
      val solutionsList = solutions.asScala
      
      // Calculate statistics across all solutions
      val responseRates = solutionsList.map(-_.getObjective(0))
      val costs = solutionsList.map(_.getObjective(1))
      val satisfactions = solutionsList.map(-_.getObjective(2))
      
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
        println(f"Cost per Response: $$${avgCost/avgResponseRate}%.2f")
      }
    }
    
    println(f"\n${problemStats}")
    println(f"\n${customerStats}")
    
    println("\n=== NEXT STEPS ===")
    println("1. Review the saved TSV files for detailed results")
    println("2. Try running with larger population or more evaluations")
    println("3. Experiment with different customer numbers")
    println("4. Use the full CampaignSchedulingOptimizer for production scale")
  }
  
  private def saveScheduleAsParquetSimple(
    schedule: CampaignSchedule,
    solution: DoubleSolution,
    filename: String,
    spark: SparkSession
  ): Unit = {
    
    try {
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
      
      val hdfsPath = s"hdfs://scluster/user/g1110566/campaign_optimization/simple_schedule_${filename}"
      
      val df = scheduleData.toList.toDF()
      
      df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("channel")
        .parquet(hdfsPath)
      
      println(s"Schedule saved to HDFS as Parquet: $hdfsPath")
      println(s"  Records: ${scheduleData.length}")
      
    } catch {
      case e: Exception =>
        println(s"Failed to save to HDFS, falling back to local CSV: ${e.getMessage}")
        saveScheduleToCSV(schedule, s"SCHEDULE_simple_campaign_${filename}.csv")
    }
  }
  
  private def isHadoopAvailableSimple(spark: SparkSession): Boolean = {
    try {
      // Simple check for Hadoop availability
      val hadoopHome = sys.env.get("HADOOP_HOME")
      val hadoopConfDir = sys.env.get("HADOOP_CONF_DIR")
      val yarnConfDir = sys.env.get("YARN_CONF_DIR")
      
      if (hadoopHome.isEmpty && hadoopConfDir.isEmpty && yarnConfDir.isEmpty) {
        println("Hadoop environment variables not found - saving as local CSV")
        return false
      }
      
      // Use the provided Spark session to test HDFS connectivity
      try {
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        val defaultFS = hadoopConf.get("fs.defaultFS", "")
        
        if (!defaultFS.startsWith("hdfs://")) {
          println("HDFS not configured - saving as local CSV")
          return false
        }
        
        val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        val testPath = new org.apache.hadoop.fs.Path("/")
        val exists = fs.exists(testPath)
        
        if (exists) {
          println(s"HDFS available at: $defaultFS - will save as Parquet")
          true
        } else {
          println("HDFS not accessible - saving as local CSV")
          false
        }
      }
      
    } catch {
      case e: Exception =>
        println(s"Hadoop/HDFS not available: ${e.getMessage} - saving as local CSV")
        false
    }
  }
} 