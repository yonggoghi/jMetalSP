error id: file://<WORKSPACE>/spark_example/CampaignOptimizer.scala:`<none>`.
file://<WORKSPACE>/spark_example/CampaignOptimizer.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1577
uri: file://<WORKSPACE>/spark_example/CampaignOptimizer.scala
text:
```scala
package org.uma.jmetalsp.spark.examples.campaign

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.uma.jmetal.algorithm.multiobjective.nsgaiii.NSGAIIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import org.uma.jmetal.util.referencedirection.impl.UniformReferenceDirectionFactory
import org.uma.jmetalsp.spark.evaluator.SparkSolutionListEvaluator
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Campaign Message Sending Scheduling Optimizer using Spark and NSGA-III
 * 
 * This optimizer handles large-scale multi-objective optimization for
 * campaign scheduling with millions of customers.
 */
object CampaignOptimizer {

  // Configuration case classes
  case class OptimizationConfig(
      populationSize: Int = 100,
      maxEvaluations: Int = 10000,
      crossoverProbability: Double = 0.9,
      crossoverDistributionIndex: Double = 20.0,
      mutationDistributionIndex: Double = 20.0,
      numberOfObjectives: Int = 3, // Response rate, constraints, load balance
      numberOfDivisions: Int = 12 // For NSGA-III r@@eference points
  )

  case class ProblemConfig(
      numberOfCustomers: Int = 100000, // Start smaller for testing, scale up
      numberOfTimeSlots: Int = 60,
      numberOfChannels: Int = 4,
      maxCustomersPerHour: Int = 2000,
      minSendingInterval: Int = 48
  )

  case class SparkConfig(
      appName: String = "Campaign Scheduling Optimizer",
      master: String = "local[*]",
      executorMemory: String = "4g",
      driverMemory: String = "2g",
      maxResultSize: String = "1g"
  )

  def main(args: Array[String]): Unit = {
    println("=== Campaign Message Sending Scheduling Optimizer ===")
    
    val optimizationConfig = OptimizationConfig()
    val problemConfig = ProblemConfig()
    val sparkConfig = SparkConfig()
    
    println(s"Problem size: ${problemConfig.numberOfCustomers} customers")
    println(s"Time slots: ${problemConfig.numberOfTimeSlots}")
    println(s"Channels: ${problemConfig.numberOfChannels}")
    println(s"Population size: ${optimizationConfig.populationSize}")
    println(s"Max evaluations: ${optimizationConfig.maxEvaluations}")
    println("=" * 55)
    
    Try {
      runOptimization(optimizationConfig, problemConfig, sparkConfig)
    } match {
      case Success(_) =>
        println("Optimization completed successfully!")
        
      case Failure(exception) =>
        println(s"Error during optimization: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }

  def runOptimization(
      optimizationConfig: OptimizationConfig,
      problemConfig: ProblemConfig,
      sparkConfig: SparkConfig
  ): Unit = {
    
    // Initialize Spark
    val sparkConf = createSparkConfiguration(sparkConfig)
    val sparkContext = new SparkContext(sparkConf)
    val javaSparkContext = new JavaSparkContext(sparkContext)
    
    try {
      sparkContext.setLogLevel("WARN")
      println("Spark Context initialized successfully")
      println(s"Spark Master: ${sparkContext.master}")
      println(s"Default parallelism: ${sparkContext.defaultParallelism}")
      
      // Create the campaign scheduling problem
      val problem = new CampaignSchedulingProblem(
        numberOfCustomers = problemConfig.numberOfCustomers,
        numberOfTimeSlots = problemConfig.numberOfTimeSlots,
        numberOfChannels = problemConfig.numberOfChannels,
        maxCustomersPerHour = problemConfig.maxCustomersPerHour,
        minSendingInterval = problemConfig.minSendingInterval
      )
      
      println(s"Problem created with ${problem.getNumberOfVariables} variables")
      println(s"Number of objectives: ${problem.getNumberOfObjectives}")
      
      // Create genetic operators
      val mutationProbability = 1.0 / problem.getNumberOfVariables
      val crossover = new SBXCrossover(
        optimizationConfig.crossoverProbability,
        optimizationConfig.crossoverDistributionIndex
      )
      val mutation = new PolynomialMutation(
        mutationProbability,
        optimizationConfig.mutationDistributionIndex
      )
      val selection = new BinaryTournamentSelection[DoubleSolution](
        new RankingAndCrowdingDistanceComparator[DoubleSolution]()
      )
      
      // Create Spark-based solution evaluator
      val evaluator = new SparkSolutionListEvaluator[DoubleSolution](javaSparkContext)
      
      // Configure NSGA-III algorithm with reference directions
      val referenceDirectionFactory = new UniformReferenceDirectionFactory(
        optimizationConfig.numberOfObjectives, 
        optimizationConfig.numberOfDivisions
      )
      val referenceDirections = referenceDirectionFactory.getReferenceDirections()
      
      val algorithm = new NSGAIIIBuilder[DoubleSolution](problem, referenceDirections)
        .setCrossoverOperator(crossover)
        .setMutationOperator(mutation)
        .setSelectionOperator(selection)
        .setPopulationSize(optimizationConfig.populationSize)
        .setMaxIterations(optimizationConfig.maxEvaluations / optimizationConfig.populationSize)
        .setSolutionListEvaluator(evaluator)
        .build()
      
      println("Algorithm configured. Starting optimization...")
      val startTime = System.currentTimeMillis()
      
      // Run the algorithm
      val algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute()
      
      // Get results
      val population = algorithm.getResult
      val computingTime = algorithmRunner.getComputingTime
      val endTime = System.currentTimeMillis()
      
      println(s"Optimization completed!")
      println(s"Total execution time: ${computingTime}ms")
      println(s"Wall clock time: ${endTime - startTime}ms")
      println(s"Solutions found: ${population.size()}")
      
      // Analyze and display results
      analyzeResults(problem, population.asScala.toList)
      
      // Save results
      saveResults(population)
      
    } finally {
      javaSparkContext.close()
      sparkContext.stop()
      println("Spark Context stopped.")
    }
  }

  private def analyzeResults(
      problem: CampaignSchedulingProblem,
      solutions: List[DoubleSolution]
  ): Unit = {
    
    if (solutions.isEmpty) {
      println("No solutions found!")
      return
    }
    
    println("\n=== Solution Analysis ===")
    
    // Analyze objectives
    val responseRates = solutions.map(-_.getObjective(0)) // Convert back from minimization
    val constraintViolations = solutions.map(_.getObjective(1))
    val loadBalanceScores = solutions.map(_.getObjective(2))
    
    println(f"Response Rate - Min: ${responseRates.min}%.2f, Max: ${responseRates.max}%.2f, Avg: ${responseRates.sum / responseRates.length}%.2f")
    println(f"Constraint Violations - Min: ${constraintViolations.min}%.0f, Max: ${constraintViolations.max}%.0f, Avg: ${constraintViolations.sum / constraintViolations.length}%.2f")
    println(f"Load Balance Score - Min: ${loadBalanceScores.min}%.2f, Max: ${loadBalanceScores.max}%.2f, Avg: ${loadBalanceScores.sum / loadBalanceScores.length}%.2f")
    
    // Find best solution for each objective
    val bestResponseRateSolution = solutions.maxBy(-_.getObjective(0))
    val bestConstraintSolution = solutions.minBy(_.getObjective(1))
    val bestLoadBalanceSolution = solutions.minBy(_.getObjective(2))
    
    println("\n=== Best Solutions ===")
    println("Best Response Rate Solution:")
    printSolutionAnalysis(problem, bestResponseRateSolution)
    
    println("\nBest Constraint Solution:")
    printSolutionAnalysis(problem, bestConstraintSolution)
    
    println("\nBest Load Balance Solution:")
    printSolutionAnalysis(problem, bestLoadBalanceSolution)
    
    // Analyze feasible solutions (no constraint violations)
    val feasibleSolutions = solutions.filter(_.getObjective(1) == 0.0)
    if (feasibleSolutions.nonEmpty) {
      println(f"\n${feasibleSolutions.length} feasible solutions found (${feasibleSolutions.length.toDouble / solutions.length * 100}%.1f%%)")
      val bestFeasible = feasibleSolutions.maxBy(-_.getObjective(0))
      println("Best Feasible Solution:")
      printSolutionAnalysis(problem, bestFeasible)
    } else {
      println("\nNo fully feasible solutions found. Consider relaxing constraints or increasing population size.")
    }
  }

  private def printSolutionAnalysis(problem: CampaignSchedulingProblem, solution: DoubleSolution): Unit = {
    val analysis = problem.analyzeAssignments(solution)
    
    println(f"  Response Rate: ${analysis.totalResponseRate}%.2f")
    println(f"  Constraint Violations: ${analysis.constraintViolations}%.0f")
    println(f"  Load Balance Score: ${analysis.loadBalanceScore}%.2f")
    
    // Show distribution across time slots (first 24 hours)
    println("  Time Slot Distribution (first 24 hours):")
    (0 until math.min(24, analysis.customersPerTimeSlot.length)).foreach { hour =>
      val count = analysis.customersPerTimeSlot(hour)
      val percentage = count.toDouble / problem.getCustomerCount * 100
      println(f"    Hour $hour%2d: $count%6d customers (${percentage}%.1f%%)")
    }
    
    // Show channel distribution
    println("  Channel Distribution:")
    analysis.customersPerChannel.zipWithIndex.foreach { case (count, channel) =>
      val percentage = count.toDouble / problem.getCustomerCount * 100
      println(f"    Channel $channel: $count%8d customers (${percentage}%.1f%%)")
    }
  }

  private def saveResults(population: java.util.List[DoubleSolution]): Unit = {
    val timestamp = System.currentTimeMillis()
    
    val output = new SolutionListOutput(population)
      .setSeparator("\t")
      .setVarFileOutputContext(new DefaultFileOutputContext(s"campaign_variables_$timestamp.tsv"))
      .setFunFileOutputContext(new DefaultFileOutputContext(s"campaign_objectives_$timestamp.tsv"))
    
    output.print()
    
    println(s"\n=== Results Saved ===")
    println(s"Variables: campaign_variables_$timestamp.tsv")
    println(s"Objectives: campaign_objectives_$timestamp.tsv")
  }

  private def createSparkConfiguration(sparkConfig: SparkConfig): SparkConf = {
    new SparkConf()
      .setAppName(sparkConfig.appName)
      .setIfMissing("spark.master", sparkConfig.master)
      .set("spark.executor.memory", sparkConfig.executorMemory)
      .set("spark.driver.memory", sparkConfig.driverMemory)
      .set("spark.driver.maxResultSize", sparkConfig.maxResultSize)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      // Optimize for large datasets
      .set("spark.sql.execution.arrow.pyspark.enabled", "true")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
  }

  // Utility method for Zeppelin notebook integration
  def createOptimizationSession(
      customers: Int = 100000,
      timeSlots: Int = 60,
      channels: Int = 4,
      populationSize: Int = 100,
      maxEvaluations: Int = 10000
  ): (CampaignSchedulingProblem, OptimizationConfig) = {
    
    val problemConfig = ProblemConfig(
      numberOfCustomers = customers,
      numberOfTimeSlots = timeSlots,
      numberOfChannels = channels
    )
    
    val optimizationConfig = OptimizationConfig(
      populationSize = populationSize,
      maxEvaluations = maxEvaluations
    )
    
    val problem = new CampaignSchedulingProblem(
      numberOfCustomers = problemConfig.numberOfCustomers,
      numberOfTimeSlots = problemConfig.numberOfTimeSlots,
      numberOfChannels = problemConfig.numberOfChannels,
      maxCustomersPerHour = problemConfig.maxCustomersPerHour,
      minSendingInterval = problemConfig.minSendingInterval
    )
    
    (problem, optimizationConfig)
  }
} 
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.