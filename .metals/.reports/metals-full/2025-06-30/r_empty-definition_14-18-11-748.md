error id: file://<WORKSPACE>/spark_example/CampaignScheduling_Zeppelin_Notebook.scala:`<none>`.
file://<WORKSPACE>/spark_example/CampaignScheduling_Zeppelin_Notebook.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 2253
uri: file://<WORKSPACE>/spark_example/CampaignScheduling_Zeppelin_Notebook.scala
text:
```scala
// ===============================================================
// Campaign Message Sending Scheduling Optimization
// Zeppelin Notebook - Scala Code
// ===============================================================

// ---------------------------------------------------------------
// Paragraph 1: Import Libraries
// ---------------------------------------------------------------
// %spark
// Import necessary libraries
import org.apache.spark.{SparkConf, SparkContext}
import org.uma.jmetal.algorithm.multiobjective.nsgaiii.NSGAIIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetalsp.spark.evaluator.SparkSolutionListEvaluator
import org.uma.jmetal.util.reference.DefaultReferenceDirectionsFactory

// Import our custom campaign scheduling classes
// Note: In practice, these would be compiled and added to the classpath
// import org.uma.jmetalsp.spark.examples.campaign.{CampaignSchedulingProblem, CampaignOptimizer}

println("Libraries imported successfully!")
println(s"Spark version: ${spark.version}")
println(s"Scala version: ${scala.util.Properties.versionString}")

// ---------------------------------------------------------------
// Paragraph 2: Configuration
// ---------------------------------------------------------------
// %spark
// Configuration for the optimization problem
case class OptimizationConfig(
    populationSize: Int = 50,        // Smaller for demo
    maxEvaluations: Int = 1000,      // Smaller for demo
    crossoverProbability: Double = 0.9,
    crossoverDistributionIndex: Double = 20.0,
    mutationDistributionIndex: Double = 20.0,
    numberOfDivisions: Int = 8       // For NSGA-III reference points
)

case class ProblemConfig(
    numberOfCustomers: Int = 10000,  // Scaled down from 10M for demo
    numberOfTimeSlots: Int = 60,     // 60 hours
    numberOfChannels: Int = 4,       // 4 channel types
    maxCustomersPerHour: Int = 200,  // Scaled down constra@@int
    minSendingInterval: Int = 48     // 48-hour minimum interval
)

val optimizationConfig = OptimizationConfig()
val problemConfig = ProblemConfig()

println("Configuration created:")
println(s"- Customers: ${problemConfig.numberOfCustomers}")
println(s"- Time slots: ${problemConfig.numberOfTimeSlots}")
println(s"- Channels: ${problemConfig.numberOfChannels}")
println(s"- Population size: ${optimizationConfig.populationSize}")
println(s"- Max evaluations: ${optimizationConfig.maxEvaluations}")

// ---------------------------------------------------------------
// Paragraph 3: Problem Definition
// ---------------------------------------------------------------
// %spark
// Simplified Campaign Scheduling Problem for demonstration
// (In practice, you would use the full CampaignSchedulingProblem class)

import org.uma.jmetal.problem.impl.AbstractDoubleProblem
import org.uma.jmetal.solution.DoubleSolution
import scala.util.Random
import scala.collection.mutable
import scala.collection.JavaConverters._

class SimpleCampaignProblem(
    val numberOfCustomers: Int = 10000,
    val numberOfTimeSlots: Int = 60,
    val numberOfChannels: Int = 4,
    val maxCustomersPerHour: Int = 200
) extends AbstractDoubleProblem {

  // Problem setup
  setNumberOfVariables(numberOfCustomers * 2) // Each customer: [timeSlot, channel]
  setNumberOfObjectives(3) // Response rate, constraints, load balance
  setName("SimpleCampaignSchedulingProblem")

  // Variable bounds
  private val lowerBounds = Array.fill(numberOfCustomers * 2)(0.0)
  private val upperBounds = (0 until numberOfCustomers * 2).map { i =>
    if (i % 2 == 0) numberOfTimeSlots.toDouble - 1 // Time slot
    else numberOfChannels.toDouble - 1             // Channel
  }.toArray

  setLowerLimit(lowerBounds.map(Double.box).toList.asJava)
  setUpperLimit(upperBounds.map(Double.box).toList.asJava)

  // Simulated customer response rates
  private val random = new Random(42)
  private val customerResponseRates = Array.fill(numberOfCustomers, numberOfChannels) {
    0.01 + random.nextDouble() * 0.14 // 1% to 15% response rate
  }

  override def evaluate(solution: DoubleSolution): Unit = {
    val assignments = extractAssignments(solution)
    
    // Objective 1: Maximize total expected response rate (negated for minimization)
    val totalResponseRate = assignments.zipWithIndex.map { case ((timeSlot, channel), customerId) =>
      customerResponseRates(customerId)(channel)
    }.sum
    
    // Objective 2: Minimize constraint violations
    val constraintViolations = calculateConstraintViolations(assignments)
    
    // Objective 3: Load balancing
    val loadBalanceScore = calculateLoadBalanceScore(assignments)
    
    solution.setObjective(0, -totalResponseRate)  // Maximize response rate
    solution.setObjective(1, constraintViolations) // Minimize violations
    solution.setObjective(2, loadBalanceScore)     // Minimize load imbalance
  }

  private def extractAssignments(solution: DoubleSolution): Array[(Int, Int)] = {
    (0 until numberOfCustomers).map { customerId =>
      val timeSlot = solution.getVariableValue(customerId * 2).toInt
      val channel = solution.getVariableValue(customerId * 2 + 1).toInt
      (timeSlot, channel)
    }.toArray
  }

  private def calculateConstraintViolations(assignments: Array[(Int, Int)]): Double = {
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    assignments.foreach { case (timeSlot, _) =>
      customersPerTimeSlot(timeSlot) += 1
    }
    
    // Count violations of max customers per hour
    customersPerTimeSlot.map(count => math.max(0, count - maxCustomersPerHour)).sum.toDouble
  }

  private def calculateLoadBalanceScore(assignments: Array[(Int, Int)]): Double = {
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    assignments.foreach { case (timeSlot, _) =>
      customersPerTimeSlot(timeSlot) += 1
    }
    
    val mean = customersPerTimeSlot.sum.toDouble / numberOfTimeSlots
    val variance = customersPerTimeSlot.map(count => math.pow(count - mean, 2)).sum / numberOfTimeSlots
    math.sqrt(variance)
  }
  
  def analyzeAssignments(solution: DoubleSolution): (Double, Double, Double, Array[Int]) = {
    val assignments = extractAssignments(solution)
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    assignments.foreach { case (timeSlot, _) => customersPerTimeSlot(timeSlot) += 1 }
    
    (-solution.getObjective(0), solution.getObjective(1), solution.getObjective(2), customersPerTimeSlot.toArray)
  }
}

println("SimpleCampaignProblem class defined successfully!")

// ---------------------------------------------------------------
// Paragraph 4: Create Problem and Operators
// ---------------------------------------------------------------
// %spark
// Create and run the optimization
val problem = new SimpleCampaignProblem(
  numberOfCustomers = problemConfig.numberOfCustomers,
  numberOfTimeSlots = problemConfig.numberOfTimeSlots,
  numberOfChannels = problemConfig.numberOfChannels,
  maxCustomersPerHour = problemConfig.maxCustomersPerHour
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

println("Genetic operators created")
println(f"Mutation probability: ${mutationProbability}%.6f")

// ---------------------------------------------------------------
// Paragraph 5: Run Optimization
// ---------------------------------------------------------------
// %spark
// Build and run NSGA-III algorithm
val evaluator = new SparkSolutionListEvaluator(problem)

// Configure NSGA-III algorithm
val referenceDirections = new DefaultReferenceDirectionsFactory().getDirections(
  optimizationConfig.numberOfObjectives, 
  optimizationConfig.numberOfDivisions
)

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

// ---------------------------------------------------------------
// Paragraph 6: Analyze Results
// ---------------------------------------------------------------
// %spark
// Analyze results
import scala.collection.JavaConverters._

val solutions = population.asScala.toList

if (solutions.nonEmpty) {
  println("\n=== Solution Analysis ===")
  
  // Analyze objectives
  val responseRates = solutions.map(-_.getObjective(0)) // Convert back from minimization
  val constraintViolations = solutions.map(_.getObjective(1))
  val loadBalanceScores = solutions.map(_.getObjective(2))
  
  println(f"Response Rate - Min: ${responseRates.min}%.2f, Max: ${responseRates.max}%.2f, Avg: ${responseRates.sum / responseRates.length}%.2f")
  println(f"Constraint Violations - Min: ${constraintViolations.min}%.0f, Max: ${constraintViolations.max}%.0f, Avg: ${constraintViolations.sum / constraintViolations.length}%.2f")
  println(f"Load Balance Score - Min: ${loadBalanceScores.min}%.2f, Max: ${loadBalanceScores.max}%.2f, Avg: ${loadBalanceScores.sum / loadBalanceScores.length}%.2f")
  
  // Find best solutions
  val bestResponseRateSolution = solutions.maxBy(-_.getObjective(0))
  val bestConstraintSolution = solutions.minBy(_.getObjective(1))
  val bestLoadBalanceSolution = solutions.minBy(_.getObjective(2))
  
  println("\n=== Best Solutions ===")
  
  // Analyze best response rate solution
  val (respRate, constViol, loadBal, timeDistribution) = problem.analyzeAssignments(bestResponseRateSolution)
  println(s"Best Response Rate Solution:")
  println(f"  Response Rate: ${respRate}%.2f")
  println(f"  Constraint Violations: ${constViol}%.0f")
  println(f"  Load Balance Score: ${loadBal}%.2f")
  
  // Show time slot distribution for first 24 hours
  println("  Time Slot Distribution (first 24 hours):")
  (0 until math.min(24, timeDistribution.length)).foreach { hour =>
    val count = timeDistribution(hour)
    val percentage = count.toDouble / problemConfig.numberOfCustomers * 100
    println(f"    Hour $hour%2d: $count%4d customers (${percentage}%.1f%%)")
  }
  
  // Analyze feasible solutions
  val feasibleSolutions = solutions.filter(_.getObjective(1) == 0.0)
  if (feasibleSolutions.nonEmpty) {
    println(f"\n${feasibleSolutions.length} feasible solutions found (${feasibleSolutions.length.toDouble / solutions.length * 100}%.1f%%)")
    val bestFeasible = feasibleSolutions.maxBy(-_.getObjective(0))
    val (fRespRate, _, fLoadBal, _) = problem.analyzeAssignments(bestFeasible)
    println(f"Best Feasible Solution - Response Rate: ${fRespRate}%.2f, Load Balance: ${fLoadBal}%.2f")
  } else {
    println("\nNo fully feasible solutions found. Consider relaxing constraints or increasing population size.")
  }
  
} else {
  println("No solutions found!")
}

// ---------------------------------------------------------------
// Paragraph 7: Prepare Data for Visualization
// ---------------------------------------------------------------
// %spark
// Visualization of results
case class SolutionPoint(responseRate: Double, constraintViolations: Double, loadBalance: Double)

val solutionPoints = solutions.map { sol =>
  SolutionPoint(-sol.getObjective(0), sol.getObjective(1), sol.getObjective(2))
}

// Create DataFrame for visualization
val solutionDF = spark.createDataFrame(solutionPoints)
solutionDF.createOrReplaceTempView("solutions")

println("Solution data prepared for visualization")
solutionDF.show(10)

// ---------------------------------------------------------------
// Paragraph 8: SQL Visualization (use %sql interpreter)
// ---------------------------------------------------------------
/*
%sql
-- Visualize the Pareto front
SELECT responseRate, constraintViolations, loadBalance
FROM solutions
ORDER BY responseRate DESC
*/

// ---------------------------------------------------------------
// Paragraph 9: Production Configuration Example
// ---------------------------------------------------------------
// %spark
// Production-scale configuration example
val productionConfig = OptimizationConfig(
    populationSize = 200,           // Larger population for better diversity
    maxEvaluations = 50000,         // More evaluations for convergence
    crossoverProbability = 0.9,
    crossoverDistributionIndex = 20.0,
    mutationDistributionIndex = 20.0,
    numberOfDivisions = 15          // More reference points for 3 objectives
)

val productionProblemConfig = ProblemConfig(
    numberOfCustomers = 10000000,   // Full 10M customers
    numberOfTimeSlots = 60,         // 60 hours
    numberOfChannels = 4,           // 4 channel types
    maxCustomersPerHour = 200000,   // 200K customers per hour max
    minSendingInterval = 48         // 48-hour minimum interval
)

// Spark configuration for production
val productionSparkConfig = Map(
  "spark.executor.memory" -> "8g",
  "spark.executor.cores" -> "4",
  "spark.executor.instances" -> "20",
  "spark.driver.memory" -> "4g",
  "spark.driver.maxResultSize" -> "2g",
  "spark.sql.adaptive.enabled" -> "true",
  "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
  "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
)

println("Production configuration:")
println(s"- Customers: ${productionProblemConfig.numberOfCustomers}")
println(s"- Variables: ${productionProblemConfig.numberOfCustomers * 2}")
println(s"- Population size: ${productionConfig.populationSize}")
println(s"- Max evaluations: ${productionConfig.maxEvaluations}")
println(s"- Estimated memory requirements: ~${(productionProblemConfig.numberOfCustomers * 2 * 8) / (1024 * 1024 * 1024)}GB for variables")

// Estimated runtime calculation
val estimatedEvaluationsPerSecond = 10 // Conservative estimate
val estimatedRuntimeMinutes = productionConfig.maxEvaluations / estimatedEvaluationsPerSecond / 60
println(f"- Estimated runtime: ~${estimatedRuntimeMinutes}%.0f minutes")

// ---------------------------------------------------------------
// Additional Utility Functions
// ---------------------------------------------------------------
// %spark
// Utility functions for deeper analysis

def analyzeTimeSlotDistribution(solutions: List[DoubleSolution], problem: SimpleCampaignProblem): Unit = {
  println("\n=== Time Slot Distribution Analysis ===")
  
  solutions.take(5).zipWithIndex.foreach { case (solution, index) =>
    val (_, _, _, timeDistribution) = problem.analyzeAssignments(solution)
    println(s"\nSolution ${index + 1}:")
    
    // Find peak hours
    val maxCustomers = timeDistribution.max
    val peakHours = timeDistribution.zipWithIndex.filter(_._1 == maxCustomers).map(_._2)
    println(s"  Peak hours (${maxCustomers} customers): ${peakHours.mkString(", ")}")
    
    // Calculate load variance
    val mean = timeDistribution.sum.toDouble / timeDistribution.length
    val variance = timeDistribution.map(count => math.pow(count - mean, 2)).sum / timeDistribution.length
    println(f"  Load variance: ${variance}%.2f")
  }
}

def generateOptimizationReport(solutions: List[DoubleSolution], problem: SimpleCampaignProblem): Unit = {
  println("\n=== Optimization Report ===")
  
  val responseRates = solutions.map(-_.getObjective(0))
  val constraintViolations = solutions.map(_.getObjective(1))
  val loadBalanceScores = solutions.map(_.getObjective(2))
  
  println(f"Total solutions: ${solutions.length}")
  println(f"Feasible solutions: ${solutions.count(_.getObjective(1) == 0.0)}")
  println(f"Best response rate: ${responseRates.max}%.4f")
  println(f"Average response rate: ${responseRates.sum / responseRates.length}%.4f")
  println(f"Solutions with zero violations: ${constraintViolations.count(_ == 0.0)}")
  println(f"Average constraint violations: ${constraintViolations.sum / constraintViolations.length}%.2f")
}

// Run the analysis
if (solutions.nonEmpty) {
  analyzeTimeSlotDistribution(solutions, problem)
  generateOptimizationReport(solutions, problem)
}

println("\n=== Campaign Scheduling Optimization Complete ===")

// ---------------------------------------------------------------
// Alternative: Using DecimalFormat for Number Formatting
// ---------------------------------------------------------------
// %spark
// If you need comma-separated numbers, use DecimalFormat
import java.text.DecimalFormat

val numberFormatter = new DecimalFormat("#,###")

println("Production configuration (with formatted numbers):")
println(s"- Customers: ${numberFormatter.format(productionProblemConfig.numberOfCustomers)}")
println(s"- Variables: ${numberFormatter.format(productionProblemConfig.numberOfCustomers * 2)}")
println(s"- Population size: ${numberFormatter.format(productionConfig.populationSize)}")
println(s"- Max evaluations: ${numberFormatter.format(productionConfig.maxEvaluations)}")
println(s"- Estimated memory requirements: ~${(productionProblemConfig.numberOfCustomers * 2 * 8) / (1024 * 1024 * 1024)}GB for variables") 
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.