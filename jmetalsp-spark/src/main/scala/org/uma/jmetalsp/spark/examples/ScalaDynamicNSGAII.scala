package org.uma.jmetalsp.spark.examples

import org.apache.spark.SparkConf
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.{AlgorithmRunner, ProblemUtils}
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import org.uma.jmetalsp.spark.evaluator.{SparkSolutionListEvaluator, ZDT1}
import org.apache.spark.SparkContext

import scala.util.{Failure, Success, Try}

/**
 * Simplified Scala implementation of NSGA-II using Spark for parallel evaluation
 * 
 * This example demonstrates:
 * 1. Basic NSGA-II algorithm with Spark-based parallel evaluation
 * 2. Functional programming patterns in Scala
 * 3. Configuration using case classes
 * 4. Error handling with Try/Success/Failure
 * 
 * Usage:
 * spark-submit --class="org.uma.jmetalsp.spark.examples.ScalaDynamicNSGAII" \
 *   --master local[4] \
 *   jmetalsp-spark-2.1-SNAPSHOT-jar-with-dependencies.jar
 */
object ScalaDynamicNSGAII {
  
  // Algorithm configuration
  case class AlgorithmConfig(
    populationSize: Int = 100,
    maxEvaluations: Int = 25000,
    crossoverProbability: Double = 0.9,
    crossoverDistributionIndex: Double = 20.0,
    mutationDistributionIndex: Double = 20.0
  )
  
  def main(args: Array[String]): Unit = {
    
    val config = AlgorithmConfig()
    
    println("=== Scala NSGA-II with Spark ===")
    println(s"Population size: ${config.populationSize}")
    println(s"Max evaluations: ${config.maxEvaluations}")
    println("=================================")
    
    Try {
      runOptimization(config)
    } match {
      case Success(_) =>
        println("Optimization completed successfully!")
        
      case Failure(exception) =>
        println(s"Error during optimization: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }
  
  private def runOptimization(config: AlgorithmConfig): Unit = {
    
    // Load the ZDT1 problem
    val problem = new ZDT1()
    
    // Calculate mutation probability
    val mutationProbability = 1.0 / problem.getNumberOfVariables
    
    // Create genetic operators using functional style
    val operators = createGeneticOperators(config, mutationProbability)
    
    // Configure Spark
    val sparkConf = createSparkConfiguration()
    val sparkContext = new SparkContext(sparkConf)
    
    try {
      sparkContext.setLogLevel("WARN")
      
      println("Spark Context initialized successfully")
      println(s"Spark Master: ${sparkContext.master}")
      println(s"Default parallelism: ${sparkContext.defaultParallelism}")
      
      // Create Spark-based solution evaluator
      val evaluator = new SparkSolutionListEvaluator[DoubleSolution](sparkContext)
      
      // Build NSGA-II algorithm with Spark evaluator
      val algorithm = new NSGAIIBuilder[DoubleSolution](problem, operators.crossover, operators.mutation, config.populationSize)
        .setSelectionOperator(operators.selection)
        .setMaxEvaluations(config.maxEvaluations)
        .setSolutionListEvaluator(evaluator)
        .build()
      
      println("Algorithm configured. Starting optimization...")
      
      // Run the algorithm
      val algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute()
      
      // Get results
      val population = algorithm.getResult
      val computingTime = algorithmRunner.getComputingTime
      
      println(s"Optimization completed!")
      println(s"Total execution time: ${computingTime}ms")
      println(s"Solutions found: ${population.size()}")
      
      // Display some statistics
      if (!population.isEmpty) {
        println("\n=== Solution Statistics ===")
        
        // Find best solutions for each objective
        val objective1Values = (0 until population.size()).map(i => population.get(i).getObjective(0))
        val objective2Values = (0 until population.size()).map(i => population.get(i).getObjective(1))
        
        println(f"Objective 1 - Min: ${objective1Values.min}%.6f, Max: ${objective1Values.max}%.6f")
        println(f"Objective 2 - Min: ${objective2Values.min}%.6f, Max: ${objective2Values.max}%.6f")
        
        // Show first few solutions
        println("\n=== First 5 Solutions ===")
        for (i <- 0 until Math.min(5, population.size())) {
          val solution = population.get(i)
          val obj1 = solution.getObjective(0)
          val obj2 = solution.getObjective(1)
          println(f"Solution $i: f1=${obj1}%.6f, f2=${obj2}%.6f")
        }
      }
      
      // Save results to files
      val output = new SolutionListOutput(population)
        .setSeparator("\t")
        .setVarFileOutputContext(new DefaultFileOutputContext("VAR_scala_dynamic.tsv"))
        .setFunFileOutputContext(new DefaultFileOutputContext("FUN_scala_dynamic.tsv"))
      
      output.print()
      
      println("\n=== Results saved ===")
      println("Variables: VAR_scala_dynamic.tsv")
      println("Objectives: FUN_scala_dynamic.tsv")
      
    } finally {
      // Clean up Spark context
      sparkContext.stop()
      println("Spark Context stopped.")
    }
  }
  
  // Case class for genetic operators
  case class GeneticOperators(
    crossover: SBXCrossover,
    mutation: PolynomialMutation,
    selection: BinaryTournamentSelection[DoubleSolution]
  )
  
  private def createGeneticOperators(config: AlgorithmConfig, mutationProbability: Double): GeneticOperators = {
    GeneticOperators(
      crossover = new SBXCrossover(config.crossoverProbability, config.crossoverDistributionIndex),
      mutation = new PolynomialMutation(mutationProbability, config.mutationDistributionIndex),
      selection = new BinaryTournamentSelection[DoubleSolution](
        new RankingAndCrowdingDistanceComparator[DoubleSolution]()
      )
    )
  }
  
  private def createSparkConfiguration(): SparkConf = {
    new SparkConf()
      .setAppName("Scala NSGA-II with jMetalSP")
      .setIfMissing("spark.master", "local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
  }
} 