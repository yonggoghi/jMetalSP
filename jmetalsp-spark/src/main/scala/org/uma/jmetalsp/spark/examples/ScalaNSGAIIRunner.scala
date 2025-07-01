package org.uma.jmetalsp.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.{AlgorithmRunner, JMetalLogger, ProblemUtils}
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import org.uma.jmetalsp.spark.evaluator.{SparkSolutionListEvaluator, ZDT1}

import scala.util.{Failure, Success, Try}

/**
 * Scala implementation of NSGA-II using Spark for parallel evaluation
 * 
 * This example demonstrates how to:
 * 1. Configure Spark in Scala
 * 2. Set up NSGA-II algorithm with Spark-based parallel evaluation
 * 3. Solve a multi-objective optimization problem (ZDT1)
 * 4. Handle results and cleanup
 * 
 * Usage:
 * spark-submit --class="org.uma.jmetalsp.spark.examples.ScalaNSGAIIRunner" \
 *   --master local[4] \
 *   jmetalsp-spark-2.1-SNAPSHOT-jar-with-dependencies.jar
 */
object ScalaNSGAIIRunner {
  
  def main(args: Array[String]): Unit = {
    
    // Algorithm parameters
    val populationSize = 100
    val maxEvaluations = 25000
    val crossoverProbability = 0.9
    val crossoverDistributionIndex = 20.0
    val mutationDistributionIndex = 20.0
    
    // Problem selection
    val problemName = if (args.length == 1) args(0) else "org.uma.jmetalsp.spark.evaluator.ZDT1"
    
    println(s"=== Scala NSGA-II with Spark ===")
    println(s"Problem: $problemName")
    println(s"Population size: $populationSize")
    println(s"Max evaluations: $maxEvaluations")
    println(s"Crossover probability: $crossoverProbability")
    println("=====================================")
    
    Try {
      // Load the problem
      val problem: Problem[DoubleSolution] = ProblemUtils.loadProblem(problemName)
      
      // Calculate mutation probability
      val mutationProbability = 1.0 / problem.getNumberOfVariables
      
      // Configure Spark
      val sparkConf = new SparkConf()
        .setAppName("Scala NSGA-II with jMetalSP")
        .setIfMissing("spark.master", "local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      
      val sparkContext = new SparkContext(sparkConf)
      
      try {
        // Set log level to reduce Spark output
        sparkContext.setLogLevel("WARN")
        
        println("Spark Context initialized successfully")
        println(s"Spark Master: ${sparkContext.master}")
        println(s"Default parallelism: ${sparkContext.defaultParallelism}")
        
        // Create genetic operators
        val crossover = new SBXCrossover(crossoverProbability, crossoverDistributionIndex)
        val mutation = new PolynomialMutation(mutationProbability, mutationDistributionIndex)
        val selection = new BinaryTournamentSelection[DoubleSolution](
          new RankingAndCrowdingDistanceComparator[DoubleSolution]()
        )
        
        // Create Spark-based solution evaluator
        val evaluator = new SparkSolutionListEvaluator[DoubleSolution](sparkContext)
        
        // Build NSGA-II algorithm with Spark evaluator
        val algorithm = new NSGAIIBuilder[DoubleSolution](problem, crossover, mutation, populationSize)
          .setSelectionOperator(selection)
          .setMaxEvaluations(maxEvaluations)
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
          .setVarFileOutputContext(new DefaultFileOutputContext("VAR_scala.tsv"))
          .setFunFileOutputContext(new DefaultFileOutputContext("FUN_scala.tsv"))
        
        output.print()
        
        println("\n=== Results saved ===")
        println("Variables: VAR_scala.tsv")
        println("Objectives: FUN_scala.tsv")
        
      } finally {
        // Clean up Spark context
        sparkContext.stop()
        println("Spark Context stopped.")
      }
      
    } match {
      case Success(_) =>
        println("Algorithm execution completed successfully!")
        
      case Failure(exception) =>
        println(s"Error during algorithm execution: ${exception.getMessage}")
        exception.printStackTrace()
        System.exit(1)
    }
  }
} 