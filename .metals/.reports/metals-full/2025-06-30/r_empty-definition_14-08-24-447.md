error id: file://<WORKSPACE>/spark_example/CompilationTest.scala:`<none>`.
file://<WORKSPACE>/spark_example/CompilationTest.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 866
uri: file://<WORKSPACE>/spark_example/CompilationTest.scala
text:
```scala
package org.uma.jmetalsp.spark.examples.campaign

import org.uma.jmetal.problem.impl.AbstractDoubleProblem
import org.uma.jmetal.solution.DoubleSolution
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Simple compilation test to verify all imports and basic syntax are correct
 */
object CompilationTest {
  
  def main(args: Array[String]): Unit = {
    println("Testing compilation...")
    
    // Test basic JavaConverters usage
    val testList = List(1.0, 2.0, 3.0)
    val javaList = testList.asJava
    println(s"Java list created: ${javaList.size()} elements")
    
    // Test Double.box conversion
    val testArray = Array(1.0, 2.0, 3.0)
    val boxedList = testArray.map(Double.box).toList.asJava
    println(s"Boxed Java list created: ${boxedList.size()} elements")
    
    // Test basic problem instantiation (without full de@@pendencies)
    try {
      val testProblem = new SimpleTestProblem()
      println(s"Test problem created with ${testProblem.getNumberOfVariables} variables")
      println("Basic compilation test passed!")
      println("All imports are correct and syntax is valid.")
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  // Simple test class that doesn't implement complex interfaces
  class SimpleTestProblem extends AbstractDoubleProblem {
    setNumberOfVariables(4)
    setNumberOfObjectives(2)
    setName("SimpleTestProblem")
    
    // Test the asJava conversion with proper boxing
    private val bounds = Array(0.0, 1.0, 0.0, 1.0)
    setLowerLimit(bounds.take(2).map(Double.box).toList.asJava)
    setUpperLimit(bounds.drop(2).map(Double.box).toList.asJava)
    
    override def evaluate(solution: DoubleSolution): Unit = {
      solution.setObjective(0, solution.getVariableValue(0))
      solution.setObjective(1, solution.getVariableValue(1))
    }
  }
} 
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.