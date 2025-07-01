error id: file://<WORKSPACE>/spark_example/CampaignSchedulingProblem.scala:`<none>`.
file://<WORKSPACE>/spark_example/CampaignSchedulingProblem.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1239
uri: file://<WORKSPACE>/spark_example/CampaignSchedulingProblem.scala
text:
```scala
package org.uma.jmetalsp.spark.examples.campaign

import org.uma.jmetal.problem.impl.AbstractDoubleProblem
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.solution.impl.DefaultDoubleSolution
import org.uma.jmetalsp.DynamicProblem
import org.uma.jmetalsp.observeddata.ObservedValue
import org.uma.jmetalsp.observer.Observable
import org.uma.jmetalsp.observer.impl.DefaultObservable
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Multi-objective Campaign Message Sending Scheduling Problem
 * 
 * Problem characteristics:
 * - 10M customers
 * - 60 time slots (hours)
 * - 4 channel types
 * - Constraints: minimum 48-hour sending interval, maximum customers per hour
 * - Objectives: maximize overall response rate
 * 
 * Solution representation:
 * - Each customer has variables: [timeSlot, channel]
 * - timeSlot: 0-59 (60 hours)
 * - channel: 0-3 (4 channel types)
 */
class CampaignSchedulingProblem(
    val numberOfCustomers: Int = 10000000, // 10M customers
    val numberOfTimeSlots: Int = 60,       // 60 hours
    val numberOfChannels: Int = 4,         // 4 channel types
    val maxCustomersPerHour: Int = 200000, // Max customers per hour constraint@@
    val minSendingInterval: Int = 48,      // Minimum 48-hour interval
    observable: Observable[ObservedValue[Integer]] = new DefaultObservable[ObservedValue[Integer]]()
) extends AbstractDoubleProblem with DynamicProblem[DoubleSolution, ObservedValue[Integer]] {

  // Customer data structures
  case class Customer(
      id: Int,
      responseRates: Array[Double], // Response rate for each channel
      lastSentTime: Int = -1,       // Last time a message was sent (-1 if never)
      preferredTimeSlots: Array[Double] = Array.fill(numberOfTimeSlots)(1.0) // Time preference weights
  )

  // Initialize customer database (in practice, this would be loaded from external source)
  private val customers: Array[Customer] = initializeCustomers()
  
  // Track problem modifications for dynamic updates
  @volatile private var theProblemHasBeenModified: Boolean = false
  @volatile private var currentTime: Double = 0.0

  // Problem setup
  setNumberOfVariables(numberOfCustomers * 2) // Each customer: [timeSlot, channel]
  setNumberOfObjectives(3) // 1. Maximize response rate, 2. Minimize constraint violations, 3. Load balancing
  setName("CampaignSchedulingProblem")

  // Variable bounds
  private val lowerBounds = Array.fill(numberOfCustomers * 2)(0.0)
  private val upperBounds = (0 until numberOfCustomers * 2).map { i =>
    if (i % 2 == 0) numberOfTimeSlots.toDouble - 1 // Time slot variable
    else numberOfChannels.toDouble - 1             // Channel variable
  }.toArray

  setLowerLimit(lowerBounds.map(Double.box).toList.asJava)
  setUpperLimit(upperBounds.map(Double.box).toList.asJava)

  private def initializeCustomers(): Array[Customer] = {
    val random = new Random(42) // Fixed seed for reproducibility
    
    (0 until numberOfCustomers).map { customerId =>
      // Generate realistic response rates for each channel (0.01 to 0.15)
      val responseRates = Array.fill(numberOfChannels)(0.01 + random.nextDouble() * 0.14)
      
      // Generate time preferences (some customers prefer certain hours)
      val timePreferences = Array.tabulate(numberOfTimeSlots) { timeSlot =>
        // Simulate higher preference for business hours (9-17) and evening (18-21)
        val hour = timeSlot % 24
        if ((hour >= 9 && hour <= 17) || (hour >= 18 && hour <= 21)) {
          0.8 + random.nextDouble() * 0.4 // Higher preference
        } else {
          0.2 + random.nextDouble() * 0.6 // Lower preference
        }
      }
      
      Customer(
        id = customerId,
        responseRates = responseRates,
        preferredTimeSlots = timePreferences
      )
    }.toArray
  }

  override def evaluate(solution: DoubleSolution): Unit = {
    val assignments = extractAssignments(solution)
    
    // Objective 1: Maximize total expected response rate
    val totalResponseRate = calculateTotalResponseRate(assignments)
    
    // Objective 2: Minimize constraint violations
    val constraintViolations = calculateConstraintViolations(assignments)
    
    // Objective 3: Load balancing (minimize variance in customers per time slot)
    val loadBalanceScore = calculateLoadBalanceScore(assignments)
    
    // Set objectives (note: jMetal minimizes, so we negate response rate)
    solution.setObjective(0, -totalResponseRate)        // Maximize response rate
    solution.setObjective(1, constraintViolations)      // Minimize violations
    solution.setObjective(2, loadBalanceScore)          // Minimize load imbalance
  }

  private def extractAssignments(solution: DoubleSolution): Array[(Int, Int)] = {
    (0 until numberOfCustomers).map { customerId =>
      val timeSlot = solution.getVariableValue(customerId * 2).toInt
      val channel = solution.getVariableValue(customerId * 2 + 1).toInt
      (timeSlot, channel)
    }.toArray
  }

  private def calculateTotalResponseRate(assignments: Array[(Int, Int)]): Double = {
    assignments.zipWithIndex.map { case ((timeSlot, channel), customerId) =>
      val customer = customers(customerId)
      val baseResponseRate = customer.responseRates(channel)
      val timePreference = customer.preferredTimeSlots(timeSlot)
      
      // Apply time preference multiplier
      baseResponseRate * timePreference
    }.sum
  }

  private def calculateConstraintViolations(assignments: Array[(Int, Int)]): Double = {
    var violations = 0.0
    
    // Count customers per time slot
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    assignments.foreach { case (timeSlot, _) =>
      customersPerTimeSlot(timeSlot) += 1
    }
    
    // Penalize exceeding maximum customers per hour
    customersPerTimeSlot.foreach { count =>
      if (count > maxCustomersPerHour) {
        violations += (count - maxCustomersPerHour).toDouble
      }
    }
    
    // Check minimum sending interval constraint (simplified)
    // In practice, this would check against historical data
    assignments.zipWithIndex.foreach { case ((timeSlot, _), customerId) =>
      val customer = customers(customerId)
      if (customer.lastSentTime >= 0 && 
          math.abs(timeSlot - customer.lastSentTime) < minSendingInterval) {
        violations += 1.0
      }
    }
    
    violations
  }

  private def calculateLoadBalanceScore(assignments: Array[(Int, Int)]): Double = {
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    assignments.foreach { case (timeSlot, _) =>
      customersPerTimeSlot(timeSlot) += 1
    }
    
    val mean = customersPerTimeSlot.sum.toDouble / numberOfTimeSlots
    val variance = customersPerTimeSlot.map(count => math.pow(count - mean, 2)).sum / numberOfTimeSlots
    
    math.sqrt(variance) // Return standard deviation as load balance score
  }

  // Dynamic problem interface methods
  override def hasTheProblemBeenModified: Boolean = theProblemHasBeenModified

  override def reset(): Unit = {
    theProblemHasBeenModified = false
  }

  override def update(observedData: ObservedValue[Integer]): Unit = {
    currentTime = observedData.getValue.toDouble
    // Update customer preferences or response rates based on new data
    theProblemHasBeenModified = true
  }

  override def getObservable: Observable[ObservedValue[Integer]] = observable

  // Utility methods for analysis
  def getCustomerCount: Int = numberOfCustomers
  def getTimeSlotCount: Int = numberOfTimeSlots
  def getChannelCount: Int = numberOfChannels
  
  def analyzeAssignments(solution: DoubleSolution): CampaignAnalysis = {
    val assignments = extractAssignments(solution)
    
    val customersPerTimeSlot = Array.fill(numberOfTimeSlots)(0)
    val customersPerChannel = Array.fill(numberOfChannels)(0)
    
    assignments.foreach { case (timeSlot, channel) =>
      customersPerTimeSlot(timeSlot) += 1
      customersPerChannel(channel) += 1
    }
    
    CampaignAnalysis(
      totalResponseRate = -solution.getObjective(0), // Convert back from minimization
      constraintViolations = solution.getObjective(1),
      loadBalanceScore = solution.getObjective(2),
      customersPerTimeSlot = customersPerTimeSlot.toArray,
      customersPerChannel = customersPerChannel.toArray
    )
  }
}

case class CampaignAnalysis(
    totalResponseRate: Double,
    constraintViolations: Double,
    loadBalanceScore: Double,
    customersPerTimeSlot: Array[Int],
    customersPerChannel: Array[Int]
) 
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.