package org.uma.jmetalsp.spark.examples.campaign

import org.uma.jmetal.problem.impl.AbstractDoubleProblem
import org.uma.jmetal.solution.DoubleSolution

import scala.collection.mutable
import scala.util.Random
import java.util.{ArrayList, List => JavaList}

/**
 * Multi-objective campaign message scheduling problem
 * 
 * This problem aims to optimize campaign message scheduling with the following characteristics:
 * - 10M customers (scaled down for demo)
 * - 60 time slots (hours)
 * - 4 sending channel types
 * 
 * Constraints:
 * - Minimum 48-hour sending interval per customer
 * - Maximum customers per hour capacity
 * 
 * Objectives:
 * - Maximize overall response rate
 * - Minimize resource usage (secondary objective)
 * - Maximize customer satisfaction (balance across channels)
 * 
 * Solution encoding:
 * Each solution represents a scheduling matrix where variables encode:
 * - Customer assignment priorities
 * - Time slot preferences
 * - Channel selection weights
 */
class CampaignSchedulingProblem(
  val customers: Array[Customer],
  val timeSlots: Int = 60,
  val channels: Int = 4,
  val maxCustomersPerHour: Int = 50000, // Capacity constraint
  val campaignBudget: Double = 1000000.0 // Budget constraint
) extends AbstractDoubleProblem {

  // Problem dimensions
  private val numCustomers = customers.length
  private val solutionLength = numCustomers * 3 // [timeSlot, channel, priority] per customer
  
  // Channel costs (per message)
  private val channelCosts = Array(0.01, 0.05, 0.02, 0.03) // Email, SMS, Push, In-app
  
  // Initialize problem
  setNumberOfVariables(solutionLength)
  setNumberOfObjectives(3) // Response rate, cost efficiency, customer satisfaction
  setNumberOfConstraints(2) // Capacity and budget constraints
  setName("CampaignSchedulingProblem")
  
  // Variable bounds using jMetal API
  private val lowerLimits = new ArrayList[java.lang.Double](solutionLength)
  private val upperLimits = new ArrayList[java.lang.Double](solutionLength)
  
  for (i <- 0 until solutionLength) {
    i % 3 match {
      case 0 => // Time slot
        lowerLimits.add(0.0)
        upperLimits.add(timeSlots.toDouble - 1.0)
      case 1 => // Channel
        lowerLimits.add(0.0)
        upperLimits.add(channels.toDouble - 1.0)
      case 2 => // Priority/assignment probability
        lowerLimits.add(0.0)
        upperLimits.add(1.0)
    }
  }
  
  setLowerLimit(lowerLimits)
  setUpperLimit(upperLimits)

  override def evaluate(solution: DoubleSolution): Unit = {
    val schedule = decodeSchedule(solution)
    val metrics = evaluateSchedule(schedule)
    
    // Set objectives (to be maximized, so we negate for minimization)
    solution.setObjective(0, -metrics.totalResponseRate) // Maximize response rate
    solution.setObjective(1, metrics.totalCost)           // Minimize cost
    solution.setObjective(2, -metrics.customerSatisfaction) // Maximize satisfaction
    
    // Handle constraints using solution attributes (jMetal approach)
    val capacityViolation = Math.max(0.0, metrics.maxHourlyLoad - maxCustomersPerHour)
    val budgetViolation = Math.max(0.0, metrics.totalCost - campaignBudget)
    
    // Store constraint violations as attributes
    solution.setAttribute("CapacityViolation", capacityViolation)
    solution.setAttribute("BudgetViolation", budgetViolation)
  }

  /**
   * Decode solution variables into a concrete schedule
   */
  def decodeSchedule(solution: DoubleSolution): CampaignSchedule = {
    val assignments = mutable.ArrayBuffer[CustomerAssignment]()
    val hourlyLoads = Array.fill(timeSlots)(0)
    
    for (customerId <- customers.indices) {
      val baseIndex = customerId * 3
      val timeSlotRaw = solution.getVariableValue(baseIndex)
      val channelRaw = solution.getVariableValue(baseIndex + 1)
      val priority = solution.getVariableValue(baseIndex + 2)
      
      // Apply threshold for assignment (only assign if priority > 0.3)
      if (priority > 0.3) {
        val timeSlot = Math.floor(timeSlotRaw).toInt
        val channel = Math.floor(channelRaw).toInt
        val customer = customers(customerId)
        
        // Check if assignment is valid (48-hour constraint)
        if (customer.canBeContacted(timeSlot) && hourlyLoads(timeSlot) < maxCustomersPerHour) {
          assignments += CustomerAssignment(
            customerId = customer.id,
            timeSlot = timeSlot,
            channel = channel,
            expectedResponseRate = customer.getResponseRate(timeSlot, channel),
            cost = channelCosts(channel),
            priority = priority
          )
          hourlyLoads(timeSlot) += 1
        }
      }
    }
    
    CampaignSchedule(assignments.toArray, hourlyLoads)
  }

  /**
   * Evaluate the quality of a schedule
   */
  private def evaluateSchedule(schedule: CampaignSchedule): ScheduleMetrics = {
    val assignments = schedule.assignments
    
    // Calculate total expected response rate
    val totalResponseRate = assignments.map(_.expectedResponseRate).sum
    
    // Calculate total cost
    val totalCost = assignments.map(_.cost).sum
    
    // Calculate customer satisfaction (diversity and preference matching)
    val customerSatisfaction = calculateCustomerSatisfaction(assignments)
    
    // Calculate maximum hourly load
    val maxHourlyLoad = schedule.hourlyLoads.max
    
    // Calculate utilization efficiency
    val utilizationEfficiency = assignments.length.toDouble / numCustomers
    
    ScheduleMetrics(
      totalResponseRate = totalResponseRate,
      totalCost = totalCost,
      customerSatisfaction = customerSatisfaction,
      maxHourlyLoad = maxHourlyLoad,
      utilizationEfficiency = utilizationEfficiency,
      totalAssignments = assignments.length
    )
  }

  /**
   * Calculate customer satisfaction based on channel preferences and diversity
   */
  private def calculateCustomerSatisfaction(assignments: Array[CustomerAssignment]): Double = {
    if (assignments.isEmpty) return 0.0
    
    // Group assignments by customer
    val customerAssignments = assignments.groupBy(_.customerId)
    
    val satisfactionScores = for {
      (customerId, customerAssigns) <- customerAssignments
      customer = customers.find(_.id == customerId).get
    } yield {
      val channelsUsed = customerAssigns.map(_.channel).toSet
      val preferredChannelsUsed = channelsUsed.intersect(customer.preferredChannels)
      
      // Base satisfaction from using preferred channels
      val preferenceScore = preferredChannelsUsed.size.toDouble / Math.max(1, customer.preferredChannels.size)
      
      // Bonus for reasonable frequency (not too many messages)
      val frequencyScore = if (customerAssigns.length <= 3) 1.0 else 1.0 / customerAssigns.length
      
      preferenceScore * frequencyScore
    }
    
    satisfactionScores.sum / customerAssignments.size
  }

  /**
   * Get problem statistics for reporting
   */
  def getStatistics: ProblemStatistics = {
    ProblemStatistics(
      numCustomers = numCustomers,
      timeSlots = timeSlots,
      channels = channels,
      maxCustomersPerHour = maxCustomersPerHour,
      campaignBudget = campaignBudget,
      solutionLength = solutionLength,
      searchSpaceSize = Math.pow(timeSlots * channels * 2, numCustomers) // Rough estimate
    )
  }
}

/**
 * Represents an assignment of a customer to a time slot and channel
 */
case class CustomerAssignment(
  customerId: Long,
  timeSlot: Int,
  channel: Int,
  expectedResponseRate: Double,
  cost: Double,
  priority: Double
)

/**
 * Represents a complete campaign schedule
 */
case class CampaignSchedule(
  assignments: Array[CustomerAssignment],
  hourlyLoads: Array[Int]
) {
  
  def getTotalExpectedResponses: Double = assignments.map(_.expectedResponseRate).sum
  def getTotalCost: Double = assignments.map(_.cost).sum
  def getAssignmentCount: Int = assignments.length
  def getMaxHourlyLoad: Int = hourlyLoads.max
  def getAverageHourlyLoad: Double = hourlyLoads.sum.toDouble / hourlyLoads.length
  
  /**
   * Get assignments for a specific time slot
   */
  def getAssignmentsForTimeSlot(timeSlot: Int): Array[CustomerAssignment] = {
    assignments.filter(_.timeSlot == timeSlot)
  }
  
  /**
   * Get assignments for a specific channel
   */
  def getAssignmentsForChannel(channel: Int): Array[CustomerAssignment] = {
    assignments.filter(_.channel == channel)
  }
  
  override def toString: String = {
    s"""Campaign Schedule Summary:
       |  Total assignments: ${getAssignmentCount}
       |  Expected responses: ${getTotalExpectedResponses}
       |  Total cost: $$${getTotalCost}
       |  Max hourly load: ${getMaxHourlyLoad}
       |  Average hourly load: ${getAverageHourlyLoad}""".stripMargin
  }
}

/**
 * Metrics for evaluating a schedule
 */
case class ScheduleMetrics(
  totalResponseRate: Double,
  totalCost: Double,
  customerSatisfaction: Double,
  maxHourlyLoad: Int,
  utilizationEfficiency: Double,
  totalAssignments: Int
)

/**
 * Problem statistics for reporting
 */
case class ProblemStatistics(
  numCustomers: Int,
  timeSlots: Int,
  channels: Int,
  maxCustomersPerHour: Int,
  campaignBudget: Double,
  solutionLength: Int,
  searchSpaceSize: Double
) {
  override def toString: String = {
    s"""Problem Statistics:
       |  Customers: $numCustomers
       |  Time slots: $timeSlots hours
       |  Channels: $channels
       |  Max customers/hour: $maxCustomersPerHour
       |  Campaign budget: $$${campaignBudget}
       |  Solution length: $solutionLength variables
       |  Search space size: ~${searchSpaceSize.toInt} combinations""".stripMargin
  }
} 