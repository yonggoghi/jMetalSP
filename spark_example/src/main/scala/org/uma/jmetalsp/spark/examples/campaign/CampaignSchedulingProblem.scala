package org.uma.jmetalsp.spark.examples.campaign

import org.uma.jmetal.problem.impl.AbstractDoubleProblem
import org.uma.jmetal.solution.DoubleSolution
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Multi-objective campaign scheduling problem with sophisticated time preferences
 * 
 * Objectives:
 * 1. Maximize overall response rate (minimize negative response rate)
 * 2. Maximize ARPU-weighted expected value (minimize negative ARPU-weighted value)
 * 
 * Variables per customer: [timeSlot, channel, businessPriority]
 * - timeSlot: continuous [0, 168) representing hour in the week (0-167)
 * - channel: continuous [0, 4) representing communication channel (Email=0, SMS=1, Push=2, In-app=3)
 * - businessPriority: continuous [0, 2] representing business-driven priority based on ARPU/LTV
 * 
 * Constraints:
 * - Hourly capacity: max customers per hour across all channels
 * - Budget constraint: total campaign cost
 * - 48-hour minimum between contacts for same customer
 * - Business priority threshold for assignment (ARPU-based)
 */
class CampaignSchedulingProblem(
  val customers: Array[Customer],
  val maxCustomersPerHour: Int = 500,
  val campaignBudget: Double = 50000.0,
  val campaignDurationHours: Int = 168, // One week by default
  val businessPriorityThreshold: Double = 0.3 // Configurable ARPU-based threshold
) extends AbstractDoubleProblem {

  // 3 variables per customer: timeSlot, channel, businessPriority
  private val variablesPerCustomer = 3
  private val totalVariables = customers.length * variablesPerCustomer
  
  // 2 objectives: response rate and ARPU-weighted value
  private val numberOfObjectives = 2
  
  // 2 constraints: capacity and budget
  private val numberOfConstraints = 2
  
  // Cost per message by channel (in dollars)
  private val channelCosts = Array(0.10, 0.25, 0.05, 0.15) // Email, SMS, Push, In-app
  
  // Initialize problem
  setNumberOfVariables(totalVariables)
  setNumberOfObjectives(numberOfObjectives)
  setNumberOfConstraints(numberOfConstraints)
  setName("CampaignSchedulingProblem")
  
  // Set variable bounds
  val lowerLimit = new java.util.ArrayList[java.lang.Double]()
  val upperLimit = new java.util.ArrayList[java.lang.Double]()
  
  for (i <- 0 until customers.length) {
    lowerLimit.add(0.0)    // timeSlot: [0, 168)
    upperLimit.add(168.0)
    
    lowerLimit.add(0.0)    // channel: [0, 4)
    upperLimit.add(4.0)
    
    lowerLimit.add(0.0)    // businessPriority: [0, 2]
    upperLimit.add(2.0)
  }
  
  setLowerLimit(lowerLimit)
  setUpperLimit(upperLimit)

  override def evaluate(solution: DoubleSolution): Unit = {
    val schedule = decodeSchedule(solution)
    
    // Objective 1: Total expected response rate (negate for minimization)
    val totalResponseRate = schedule.assignments.map(_.expectedResponseRate).sum
    solution.setObjective(0, -totalResponseRate)
    
    // Objective 2: ARPU-weighted expected value (negate for minimization)
    val arpuWeightedValue = schedule.assignments.map { assignment =>
      val customer = customers(assignment.customerId.toInt)
      val arpuWeight = customer.arpu / 50.0 // Normalize around $50 baseline
      assignment.expectedResponseRate * customer.conversionValue * arpuWeight
    }.sum
    solution.setObjective(1, -arpuWeightedValue)
    
    // Constraint 1: Hourly capacity constraint
    val maxHourlyLoad = schedule.hourlyLoads.max
    val capacityViolation = Math.max(0.0, maxHourlyLoad - maxCustomersPerHour)
    
    // Constraint 2: Budget constraint
    val totalCost = schedule.assignments.map(_.cost).sum
    val budgetViolation = Math.max(0.0, totalCost - campaignBudget)
    
    // Store constraint violations as attributes (jMetal approach for handling constraints)
    solution.setAttribute("CapacityViolation", capacityViolation)
    solution.setAttribute("BudgetViolation", budgetViolation)
    
    // Store additional attributes for analysis
    solution.setAttribute("TotalCost", totalCost)
    solution.setAttribute("MaxHourlyLoad", maxHourlyLoad)
    solution.setAttribute("TotalAssignments", schedule.assignments.length)
    solution.setAttribute("CapacityViolation", capacityViolation)
    solution.setAttribute("BudgetViolation", budgetViolation)
  }

  /**
   * Decode a solution into a campaign schedule
   */
  def decodeSchedule(solution: DoubleSolution): CampaignSchedule = {
    val assignments = ArrayBuffer[CustomerAssignment]()
    val hourlyLoads = Array.fill(campaignDurationHours)(0)
    
    // Statistics for debugging
    var businessPriorityFilteredOut = 0
    var capacityFilteredOut = 0
    var contactConstraintFilteredOut = 0
    var totalProcessed = 0
    
    for (customerId <- customers.indices) {
      totalProcessed += 1
      val customer = customers(customerId)
      val baseIndex = customerId * variablesPerCustomer
      
      val timeSlotRaw = solution.getVariableValue(baseIndex)
      val channelRaw = solution.getVariableValue(baseIndex + 1)
      val businessPriorityRaw = solution.getVariableValue(baseIndex + 2)
      
      // Convert continuous variables to discrete values
      val timeSlot = Math.floor(timeSlotRaw).toInt.min(campaignDurationHours - 1).max(0)
      val channel = Math.floor(channelRaw).toInt.min(3).max(0)
      
      // Calculate actual business priority based on customer ARPU/LTV
      val customerBusinessPriority = customer.getBusinessPriority
      val finalPriority = (businessPriorityRaw / 2.0) * customerBusinessPriority // Scale by customer's business value
      
      // Check business priority threshold (ARPU-based filtering)
      if (finalPriority < businessPriorityThreshold) {
        businessPriorityFilteredOut += 1
      } else {
        // Check 48-hour contact constraint
        if (!customer.canBeContacted(timeSlot)) {
          contactConstraintFilteredOut += 1
        } else {
          // Check hourly capacity constraint
          if (hourlyLoads(timeSlot) >= maxCustomersPerHour) {
            capacityFilteredOut += 1
          } else {
            // Customer can be assigned - calculate assignment details
            val responseRate = customer.getResponseRate(timeSlot, channel)
            val timePreferenceMultiplier = customer.getTimePreferenceMultiplier(timeSlot)
            val finalResponseRate = responseRate * timePreferenceMultiplier
            
            val cost = channelCosts(channel)
            
            assignments += CustomerAssignment(
              customerId = customerId.toLong,
              timeSlot = timeSlot,
              channel = channel,
              expectedResponseRate = finalResponseRate,
              cost = cost,
              priority = finalPriority
            )
            
            hourlyLoads(timeSlot) += 1
          }
        }
      }
    }
    
    // Store filtering statistics in solution for debugging
    solution.setAttribute("BusinessPriorityFilteredOut", businessPriorityFilteredOut)
    solution.setAttribute("ContactConstraintFilteredOut", contactConstraintFilteredOut)
    solution.setAttribute("CapacityFilteredOut", capacityFilteredOut)
    solution.setAttribute("TotalProcessed", totalProcessed)
    
    CampaignSchedule(
      assignments = assignments.toArray,
      hourlyLoads = hourlyLoads,
      totalCost = assignments.map(_.cost).sum,
      totalExpectedResponses = assignments.map(_.expectedResponseRate).sum
    )
  }

  /**
   * Get problem statistics for reporting
   */
  def getStatistics: ProblemStatistics = {
    val totalCustomers = customers.length
    val totalTimeSlots = campaignDurationHours
    val totalChannels = 4
    val maxPossibleAssignments = totalCustomers.toLong
    val maxTheoreticalHourlyLoad = totalCustomers
    
    // Calculate business value distribution
    val customerPriorities = customers.map(_.getBusinessPriority)
    val avgPriority = customerPriorities.sum / customerPriorities.length
    val highPriorityCustomers = customerPriorities.count(_ > 1.0)
    
    val totalBudgetIfAllAssigned = customers.length * channelCosts.min // Minimum cost scenario
    val avgArpu = customers.map(_.arpu).sum / customers.length
    
    ProblemStatistics(
      totalCustomers = totalCustomers,
      totalTimeSlots = totalTimeSlots,
      totalChannels = totalChannels,
      maxPossibleAssignments = maxPossibleAssignments,
      maxCustomersPerHour = maxCustomersPerHour,
      campaignBudget = campaignBudget,
      maxTheoreticalHourlyLoad = maxTheoreticalHourlyLoad,
      budgetUtilizationIfAllAssigned = totalBudgetIfAllAssigned / campaignBudget,
      businessPriorityThreshold = businessPriorityThreshold,
      averageCustomerPriority = avgPriority,
      highPriorityCustomerCount = highPriorityCustomers,
      averageArpu = avgArpu
    )
  }
}

/**
 * Represents a customer assignment in the campaign schedule
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
  hourlyLoads: Array[Int],
  totalCost: Double,
  totalExpectedResponses: Double
)

/**
 * Performance metrics for a schedule
 */
case class ScheduleMetrics(
  totalResponseRate: Double,
  totalCost: Double,
  customerValue: Double, // ARPU-weighted value
  maxHourlyLoad: Int,
  utilizationEfficiency: Double, // Percentage of customers assigned
  costPerResponse: Double,
  arpuWeightedROI: Double, // Value/Cost ratio
  channelDistribution: Map[Int, Int] // Channel -> count
)

/**
 * Problem configuration and statistics
 */
case class ProblemStatistics(
  totalCustomers: Int,
  totalTimeSlots: Int,
  totalChannels: Int,
  maxPossibleAssignments: Long,
  maxCustomersPerHour: Int,
  campaignBudget: Double,
  maxTheoreticalHourlyLoad: Int,
  budgetUtilizationIfAllAssigned: Double,
  businessPriorityThreshold: Double,
  averageCustomerPriority: Double,
  highPriorityCustomerCount: Int,
  averageArpu: Double
) {
  override def toString: String = {
    f"""Problem Statistics:
       |  Total customers: $totalCustomers
       |  Campaign duration: $totalTimeSlots hours (${totalTimeSlots/24} days)
       |  Available channels: $totalChannels
       |  Max customers per hour: $maxCustomersPerHour
       |  Campaign budget: $$${campaignBudget}%.2f
       |  Business priority threshold: ${businessPriorityThreshold}%.2f
       |  Average customer priority: ${averageCustomerPriority}%.3f
       |  High priority customers (>1.0): $highPriorityCustomerCount
       |  Average customer ARPU: $$${averageArpu}%.2f/month
       |  Max theoretical hourly load: $maxTheoreticalHourlyLoad
       |  Budget utilization if all assigned: ${budgetUtilizationIfAllAssigned * 100}%.1f%%""".stripMargin
  }
} 