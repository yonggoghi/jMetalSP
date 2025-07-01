package org.uma.jmetalsp.spark.examples.campaign

import scala.util.Random

/**
 * Represents a customer in the campaign scheduling problem
 * 
 * @param id Customer unique identifier
 * @param responseRates Matrix of response rates [timeSlot][channel] -> probability
 * @param lastContactTime Last time this customer was contacted (in time slot units)
 * @param preferredChannels Preferred communication channels for this customer
 * @param arpu Average Revenue Per User (monthly)
 * @param lifetimeValue Customer lifetime value
 * @param tier Customer tier (Premium=3, Standard=2, Basic=1)
 * @param conversionValue Expected value per successful campaign response
 * @param timePreferences Customer's preferred time patterns for engagement
 */
case class Customer(
  id: Long,
  responseRates: Array[Array[Double]], // [168 time slots (week)][4 channels]
  lastContactTime: Int = -48, // Initialize to allow immediate contact
  preferredChannels: Set[Int] = Set.empty,
  arpu: Double = 50.0, // Monthly ARPU in dollars
  lifetimeValue: Double = 600.0, // Customer LTV in dollars
  tier: Int = 2, // 1=Basic, 2=Standard, 3=Premium
  conversionValue: Double = 25.0, // Expected revenue per campaign conversion
  timePreferences: CustomerTimePreferences = CustomerTimePreferences()
) {
  
  /**
   * Get response rate for a specific time slot and channel
   * Now includes sophisticated time-based preferences
   */
  def getResponseRate(timeSlot: Int, channel: Int): Double = {
    val weeklySlot = timeSlot % 168 // Convert to weekly pattern (168 hours per week)
    
    if (weeklySlot >= 0 && weeklySlot < responseRates.length && 
        channel >= 0 && channel < responseRates(weeklySlot).length) {
      responseRates(weeklySlot)(channel)
    } else {
      0.0
    }
  }
  
  /**
   * Check if customer can be contacted at given time slot (48-hour minimum interval)
   */
  def canBeContacted(timeSlot: Int): Boolean = {
    timeSlot - lastContactTime >= 48
  }
  
  /**
   * Get the best channel for a given time slot
   */
  def getBestChannel(timeSlot: Int): Int = {
    val weeklySlot = timeSlot % 168
    if (weeklySlot < responseRates.length) {
      responseRates(weeklySlot).zipWithIndex.maxBy(_._1)._2
    } else {
      0
    }
  }
  
  /**
   * Get average response rate across all time slots for a channel
   */
  def getAverageResponseRate(channel: Int): Double = {
    responseRates.map(_(channel)).sum / responseRates.length
  }
  
  /**
   * Calculate expected value of contacting this customer at specific time/channel
   * This now includes time-preference weighting
   */
  def getExpectedValue(timeSlot: Int, channel: Int): Double = {
    val responseRate = getResponseRate(timeSlot, channel)
    val timePreferenceMultiplier = getTimePreferenceMultiplier(timeSlot)
    responseRate * conversionValue * timePreferenceMultiplier
  }
  
  /**
   * Get time preference multiplier for a given time slot
   */
  def getTimePreferenceMultiplier(timeSlot: Int): Double = {
    val weeklySlot = timeSlot % 168
    val dayOfWeek = weeklySlot / 24
    val hourOfDay = weeklySlot % 24
    
    var multiplier = 1.0
    
    // Apply day preferences
    if (timePreferences.preferredDays.contains(dayOfWeek)) {
      multiplier *= 1.3
    } else if (timePreferences.avoidedDays.contains(dayOfWeek)) {
      multiplier *= 0.6
    }
    
    // Apply hour preferences
    if (timePreferences.preferredHours.contains(hourOfDay)) {
      multiplier *= 1.4
    } else if (timePreferences.avoidedHours.contains(hourOfDay)) {
      multiplier *= 0.5
    }
    
    // Apply meal time preferences
    if (timePreferences.preferAfterMeals) {
      if (hourOfDay == 13 || hourOfDay == 14 || hourOfDay == 20 || hourOfDay == 21) { // After lunch/dinner
        multiplier *= 1.2
      }
    }
    
    multiplier
  }
  
  /**
   * Get customer priority based on ARPU and business value
   * Higher ARPU customers get higher priority
   */
  def getBusinessPriority: Double = {
    val arpuNormalized = Math.min(2.0, arpu / 50.0) // Normalize around $50 base
    val tierMultiplier = tier match {
      case 3 => 1.5 // Premium customers
      case 2 => 1.0 // Standard customers  
      case 1 => 0.7 // Basic customers
      case _ => 1.0
    }
    val ltvNormalized = Math.min(2.0, lifetimeValue / 600.0) // Normalize around $600 base
    
    (arpuNormalized * 0.5 + tierMultiplier * 0.3 + ltvNormalized * 0.2).min(2.0)
  }
  
  /**
   * Get customer priority multiplier based on business value
   */
  def getBusinessValueMultiplier: Double = {
    val arpuMultiplier = Math.min(2.0, arpu / 50.0) // Normalize around $50 base
    val tierMultiplier = tier match {
      case 3 => 1.5 // Premium customers
      case 2 => 1.0 // Standard customers  
      case 1 => 0.7 // Basic customers
      case _ => 1.0
    }
    val ltvMultiplier = Math.min(2.0, lifetimeValue / 600.0) // Normalize around $600 base
    
    (arpuMultiplier + tierMultiplier + ltvMultiplier) / 3.0
  }
  
  /**
   * Get tier name for reporting
   */
  def getTierName: String = tier match {
    case 3 => "Premium"
    case 2 => "Standard"
    case 1 => "Basic"
    case _ => "Unknown"
  }
}

/**
 * Customer time preferences for sophisticated scheduling
 */
case class CustomerTimePreferences(
  preferredDays: Set[Int] = Set.empty, // 0=Sunday, 1=Monday, ..., 6=Saturday
  avoidedDays: Set[Int] = Set.empty,
  preferredHours: Set[Int] = Set.empty, // 0-23 hours
  avoidedHours: Set[Int] = Set.empty,
  preferAfterMeals: Boolean = false, // Prefers messages after lunch/dinner
  isEarlyBird: Boolean = false, // Prefers morning messages
  isNightOwl: Boolean = false, // Prefers evening messages
  workingProfessional: Boolean = false // Prefers non-working hours
)

object Customer {
  
  /**
   * Generate random customers for testing/simulation
   * 
   * @param numCustomers Number of customers to generate
   * @param timeSlots Number of time slots (default 60)
   * @param channels Number of channels (default 4)
   * @param seed Random seed for reproducibility
   * @return Array of randomly generated customers
   */
  def generateRandomCustomers(
    numCustomers: Int,
    timeSlots: Int = 168, // Default to weekly pattern (168 hours)
    channels: Int = 4,
    seed: Long = 42L
  ): Array[Customer] = {
    
    val random = new Random(seed)
    
    (0 until numCustomers).map { customerId =>
      // Generate customer business value attributes first
      val customerTier = random.nextDouble() match {
        case x if x < 0.15 => 3 // 15% Premium customers
        case x if x < 0.70 => 2 // 55% Standard customers  
        case _ => 1             // 30% Basic customers
      }
      
      // Generate ARPU based on tier with realistic distributions
      val baseArpu = customerTier match {
        case 3 => 120.0 + random.nextGaussian() * 40.0 // Premium: $120 ± $40
        case 2 => 55.0 + random.nextGaussian() * 20.0  // Standard: $55 ± $20
        case 1 => 25.0 + random.nextGaussian() * 10.0  // Basic: $25 ± $10
        case _ => 50.0
      }
      val arpu = Math.max(5.0, baseArpu) // Minimum $5 ARPU
      
      // Calculate LTV (typically 12-24x monthly ARPU)
      val ltvMultiplier = 12.0 + random.nextDouble() * 12.0 // 12-24 months
      val lifetimeValue = arpu * ltvMultiplier
      
      // Conversion value (typically 20-80% of ARPU per campaign)
      val conversionRate = 0.2 + random.nextDouble() * 0.6 // 20-80%
      val conversionValue = arpu * conversionRate
      
      // Generate sophisticated time preferences
      val timePrefs = generateTimePreferences(random)
      
      // Generate base response rates for each channel
      val channelBaselines = Array(
        0.12 + random.nextGaussian() * 0.04, // Email: 12% ± 4%
        0.18 + random.nextGaussian() * 0.05, // SMS: 18% ± 5%
        0.15 + random.nextGaussian() * 0.04, // Push: 15% ± 4%
        0.08 + random.nextGaussian() * 0.03  // In-app: 8% ± 3%
      ).map(rate => Math.max(0.01, Math.min(0.5, rate)))
      
      // Higher-value customers tend to have slightly better engagement
      val valueMultiplier = customerTier match {
        case 3 => 1.2 // Premium customers 20% better engagement
        case 2 => 1.0 // Standard baseline
        case 1 => 0.8 // Basic customers 20% lower engagement
        case _ => 1.0
      }
      
      // Generate sophisticated weekly response patterns (168 hours = 7 days × 24 hours)
      val responseRates = generateWeeklyResponsePattern(channelBaselines, timePrefs, valueMultiplier, random)
      
      // Create preferred channels set (1-2 channels typically)
      val numPreferred = 1 + random.nextInt(2)
      val preferredChannels = random.shuffle((0 until channels).toList).take(numPreferred).toSet
      
      Customer(
        id = customerId.toLong,
        responseRates = responseRates,
        preferredChannels = preferredChannels,
        arpu = arpu,
        lifetimeValue = lifetimeValue,
        tier = customerTier,
        conversionValue = conversionValue,
        timePreferences = timePrefs
      )
    }.toArray
  }
  
  /**
   * Generate realistic time preferences for a customer
   */
  private def generateTimePreferences(random: scala.util.Random): CustomerTimePreferences = {
    // Customer archetype patterns
    val archetype = random.nextDouble() match {
      case x if x < 0.25 => "working_professional"
      case x if x < 0.40 => "early_bird"
      case x if x < 0.55 => "night_owl"
      case x if x < 0.70 => "after_meals"
      case x if x < 0.85 => "weekend_warrior"
      case _ => "random"
    }
    
    archetype match {
      case "working_professional" =>
        CustomerTimePreferences(
          preferredDays = Set.empty, // No strong day preference
          avoidedDays = Set.empty,
          preferredHours = Set(6, 7, 8, 18, 19, 20, 21, 22), // Before work and after work
          avoidedHours = Set(9, 10, 11, 12, 13, 14, 15, 16, 17), // Working hours
          preferAfterMeals = false,
          isEarlyBird = false,
          isNightOwl = false,
          workingProfessional = true
        )
        
      case "early_bird" =>
        CustomerTimePreferences(
          preferredDays = Set(1, 2, 3, 4, 5), // Weekdays
          avoidedDays = Set.empty,
          preferredHours = Set(6, 7, 8, 9, 10, 11), // Morning hours
          avoidedHours = Set(20, 21, 22, 23, 0, 1), // Late night
          preferAfterMeals = false,
          isEarlyBird = true,
          isNightOwl = false,
          workingProfessional = false
        )
        
      case "night_owl" =>
        CustomerTimePreferences(
          preferredDays = Set(5, 6, 0), // Weekend + Friday
          avoidedDays = Set(1, 2), // Monday/Tuesday
          preferredHours = Set(19, 20, 21, 22, 23, 0), // Evening/night
          avoidedHours = Set(6, 7, 8, 9, 10), // Early morning
          preferAfterMeals = false,
          isEarlyBird = false,
          isNightOwl = true,
          workingProfessional = false
        )
        
      case "after_meals" =>
        CustomerTimePreferences(
          preferredDays = Set.empty,
          avoidedDays = Set.empty,
          preferredHours = Set(13, 14, 20, 21), // After lunch and dinner
          avoidedHours = Set(11, 12, 18, 19), // Meal times
          preferAfterMeals = true,
          isEarlyBird = false,
          isNightOwl = false,
          workingProfessional = false
        )
        
      case "weekend_warrior" =>
        CustomerTimePreferences(
          preferredDays = Set(6, 0), // Saturday, Sunday
          avoidedDays = Set(1), // Monday
          preferredHours = Set(10, 11, 12, 15, 16, 17), // Weekend leisure hours
          avoidedHours = Set.empty,
          preferAfterMeals = false,
          isEarlyBird = false,
          isNightOwl = false,
          workingProfessional = false
        )
        
      case _ => // Random pattern
        val numPreferredDays = random.nextInt(3)
        val numPreferredHours = 2 + random.nextInt(4)
        CustomerTimePreferences(
          preferredDays = random.shuffle((0 to 6).toList).take(numPreferredDays).toSet,
          avoidedDays = Set.empty,
          preferredHours = random.shuffle((0 to 23).toList).take(numPreferredHours).toSet,
          avoidedHours = Set.empty,
          preferAfterMeals = random.nextBoolean(),
          isEarlyBird = false,
          isNightOwl = false,
          workingProfessional = false
        )
    }
  }
  
  /**
   * Generate realistic weekly response patterns based on time preferences
   */
  private def generateWeeklyResponsePattern(
    baseRates: Array[Double], 
    timePrefs: CustomerTimePreferences, 
    valueMultiplier: Double,
    random: scala.util.Random
  ): Array[Array[Double]] = {
    
    // Generate for 168 hours (7 days × 24 hours)
    Array.tabulate(168) { hourSlot =>
      val dayOfWeek = hourSlot / 24
      val hourOfDay = hourSlot % 24
      
      // Calculate time preference multiplier
      var timeMultiplier = 1.0
      
      // Day preference effect
      if (timePrefs.preferredDays.contains(dayOfWeek)) {
        timeMultiplier *= 1.4
      } else if (timePrefs.avoidedDays.contains(dayOfWeek)) {
        timeMultiplier *= 0.6
      }
      
      // Hour preference effect
      if (timePrefs.preferredHours.contains(hourOfDay)) {
        timeMultiplier *= 1.5
      } else if (timePrefs.avoidedHours.contains(hourOfDay)) {
        timeMultiplier *= 0.4
      }
      
      // Special patterns
      if (timePrefs.preferAfterMeals && (hourOfDay == 13 || hourOfDay == 14 || hourOfDay == 20 || hourOfDay == 21)) {
        timeMultiplier *= 1.3
      }
      
      if (timePrefs.isEarlyBird && hourOfDay >= 6 && hourOfDay <= 10) {
        timeMultiplier *= 1.3
      }
      
      if (timePrefs.isNightOwl && (hourOfDay >= 20 || hourOfDay <= 1)) {
        timeMultiplier *= 1.3
      }
      
      if (timePrefs.workingProfessional && dayOfWeek >= 1 && dayOfWeek <= 5 && hourOfDay >= 9 && hourOfDay <= 17) {
        timeMultiplier *= 0.5 // Low response during working hours
      }
      
      // Apply general weekly patterns
      val weekdayMultiplier = if (dayOfWeek >= 1 && dayOfWeek <= 5) 1.1 else 0.9 // Slight weekday preference
      val businessHourMultiplier = if (hourOfDay >= 9 && hourOfDay <= 17) 1.0 else 0.8 // Slight business hour preference
      
      timeMultiplier *= weekdayMultiplier * businessHourMultiplier
      
      // Generate response rates for each channel with sophisticated channel-specific patterns
      baseRates.zipWithIndex.map { case (baseRate, channel) =>
        // Channel-specific time adjustments
        val channelTimeMultiplier = channel match {
          case 0 => // Email - consistent throughout the day, peaks in morning and evening
            if (hourOfDay >= 8 && hourOfDay <= 10) 1.2
            else if (hourOfDay >= 17 && hourOfDay <= 19) 1.3
            else if (hourOfDay >= 0 && hourOfDay <= 6) 0.5
            else 1.0
            
          case 1 => // SMS - best during waking hours, avoid very late/early
            if (hourOfDay >= 8 && hourOfDay <= 20) 1.1
            else if (hourOfDay >= 21 && hourOfDay <= 23) 0.8
            else 0.4 // Very low for late night/early morning
            
          case 2 => // Push notification - best during active phone usage
            if (hourOfDay >= 7 && hourOfDay <= 9) 1.3 // Morning commute
            else if (hourOfDay >= 12 && hourOfDay <= 14) 1.1 // Lunch break
            else if (hourOfDay >= 17 && hourOfDay <= 22) 1.4 // Evening usage
            else if (hourOfDay >= 0 && hourOfDay <= 6) 0.3 // Sleep hours
            else 0.9
            
          case 3 => // In-app - depends on app usage patterns
            if (dayOfWeek == 0 || dayOfWeek == 6) { // Weekends
              if (hourOfDay >= 10 && hourOfDay <= 23) 1.2 else 0.7
            } else { // Weekdays
              if (hourOfDay >= 7 && hourOfDay <= 9) 1.1 // Morning routine
              else if (hourOfDay >= 12 && hourOfDay <= 13) 1.0 // Lunch
              else if (hourOfDay >= 18 && hourOfDay <= 23) 1.3 // Evening leisure
              else if (hourOfDay >= 9 && hourOfDay <= 17) 0.6 // Work hours
              else 0.4
            }
            
          case _ => 1.0
        }
        
        // Combine all multipliers
        val variation = 0.8 + random.nextDouble() * 0.4 // ±20% random variation
        val finalRate = baseRate * timeMultiplier * channelTimeMultiplier * valueMultiplier * variation
        Math.max(0.001, Math.min(0.95, finalRate))
      }
    }
  }
  
  /**
   * Load customers from a data source (for real-world usage)
   * This is a placeholder for actual data loading implementation
   */
  def loadFromDataSource(dataPath: String): Array[Customer] = {
    // In a real implementation, this would load from:
    // - Database query
    // - CSV/Parquet files
    // - API endpoints
    // - Historical campaign data
    
    // For now, return sample data
    generateRandomCustomers(1000) // Sample 1K customers
  }
  
  /**
   * Calculate statistics for a customer array
   */
  def getStatistics(customers: Array[Customer]): CustomerStatistics = {
    val totalCustomers = customers.length
    val avgResponseRates = Array.ofDim[Double](4)
    
    for (channel <- 0 until 4) {
      avgResponseRates(channel) = customers.map(_.getAverageResponseRate(channel)).sum / totalCustomers
    }
    
    // Calculate business value statistics
    val avgArpu = customers.map(_.arpu).sum / totalCustomers
    val avgLtv = customers.map(_.lifetimeValue).sum / totalCustomers
    val avgConversionValue = customers.map(_.conversionValue).sum / totalCustomers
    val tierDistribution = customers.groupBy(_.tier).mapValues(_.length)
    
    CustomerStatistics(
      totalCustomers = totalCustomers,
      averageResponseRates = avgResponseRates,
      totalPossibleContacts = totalCustomers * 60L * 4L, // customers * timeSlots * channels
      averageArpu = avgArpu,
      averageLtv = avgLtv,
      averageConversionValue = avgConversionValue,
      tierDistribution = tierDistribution
    )
  }
}

/**
 * Statistics about customer data
 */
case class CustomerStatistics(
  totalCustomers: Int,
  averageResponseRates: Array[Double],
  totalPossibleContacts: Long,
  averageArpu: Double,
  averageLtv: Double,
  averageConversionValue: Double,
  tierDistribution: Map[Int, Int]
) {
  override def toString: String = {
    val premiumCount = tierDistribution.getOrElse(3, 0)
    val standardCount = tierDistribution.getOrElse(2, 0)
    val basicCount = tierDistribution.getOrElse(1, 0)
    
    s"""Customer Statistics:
       |  Total customers: $totalCustomers
       |  Average response rates by channel:
       |    Channel 0 (Email): ${averageResponseRates(0) * 100}%
       |    Channel 1 (SMS): ${averageResponseRates(1) * 100}%
       |    Channel 2 (Push): ${averageResponseRates(2) * 100}%
       |    Channel 3 (In-app): ${averageResponseRates(3) * 100}%
       |  Business value metrics:
       |    Average ARPU: $$${averageArpu}%.2f/month
       |    Average LTV: $$${averageLtv}%.2f
       |    Average conversion value: $$${averageConversionValue}%.2f
       |  Customer tiers:
       |    Premium ($premiumCount customers): ${premiumCount * 100.0 / totalCustomers}%.1f%%
       |    Standard ($standardCount customers): ${standardCount * 100.0 / totalCustomers}%.1f%%
       |    Basic ($basicCount customers): ${basicCount * 100.0 / totalCustomers}%.1f%%
       |  Total possible contacts: $totalPossibleContacts""".stripMargin
  }
} 