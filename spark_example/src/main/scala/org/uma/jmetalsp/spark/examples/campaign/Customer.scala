package org.uma.jmetalsp.spark.examples.campaign

import scala.util.Random

/**
 * Represents a customer in the campaign scheduling problem
 * 
 * @param id Customer unique identifier
 * @param responseRates Matrix of response rates [timeSlot][channel] -> probability
 * @param lastContactTime Last time this customer was contacted (in time slot units)
 * @param preferredChannels Preferred communication channels for this customer
 */
case class Customer(
  id: Long,
  responseRates: Array[Array[Double]], // [60 time slots][4 channels]
  lastContactTime: Int = -48, // Initialize to allow immediate contact
  preferredChannels: Set[Int] = Set.empty
) {
  
  /**
   * Get response rate for a specific time slot and channel
   */
  def getResponseRate(timeSlot: Int, channel: Int): Double = {
    if (timeSlot >= 0 && timeSlot < responseRates.length && 
        channel >= 0 && channel < responseRates(timeSlot).length) {
      responseRates(timeSlot)(channel)
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
    responseRates(timeSlot).zipWithIndex.maxBy(_._1)._2
  }
  
  /**
   * Get average response rate across all time slots for a channel
   */
  def getAverageResponseRate(channel: Int): Double = {
    responseRates.map(_(channel)).sum / responseRates.length
  }
}

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
    timeSlots: Int = 60,
    channels: Int = 4,
    seed: Long = 42L
  ): Array[Customer] = {
    
    val random = new Random(seed)
    
    (0 until numCustomers).map { customerId =>
      // Generate response rates with some realistic patterns
      val responseRates = Array.ofDim[Double](timeSlots, channels)
      
      // Each customer has different base response rates for each channel
      val channelBaselines = Array.fill(channels)(0.05 + random.nextDouble() * 0.15) // 5-20% base
      
      // Customer behavior patterns
      val isWeekendPreferrer = random.nextBoolean()
      val isPeakHourPreferrer = random.nextBoolean()
      val preferredChannel = random.nextInt(channels)
      
      for {
        timeSlot <- 0 until timeSlots
        channel <- 0 until channels
      } {
        var responseRate = channelBaselines(channel)
        
        // Time-based adjustments
        val hourOfWeek = timeSlot % 168 // 168 hours per week
        val dayOfWeek = hourOfWeek / 24
        val hourOfDay = hourOfWeek % 24
        
        // Weekend effect
        if (isWeekendPreferrer && (dayOfWeek == 5 || dayOfWeek == 6)) {
          responseRate *= 1.3
        } else if (!isWeekendPreferrer && (dayOfWeek == 5 || dayOfWeek == 6)) {
          responseRate *= 0.7
        }
        
        // Peak hours effect (9-17)
        if (isPeakHourPreferrer && hourOfDay >= 9 && hourOfDay <= 17) {
          responseRate *= 1.2
        } else if (!isPeakHourPreferrer && (hourOfDay < 9 || hourOfDay > 17)) {
          responseRate *= 1.2
        }
        
        // Preferred channel bonus
        if (channel == preferredChannel) {
          responseRate *= 1.4
        }
        
        // Channel-specific adjustments
        channel match {
          case 0 => // Email - consistent throughout the day
            responseRate *= 1.0
          case 1 => // SMS - better during day hours
            if (hourOfDay >= 8 && hourOfDay <= 20) responseRate *= 1.1 else responseRate *= 0.8
          case 2 => // Push notification - better during active hours
            if (hourOfDay >= 7 && hourOfDay <= 22) responseRate *= 1.2 else responseRate *= 0.6
          case 3 => // In-app - depends on app usage patterns
            if (hourOfDay >= 19 && hourOfDay <= 23) responseRate *= 1.3 else responseRate *= 0.9
        }
        
        // Add some noise and ensure bounds
        responseRate += (random.nextGaussian() * 0.01) // Small noise
        responseRate = Math.max(0.001, Math.min(0.8, responseRate)) // Clamp between 0.1% and 80%
        
        responseRates(timeSlot)(channel) = responseRate
      }
      
      // Create preferred channels set (1-2 channels typically)
      val numPreferred = 1 + random.nextInt(2)
      val preferredChannels = random.shuffle((0 until channels).toList).take(numPreferred).toSet
      
      Customer(
        id = customerId.toLong,
        responseRates = responseRates,
        preferredChannels = preferredChannels
      )
    }.toArray
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
    
    CustomerStatistics(
      totalCustomers = totalCustomers,
      averageResponseRates = avgResponseRates,
      totalPossibleContacts = totalCustomers * 60L * 4L // customers * timeSlots * channels
    )
  }
}

/**
 * Statistics about customer data
 */
case class CustomerStatistics(
  totalCustomers: Int,
  averageResponseRates: Array[Double],
  totalPossibleContacts: Long
) {
  override def toString: String = {
    s"""Customer Statistics:
       |  Total customers: $totalCustomers
       |  Average response rates by channel:
       |    Channel 0 (Email): ${averageResponseRates(0) * 100}%
       |    Channel 1 (SMS): ${averageResponseRates(1) * 100}%
       |    Channel 2 (Push): ${averageResponseRates(2) * 100}%
       |    Channel 3 (In-app): ${averageResponseRates(3) * 100}%
       |  Total possible contacts: $totalPossibleContacts""".stripMargin
  }
} 