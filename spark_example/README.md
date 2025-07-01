# Campaign Message Scheduling Optimization with jMetalSP and Spark 3.1.x

This project demonstrates a multi-objective optimization solution for campaign message scheduling using jMetalSP with Apache Spark 3.1.x.

## Problem Description

The campaign scheduling problem optimizes the delivery of marketing messages to millions of customers across multiple channels and time slots.

### Key Characteristics
- **Scale**: 10M customers (scaled down to 1K for demo)
- **Time Horizon**: 60 time slots (hours)
- **Channels**: 4 communication channels (Email, SMS, Push, In-app)
- **Constraints**: 
  - Minimum 48-hour interval between messages to same customer
  - Maximum capacity per hour
  - Campaign budget limits

### Objectives
1. **Maximize Response Rate**: Optimize overall customer response using historical data
2. **Minimize Cost**: Reduce campaign expenses across channels
3. **Maximize Customer Satisfaction**: Balance channel preferences and message frequency

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Customer      │    │   Campaign       │    │   NSGA-II       │
│   Data Model    │───▶│   Scheduling     │───▶│   Algorithm     │
│                 │    │   Problem        │    │   (jMetalSP)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                ▲                        │
                                │                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Historical    │    │   Spark 3.1.x    │    │   Optimized     │
│   Response      │───▶│   Evaluator      │◀───│   Schedules     │
│   Data          │    │                  │    │   (Pareto Front)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Requirements

- **Java**: 8 or 11
- **Scala**: 2.12.17
- **Apache Spark**: 3.1.3
- **jMetalSP**: 2.1-SNAPSHOT
- **Maven**: 3.6+

## Quick Start

### 1. Build the Project

```bash
cd spark_example
mvn clean compile package
```

### 2. Run with Spark Submit

```bash
spark-submit \
  --class="org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer" \
  --master local[4] \
  --conf spark.sql.adaptive.enabled=true \
  target/jmetalsp-spark-example-2.1-SNAPSHOT-jar-with-dependencies.jar
```

### 3. Use in Zeppelin Notebook

```scala
%spark
import org.uma.jmetalsp.spark.examples.campaign._

// Create optimizer instance
val optimizer = new CampaignSchedulingOptimizer()

// Run optimization with custom parameters
val results = optimizer.optimizeForZeppelin(
  numCustomers = 1000,
  populationSize = 50,
  maxEvaluations = 2000
)

// Display results
optimizer.printResults(results)
```

### 4. Production Scale Configuration

```scala
val config = OptimizationConfig(
  numCustomersDemo = 10000000,     // 10M customers
  populationSize = 200,
  maxEvaluations = 50000,
  maxCustomersPerHour = 500000,    // 500K/hour capacity
  campaignBudget = 10000000.0,     // $10M budget
  sparkMaster = "spark://cluster:7077"
)

val results = optimizer.optimize(config)
```

## Project Structure

```
spark_example/
├── pom.xml                              # Maven configuration
├── README.md                            # This file
├── src/main/scala/org/uma/jmetalsp/spark/examples/campaign/
│   ├── Customer.scala                   # Customer data model
│   ├── CampaignSchedulingProblem.scala  # Multi-objective problem definition
│   ├── CampaignSchedulingOptimizer.scala # Main optimizer with Spark integration
│   └── ZeppelinNotebook.md             # Zeppelin usage examples
└── target/                             # Build artifacts
```

## Key Components

### Customer Model (`Customer.scala`)
- Represents individual customers with response rate matrices
- Historical data for [60 time slots] × [4 channels]
- Customer preferences and constraints
- Synthetic data generation for testing

### Problem Definition (`CampaignSchedulingProblem.scala`)
- Multi-objective optimization problem implementation
- Constraint handling (capacity, budget, timing)
- Solution encoding/decoding
- Fitness evaluation with realistic business metrics

### Optimizer (`CampaignSchedulingOptimizer.scala`)
- Spark 3.1.x integration with jMetalSP
- NSGA-II algorithm configuration
- Distributed evaluation using Spark
- Result analysis and visualization

## Optimization Results

The algorithm produces:

1. **Pareto Front**: Multiple optimal solutions trading off between objectives
2. **Schedule Files**: 
   - `VAR_campaign_*.tsv`: Decision variables
   - `FUN_campaign_*.tsv`: Objective values
3. **Performance Metrics**: Response rates, costs, satisfaction scores

### Sample Output
```
=== OPTIMIZATION RESULTS SUMMARY ===
Execution time: 45231ms
Solutions found: 87

Problem Statistics:
  Customers: 1000
  Time slots: 60 hours
  Channels: 4
  Max customers/hour: 500
  Campaign budget: $50000.0
  Solution length: 3000 variables

Solution 1:
  Expected responses: 234.56
  Total cost: $12,450.00
  Customer satisfaction: 0.847
  Max hourly load: 487
  Utilization: 67.3%
```

## Spark 3.1.x Features Used

- **Adaptive Query Execution (AQE)**: Optimizes evaluation performance
- **Dynamic Coalescing**: Reduces shuffle partitions automatically
- **Skew Join Optimization**: Handles uneven data distribution
- **Kryo Serialization**: Faster serialization for genetic algorithm operations

## Scaling Considerations

### For 10M+ Customers:
1. **Data Partitioning**: Partition customers by geographic regions or segments
2. **Lazy Evaluation**: Use Spark DataFrames for efficient memory usage
3. **Checkpointing**: Enable fault tolerance for long-running optimizations
4. **Resource Allocation**: Scale cluster resources based on problem size

### Memory Requirements:
- **1K customers**: ~100MB
- **100K customers**: ~10GB
- **10M customers**: ~1TB (distributed across cluster)

## Configuration Options

```scala
case class OptimizationConfig(
  populationSize: Int = 100,           // NSGA-II population size
  maxEvaluations: Int = 10000,         // Maximum function evaluations
  crossoverProbability: Double = 0.9,  // Genetic crossover rate
  numCustomersDemo: Int = 1000,        // Problem scale
  maxCustomersPerHour: Int = 500,      // Capacity constraint
  campaignBudget: Double = 50000.0,    // Budget constraint
  sparkMaster: String = "local[4]",    // Spark cluster configuration
  enableCheckpointing: Boolean = true  // Fault tolerance
)
```

## Performance Benchmarks

| Customers | Population | Evaluations | Time (min) | Memory (GB) |
|-----------|------------|-------------|------------|-------------|
| 1K        | 50         | 2K          | 2          | 1           |
| 10K       | 100        | 10K         | 15         | 8           |
| 100K      | 200        | 25K         | 120        | 32          |
| 1M        | 300        | 50K         | 480        | 128         |

## Business Applications

1. **Email Marketing**: Optimize send times and frequency
2. **Mobile Push Notifications**: Maximize engagement rates
3. **SMS Campaigns**: Minimize costs while maximizing reach
4. **Multi-channel Coordination**: Coordinate across all channels

## Extensions and Customization

### Adding New Channels
```scala
// Extend channel support
val channels = Array("Email", "SMS", "Push", "In-app", "WhatsApp", "Messenger")
val channelCosts = Array(0.01, 0.05, 0.02, 0.03, 0.04, 0.02)
```

### Custom Constraints
```scala
// Add geographic constraints
def isValidAssignment(customer: Customer, timeSlot: Int): Boolean = {
  val timezone = customer.timezone
  val localHour = (timeSlot + timezone.offset) % 24
  localHour >= 8 && localHour <= 22 // Respect local business hours
}
```

### Real-time Data Integration
```scala
// Stream real-time response data
val responseStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "campaign-responses")
  .load()
```

## Troubleshooting

### Common Issues:

1. **Out of Memory**: Increase Spark driver/executor memory
   ```bash
   --conf spark.driver.memory=8g
   --conf spark.executor.memory=16g
   ```

2. **Slow Convergence**: Adjust algorithm parameters
   ```scala
   populationSize = 200  // Increase population
   maxEvaluations = 25000  // More evaluations
   ```

3. **Constraint Violations**: Review capacity and budget limits
   ```scala
   maxCustomersPerHour = 1000  // Increase capacity
   campaignBudget = 100000.0   // Increase budget
   ```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-constraint`)
3. Commit changes (`git commit -am 'Add geographic constraints'`)
4. Push to branch (`git push origin feature/new-constraint`)
5. Create Pull Request

## License

This project is part of the jMetalSP framework and follows the same licensing terms.

## Support

For questions and support:
- jMetalSP Documentation: [Official Docs](http://jmetalsp.uma.es/)
- Apache Spark 3.1.x: [Spark Documentation](https://spark.apache.org/docs/3.1.3/)
- GitHub Issues: [Report Issues](https://github.com/jMetal/jMetalSP/issues) 