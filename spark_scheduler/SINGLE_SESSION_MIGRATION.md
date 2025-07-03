# Single Spark Session Migration Guide

## Overview

The Scala code in the `spark_scheduler` directory has been refactored to use a **single Spark session pattern** instead of creating multiple sessions. This prevents session conflicts, resource leaks, and improves performance.

## Changes Made

### 1. CampaignSchedulingOptimizer.scala

**New Methods:**
- `optimizeWithSpark(spark: SparkSession, config)` - **Primary method** that accepts existing SparkSession
- `optimize(config)` - Legacy method that creates its own session (for standalone use)
- `optimizeForZeppelin(spark: SparkSession, ...)` - Convenience method for Zeppelin

**Key Changes:**
- Moved session creation to `createSparkSession()` helper method
- `optimizeWithSpark()` no longer creates or stops sessions
- Session lifecycle managed externally

### 2. SimpleCampaignOptimizer.scala

**New Methods:**
- `runSparkOptimizationWithSession(spark: SparkSession)` - **Primary method** that accepts existing SparkSession
- `runSparkOptimizationStandalone()` - Creates its own session (for standalone use)
- `optimizeForZeppelin(spark: SparkSession, ...)` - Convenience method for Zeppelin
- `createSparkSession(masterUrl)` - Helper to create sessions

**Key Changes:**
- Separated session creation from optimization logic
- `runSparkOptimizationWithSession()` accepts existing session
- Simplified configuration for Zeppelin usage

## Usage Patterns

### ✅ Recommended: Use Existing Session

```scala
// In Zeppelin/Jupyter notebooks
import org.uma.jmetalsp.spark.examples.campaign._

// Simple optimization
SimpleCampaignOptimizer.optimizeForZeppelin(spark)

// Full optimization
val optimizer = new CampaignSchedulingOptimizer()
val results = optimizer.optimizeWithSpark(spark)

// With custom configuration
val results = optimizer.optimizeWithSpark(spark, config)
```

### ✅ Standalone Usage (Creates Own Session)

```scala
// For standalone applications
val optimizer = new CampaignSchedulingOptimizer()
val results = optimizer.optimize() // Creates and manages session

// Or using main method
CampaignSchedulingOptimizer.main(Array())
SimpleCampaignOptimizer.main(Array("--spark"))
```

### ❌ Deprecated: Multiple Session Creation

```scala
// Don't do this - creates multiple sessions
val optimizer1 = new CampaignSchedulingOptimizer()
val results1 = optimizer1.optimize()  // Creates session 1

val optimizer2 = new CampaignSchedulingOptimizer() 
val results2 = optimizer2.optimize()  // Creates session 2 - CONFLICT!
```

## Benefits

1. **No Session Conflicts**: Only one active session at a time
2. **Resource Efficiency**: Reuses existing Spark context and resources
3. **Zeppelin Compatible**: Works seamlessly with existing notebook sessions
4. **Flexibility**: Choose between using existing session or creating new one
5. **Performance**: Avoids session startup/shutdown overhead

## Session Lifecycle

### With Existing Session (Recommended)
```
User creates session → Use optimizeWithSpark() → User manages session lifecycle
```

### Standalone Mode
```
optimize() creates session → Runs optimization → Automatically stops session
```

## Migration Examples

### Before (Multiple Sessions)
```scala
// Old code that could create conflicts
val optimizer1 = new CampaignSchedulingOptimizer()
val results1 = optimizer1.optimize()

val optimizer2 = new CampaignSchedulingOptimizer()
val results2 = optimizer2.optimize()
```

### After (Single Session)
```scala
// New code using single session
val optimizer = new CampaignSchedulingOptimizer()

// Option 1: Let optimizer manage session
val results1 = optimizer.optimize(config1)
val results2 = optimizer.optimize(config2)  // Creates new session each time

// Option 2: Manage session yourself (more efficient)
val spark = optimizer.createSparkSession(OptimizationConfig())
try {
  val results1 = optimizer.optimizeWithSpark(spark, config1)
  val results2 = optimizer.optimizeWithSpark(spark, config2)  // Reuses session
} finally {
  spark.stop()
}

// Option 3: In Zeppelin (best)
val results1 = optimizer.optimizeWithSpark(spark, config1)  // Uses existing session
val results2 = optimizer.optimizeWithSpark(spark, config2)  // Reuses same session
```

## Testing

All existing functionality remains available, but the recommended usage pattern has changed:

```bash
# Test standalone mode
mvn test

# Test Spark mode
java -cp target/*jar* org.uma.jmetalsp.spark.examples.campaign.SimpleCampaignOptimizer --spark
```

## Compatibility

- ✅ Fully backward compatible for standalone usage
- ✅ Zeppelin/Jupyter notebooks work better than before
- ✅ All configuration options preserved
- ✅ All output formats unchanged
- ⚠️ Code using internal methods may need updates

## Summary

The refactoring provides a clean separation between session management and optimization logic, making the code more robust and suitable for both interactive (Zeppelin) and batch (spark-submit) usage patterns. 