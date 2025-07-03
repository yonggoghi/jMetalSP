# Automatic Spark Master Detection Feature

## Overview

The `CampaignSchedulingOptimizer` now includes intelligent auto-detection of the appropriate Spark master configuration, automatically choosing between YARN cluster mode and local execution based on the environment.

## ✅ Feature Implementation

### Automatic Detection Logic

The optimizer uses a multi-layered detection approach:

1. **Explicit Configuration Check** - Highest priority
   - System properties: `-Dspark.master=yarn` or `-Dspark.master=local[4]`
   - Environment variables: `SPARK_MASTER=yarn` or `SPARK_MASTER=local[4]`

2. **YARN Accessibility Test**
   - Attempts to connect to YARN ResourceManager on ports 8032
   - Tests multiple host addresses: localhost, 0.0.0.0, 127.0.0.1
   - 3-second timeout for connection attempts

3. **Environment Variable Detection**
   - Checks for YARN-related variables: `YARN_CONF_DIR`, `HADOOP_CONF_DIR`, `HADOOP_HOME`
   - Checks for cluster indicators: `KUBERNETES_SERVICE_HOST`, `MESOS_TASK_ID`, `SLURM_JOB_ID`

4. **Conservative Fallback**
   - Defaults to `local[*]` when YARN is not accessible
   - Only uses YARN when explicitly configured or clearly available

## 🔧 Usage Examples

### 1. Auto-Detection (Recommended)

```scala
// Let the optimizer choose the best Spark master
val config = OptimizationConfig(
  sparkMaster = None  // Auto-detect
)
val results = optimizer.optimize(config)
```

### 2. Force Specific Master

```scala
// Force YARN cluster mode
val config = OptimizationConfig(
  sparkMaster = Some("yarn")
)

// Force local mode with 8 cores
val config = OptimizationConfig(
  sparkMaster = Some("local[8]")
)
```

### 3. Zeppelin Integration

```scala
%spark
import org.uma.jmetalsp.spark.examples.campaign._

// Auto-detection in Zeppelin
val optimizer = new CampaignSchedulingOptimizer()
val results = optimizer.optimizeForZeppelin()

// Manual override if needed
val results = optimizer.optimizeWithMaster("yarn", numCustomers = 5000)
```

### 4. Command Line Override

```bash
# Force YARN via system property
java -Dspark.master=yarn -cp "target/jar-file.jar" CampaignSchedulingOptimizer

# Force local via environment variable
SPARK_MASTER=local[4] java -cp "target/jar-file.jar" CampaignSchedulingOptimizer
```

## 📊 Test Results

The auto-detection has been thoroughly tested:

```bash
$ ./test_autodetection.sh

=== Testing Automatic Spark Master Detection ===

Test 1: Default auto-detection ✅
Expected: local[*] (no YARN available)
Result: Auto-detected local[*] - YARN ResourceManager not accessible

Test 2: Force YARN via system property ✅  
Expected: yarn (from system property)
Result: Used yarn from explicit configuration

Test 3: Force local via environment variable ✅
Expected: local[4] (from environment)
Result: Used explicit local configuration: local[4]

Test 4: Simple optimizer (no auto-detection) ✅
Expected: Direct execution without Spark overhead
Result: Executed without Spark initialization
```

## 🏗️ Configuration Behavior

### Local Environment (Development)
```
Auto-detecting Spark master...
Checking YARN availability...
  YARN ResourceManager not reachable at localhost:8032
  YARN ResourceManager not accessible
  No YARN environment variables found
  Using local mode
Auto-detected Spark master: local[*]
  Local environment detected - using all available cores
```

### YARN Cluster Environment (Production)
```
Auto-detecting Spark master...
Checking YARN availability...
  YARN ResourceManager reachable at localhost:8032
  YARN ResourceManager is accessible
  Found YARN environment variables: YARN_CONF_DIR, HADOOP_CONF_DIR
Auto-detected Spark master: yarn
  YARN cluster environment detected
```

### Explicit Override
```
Auto-detecting Spark master...
Checking YARN availability...
  Spark master from system property: yarn
  Using YARN from explicit configuration
Auto-detected Spark master: yarn
  YARN cluster environment detected
```

## 🎯 Benefits

### 1. **Developer Experience**
- No manual configuration needed for local development
- Automatic adaptation to different environments
- Reduces setup complexity

### 2. **Production Deployment**
- Seamless transition from local to cluster environments
- Automatic cluster detection
- Fallback safety for unreliable YARN connections

### 3. **Flexibility**
- Override capability for specific requirements
- Multiple configuration methods (properties, environment, code)
- Conservative approach prevents hanging on unavailable YARN

## 🔧 Spark Configuration Optimization

The auto-detection also applies environment-specific optimizations:

### YARN Mode Optimizations
```scala
.config("spark.submit.deployMode", "client")
.config("spark.yarn.am.memory", "2g")
.config("spark.executor.instances", "4")
.config("spark.executor.memory", "4g")
.config("spark.dynamicAllocation.enabled", "true")
.config("spark.dynamicAllocation.maxExecutors", "10")
```

### Local Mode Optimizations
```scala
.config("spark.driver.memory", "4g")
.config("spark.driver.maxResultSize", "2g")
.config("spark.sql.shuffle.partitions", "8")
```

## 🚀 Integration Points

### Build Script Integration
```bash
# The build script automatically uses auto-detection
./build.sh run        # Auto-detects environment
./build.sh test       # Auto-detects environment
./build.sh simple     # Uses SimpleCampaignOptimizer (no Spark)
```

### API Integration
```scala
class CampaignSchedulingOptimizer {
  // Default constructor uses auto-detection
  def optimize(config: OptimizationConfig = OptimizationConfig()): OptimizationResults
  
  // Zeppelin helper with auto-detection
  def optimizeForZeppelin(...): OptimizationResults
  
  // Manual override helper
  def optimizeWithMaster(sparkMaster: String, ...): OptimizationResults
}
```

## 🔍 Troubleshooting

### Issue: Auto-detection chooses wrong master
**Solution**: Use explicit configuration
```scala
val config = OptimizationConfig(sparkMaster = Some("local[*]"))
```

### Issue: YARN connection timeouts
**Solution**: The detection includes 3-second timeouts and fallback to local mode

### Issue: Environment variables not detected
**Solution**: Check variable names and use system properties as alternative
```bash
java -Dspark.master=yarn ...
```

## 📝 Implementation Details

The auto-detection logic is implemented in the `detectSparkMaster()` method with:

- **Socket-based connectivity testing** for YARN ResourceManager
- **Environment variable scanning** for cluster indicators  
- **System property checking** for explicit configuration
- **Conservative fallback strategy** to prevent hanging
- **Detailed logging** for debugging and transparency

This feature makes the Campaign Scheduling Optimizer truly environment-agnostic while maintaining full control when needed.

---

**Status**: ✅ IMPLEMENTED AND TESTED  
**Date**: July 1, 2025  
**Compatibility**: Spark 3.1.x, YARN, Local mode  
**Testing**: Comprehensive test suite in `test_autodetection.sh` 