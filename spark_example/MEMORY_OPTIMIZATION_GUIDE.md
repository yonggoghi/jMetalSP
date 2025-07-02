# Memory Optimization Guide for Large-Scale Campaign Optimization

## 🚨 **Critical Issues Addressed**

Based on your error logs showing **571MB task size** and **OutOfMemoryError**, here are the immediate fixes:

### **Problem 1: Task Size Too Large (571MB > 1000KB limit)**
```
TaskSetManager: Stage contains a task of very large size (571633 KiB)
```

**Root Cause**: Customer data arrays being serialized and sent to each task
**Solution**: Implemented smaller batch processing and metadata-only caching

### **Problem 2: Java Heap Space OutOfMemoryError**
```
java.lang.OutOfMemoryError: Java heap space
```

**Root Cause**: Insufficient memory allocation and poor garbage collection
**Solution**: Enhanced memory configurations and Kryo serialization

### **Problem 3: Kryo Serialization Buffer Overflow**
```
KryoSerializationStream.close(KryoSerializer.scala:273)
```

**Root Cause**: Default Kryo buffers too small for large objects
**Solution**: Increased buffer sizes to 2GB max

## ⚡ **Immediate Fixes Applied**

### **1. Enhanced Spark Configuration**
```scala
// CRITICAL memory and serialization settings
.config("spark.kryoserializer.buffer.max", "2047m")  // Must be < 2048MB
.config("spark.kryoserializer.buffer", "256m")       // 256MB initial
.config("spark.serializer.objectStreamReset", "100")
.config("spark.task.maxDirectResultSize", "1048576") // 1MB max direct result
.config("spark.rpc.message.maxSize", "512")          // 512MB max RPC
```

### **2. Optimized Customer Data Processing**
```scala
// OLD: Large parallel batches causing memory issues
val customers = (0 until numBatches).par.flatMap { ... }

// NEW: Sequential smaller batches with memory control
val safeBatchSize = Math.min(config.customerBatchSize, 10000) // Max 10K
val customers = (0 until numBatches).flatMap { ... }
```

### **3. Memory-Efficient Metadata Storage**
```scala
// Store only essential metadata, not full customer objects
val customerMetadata = customers.map { c =>
  (c.id, c.arpu, c.tier, c.lifetimeValue, c.getBusinessPriority)
}
```

## 🎯 **Production Configuration for 1M Customers**

### **Cluster Resource Allocation**
```bash
# For 100 nodes × 39GB RAM × 10 cores
--num-executors 200 \
--executor-cores 4 \
--executor-memory 28g \
--executor-memoryOverhead 4g \
--driver-memory 8g \
--driver-memoryOverhead 2g
```

### **Memory Distribution Per Executor**
```
Total Executor Memory: 32GB (28GB + 4GB overhead)
├── JVM Heap: 22.4GB (80% of 28GB)
│   ├── Storage (cached data): ~11GB
│   └── Execution (computation): ~11GB
├── Off-heap overhead: 4GB
└── Reserved: 1.6GB
```

### **Serialization Optimization**
```bash
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.unsafe=true \
--conf spark.kryo.referenceTracking=false \
--conf spark.kryoserializer.buffer.max=2047m \
--conf spark.serializer.objectStreamReset=100
```

## 🔧 **Usage Examples**

### **Safe Test Run (100K customers)**
```bash
./run_large_scale_optimization.sh 100000 200 10000 0.3
```

### **Production Run (1M customers)**
```bash
./run_large_scale_optimization.sh 1000000 500 50000 0.3
```

### **Memory-Constrained Run**
```bash
spark-submit \
  --num-executors 150 \
  --executor-memory 24g \
  --executor-memoryOverhead 6g \
  --class CampaignSchedulingOptimizer app.jar \
  --num-customers 500000 \
  --customer-batch-size 5000 \
  --population-size 300
```

## 🔍 **Troubleshooting Common Issues**

### **Issue: "Task of very large size" Warning**
```
TaskSetManager: Stage contains a task of very large size (XXX KiB)
```

**Solutions:**
1. Reduce `--customer-batch-size` (try 5000 or 2500)
2. Increase `spark.rpc.message.maxSize` to 1024
3. Use `spark.task.maxDirectResultSize=2097152` (2MB)

### **Issue: OutOfMemoryError in Driver**
```
java.lang.OutOfMemoryError: Java heap space (Driver)
```

**Solutions:**
1. Increase `--driver-memory` to 12g or 16g
2. Set `spark.driver.maxResultSize=8g`
3. Reduce result collection size

### **Issue: OutOfMemoryError in Executors**
```
java.lang.OutOfMemoryError: Java heap space (Executor)
```

**Solutions:**
1. Increase `--executor-memory` (try 32g if available)
2. Reduce `--num-customers` temporarily
3. Increase `--executor-memoryOverhead` to 6g

### **Issue: Kryo Buffer Overflow**
```
Buffer overflow. Available: 0, required: XXX
```

**Solutions:**
1. Increase `spark.kryoserializer.buffer.max` to 2047m (max allowed)
2. Set `spark.kryoserializer.buffer` to 512m
3. Enable `spark.kryo.unsafe=true`

### **Issue: Network Timeouts**
```
java.util.concurrent.TimeoutException
```

**Solutions:**
1. Increase all timeout values:
   ```bash
   --conf spark.network.timeout=1800s \
   --conf spark.rpc.askTimeout=900s \
   --conf spark.executor.heartbeatInterval=120s
   ```

## 📊 **Performance Monitoring**

### **Key Metrics to Watch in Spark UI**

1. **Task Metrics**
   - Task serialization time < 100ms
   - Task deserialization time < 50ms
   - GC time < 10% of task time

2. **Memory Usage**
   - Storage memory utilization: 60-80%
   - Execution memory utilization: 60-80%
   - Off-heap usage < 90%

3. **Network**
   - Shuffle read/write < 20% of input data
   - Network I/O time < 15% of total time

### **Spark UI Sections to Monitor**
- **Jobs**: Look for failed tasks and long-running stages
- **Stages**: Check task distribution and skew
- **Storage**: Monitor cached data and memory usage
- **Executors**: Watch GC time and memory consumption

## 🎛️ **Adaptive Configuration**

### **For Different Customer Counts**

| Customers | Batch Size | Population | Evaluations | Memory | Notes |
|-----------|------------|------------|-------------|---------|-------|
| 100K      | 10,000     | 200        | 10,000      | 28g     | Safe test |
| 500K      | 8,000      | 300        | 20,000      | 32g     | Moderate scale |
| 1M        | 5,000      | 500        | 50,000      | 36g     | Full production |
| 2M+       | 2,500      | 300        | 30,000      | 36g     | Memory-optimized |

### **For Different Cluster Sizes**

| Nodes | Executors | Cores/Exec | Memory/Exec | Total Memory |
|-------|-----------|------------|-------------|--------------|
| 50    | 100       | 4          | 28g         | 2.8TB        |
| 100   | 200       | 4          | 28g         | 5.6TB        |
| 200   | 400       | 3          | 24g         | 9.6TB        |

## 🚀 **Best Practices**

### **1. Start Small and Scale Up**
```bash
# Test with small dataset first
./run_large_scale_optimization.sh 10000 50 1000 0.3

# Then scale gradually
./run_large_scale_optimization.sh 100000 200 10000 0.3
./run_large_scale_optimization.sh 1000000 500 50000 0.3
```

### **2. Monitor Resource Usage**
```bash
# Check YARN resource manager
yarn application -list
yarn logs -applicationId application_xxx

# Monitor Spark UI
# http://your-cluster:8088/proxy/application_xxx/
```

### **3. Optimize Based on Results**
- If GC time > 10%: Increase memory or reduce batch size
- If network I/O high: Reduce shuffle operations
- If tasks fail: Check serialization and timeout settings

### **4. Checkpointing Strategy**
```scala
// Enable checkpointing for long-running jobs
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
// Checkpoint every 100 generations to prevent data loss
```

## 🆘 **Emergency Fixes**

If your job is failing immediately:

1. **Reduce scale drastically**:
   ```bash
   --num-customers 50000 --customer-batch-size 2500
   ```

2. **Use conservative memory settings**:
   ```bash
   --executor-memory 20g --executor-memoryOverhead 8g
   ```

3. **Disable advanced features temporarily**:
   ```bash
   --conf spark.sql.adaptive.enabled=false
   --disable-checkpointing
   ```

4. **Use local mode for debugging**:
   ```bash
   --master local[8] --driver-memory 16g
   ```

This guide should resolve the memory and serialization issues you're experiencing with large-scale optimization runs. 