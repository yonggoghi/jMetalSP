#!/bin/bash

# Campaign Scheduling Optimizer - Build and Run Script
# Usage: ./build.sh [command]
# Commands: build, run, clean, test, simple

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Configuration
MAIN_CLASS="org.uma.jmetalsp.spark.examples.campaign.CampaignSchedulingOptimizer"
JAR_NAME="jmetalsp-spark-scheduler-2.1-SNAPSHOT-jar-with-dependencies.jar"
SPARK_MASTER="local[4]"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_message() {
    echo -e "${2}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

print_success() {
    print_message "$1" "$GREEN"
}

print_warning() {
    print_message "$1" "$YELLOW"
}

print_error() {
    print_message "$1" "$RED"
}

print_info() {
    print_message "$1" "$BLUE"
}

# Check if Maven is installed
check_maven() {
    if ! command -v mvn &> /dev/null; then
        print_error "Maven is not installed. Please install Maven to continue."
        exit 1
    fi
    print_info "Maven version: $(mvn --version | head -1)"
}

# Check if Spark is available
check_spark() {
    if command -v spark-submit &> /dev/null; then
        print_info "Spark found: $(spark-submit --version 2>&1 | grep 'version' | head -1)"
    else
        print_warning "spark-submit not found in PATH. You can still build but may need to set SPARK_HOME for running."
    fi
}

# Build the project
build() {
    print_info "Building Campaign Scheduling Optimizer..."
    
    check_maven
    
    # Clean and compile
    print_info "Cleaning previous build..."
    mvn clean -q
    
    print_info "Compiling Scala sources..."
    mvn compile -q
    
    print_info "Running tests..."
    # mvn test -q  # Uncomment when tests are added
    
    print_info "Creating JAR with dependencies..."
    mvn package -q
    
    if [ -f "target/$JAR_NAME" ]; then
        print_success "Build completed successfully!"
        print_info "JAR location: target/$JAR_NAME"
        print_info "JAR size: $(du -h "target/$JAR_NAME" | cut -f1)"
    else
        print_error "Build failed - JAR not found"
        exit 1
    fi
}

# Run the optimizer
run() {
    print_info "Running Campaign Scheduling Optimizer..."
    
    if [ ! -f "target/$JAR_NAME" ]; then
        print_warning "JAR not found. Building first..."
        build
    fi
    
    check_spark
    
    print_info "Starting optimization with Spark..."
    print_info "Master: $SPARK_MASTER"
    print_info "Main class: $MAIN_CLASS"
    
    # Run with spark-submit
    spark-submit \
        --class "$MAIN_CLASS" \
        --master "$SPARK_MASTER" \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.driver.memory=2g \
        --conf spark.executor.memory=4g \
        --verbose \
        "target/$JAR_NAME"
}

# Run with custom configuration
run_custom() {
    print_info "Running with custom configuration..."
    
    if [ ! -f "target/$JAR_NAME" ]; then
        print_warning "JAR not found. Building first..."
        build
    fi
    
    # Parse custom arguments
    CUSTOM_MASTER=${2:-$SPARK_MASTER}
    DRIVER_MEMORY=${3:-"2g"}
    EXECUTOR_MEMORY=${4:-"4g"}
    
    print_info "Custom Master: $CUSTOM_MASTER"
    print_info "Driver Memory: $DRIVER_MEMORY"
    print_info "Executor Memory: $EXECUTOR_MEMORY"
    
    spark-submit \
        --class "$MAIN_CLASS" \
        --master "$CUSTOM_MASTER" \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.driver.memory="$DRIVER_MEMORY" \
        --conf spark.executor.memory="$EXECUTOR_MEMORY" \
        "target/$JAR_NAME"
}

# Clean build artifacts
clean() {
    print_info "Cleaning build artifacts..."
    mvn clean -q
    
    # Remove result files
    rm -f VAR_campaign_*.tsv
    rm -f FUN_campaign_*.tsv
    
    print_success "Clean completed"
}

# Run quick test
test() {
    print_info "Running quick test..."
    
    if [ ! -f "target/$JAR_NAME" ]; then
        print_warning "JAR not found. Building first..."
        build
    fi
    
    print_info "Starting quick test with small dataset..."
    
    # Create a test configuration
    spark-submit \
        --class "$MAIN_CLASS" \
        --master "local[2]" \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=2g \
        "target/$JAR_NAME"
}

# Run simple test without Spark
simple() {
    print_info "Running simple test without Spark overhead..."
    
    if [ ! -f "target/$JAR_NAME" ]; then
        print_warning "JAR not found. Building first..."
        build
    fi
    
    print_info "Starting simple optimization test..."
    
    # Run simple optimizer without spark-submit
    java -cp "target/$JAR_NAME" \
        -Xmx2g \
        org.uma.jmetalsp.spark.examples.campaign.SimpleCampaignOptimizer
}

# Show project information
info() {
    print_info "Campaign Scheduling Optimizer Information"
    echo ""
    echo "Project Structure:"
    tree -I 'target|.git' || ls -la
    echo ""
    echo "Maven Dependencies:"
    mvn dependency:tree -q | head -20
    echo ""
    echo "Spark Configuration:"
    echo "  Default Master: $SPARK_MASTER"
    echo "  Main Class: $MAIN_CLASS"
    echo "  JAR Name: $JAR_NAME"
}

# Show usage information
usage() {
    echo "Campaign Scheduling Optimizer - Build and Run Script"
    echo ""
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  build              Build the project and create JAR"
    echo "  run                Run the optimizer with default settings"
    echo "  run-custom [master] [driver_mem] [executor_mem]"
    echo "                     Run with custom Spark configuration"
    echo "  test               Run quick test with small dataset"
    echo "  simple             Run simple test without Spark overhead"
    echo "  clean              Clean build artifacts and result files"
    echo "  info               Show project information"
    echo "  help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 run"
    echo "  $0 run-custom local[8] 4g 8g"
    echo "  $0 test"
    echo ""
    echo "For Zeppelin usage, see ZeppelinNotebook.md"
}

# Main script logic
case "${1:-help}" in
    "build")
        build
        ;;
    "run")
        run
        ;;
    "run-custom")
        run_custom "$@"
        ;;
    "test")
        test
        ;;
    "simple")
        simple
        ;;
    "clean")
        clean
        ;;
    "info")
        info
        ;;
    "help"|*)
        usage
        ;;
esac 