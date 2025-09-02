#!/bin/sh
set -e

echo "Downloading JMX Prometheus agent..."

# Download JMX Prometheus JavaAgent
wget -O /jars/jmx_prometheus_javaagent.jar \
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROMETHEUS_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_PROMETHEUS_AGENT_VERSION}.jar"

echo "Creating JMX configuration file..."

# Create JMX configuration
cat > /jars/jmx_config.yml << 'EOF'
---
rules:
  - pattern: ".*"
EOF

echo "JMX Prometheus agent and config downloaded successfully!"

# Verify files exist
if [ -f "/jars/jmx_prometheus_javaagent.jar" ] && [ -f "/jars/jmx_config.yml" ]; then
    echo "✅ All files created successfully:"
    ls -la /jars/jmx_prometheus_javaagent.jar /jars/jmx_config.yml
else
    echo "❌ File creation failed!"
    exit 1
fi

# Keep container running for healthcheck
echo "Keeping container alive for healthcheck..."
tail -f /dev/null