#!/bin/bash
# Makes 10 copies of virtual_producer.py (sensor_01..sensor_10), all publishing to topic "counts".
set -e
cd "$(dirname "$0")"

for n in $(seq -w 01 10); do
  cp -f virtual_producer.py "virtual_producer_sensor_${n}.py"
done

# Force topic="counts" everywhere
sed -i -E 's/(KAFKA_TOPIC_NAME_CONS\s*=\s*")[^"]*(")/\1counts\2/' virtual_producer_sensor_*.py

# Give each file a unique sensor_id
for n in $(seq -w 01 10); do
  sed -i -E "s/(message\[\s*\"sensor_id\"\s*\]\s*=\s*['\"]).*(['\"])/\1sensor_${n}\2/" "virtual_producer_sensor_${n}.py"
  sed -i -E "s/(\"sensor_id\"\s*:\s*['\"]).*(['\"])/\1sensor_${n}\2/" "virtual_producer_sensor_${n}.py"
done

echo "Prepared 10 producers for topic 'counts' (sensor_01..sensor_10):"
ls -1 virtual_producer_sensor_*.py
