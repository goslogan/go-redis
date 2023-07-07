#! /bin/bash

function start_sentinel () {
  local REDIS_PORT=$1
  local PORT=$2

  mkdir -p /nodes/$PORT

  cat << EOF >> /nodes/${PORT}/sentinel.conf
port ${PORT}
daemonize yes
sentinel monitor redis-py-test 127.0.0.1 ${REDIS_PORT} 2
sentinel down-after-milliseconds redis-py-test 5000
sentinel failover-timeout redis-py-test 60000
sentinel parallel-syncs redis-py-test 1
logfile /redis.log
EOF

  set -x
  redis-sentinel /nodes/${PORT}/sentinel.conf
  sleep 1
  if [ $? -ne 0 ]; then
    echo "Sentinel failed to start, exiting."
    continue
  fi
  echo 127.0.0.1:${PORT} >> /nodes/nodemap
}


function start_redis () {
  local PORT=$1
  local REPLICA=$2

  mkdir -p /nodes/${PORT}

  cat << EOF >> /nodes/${PORT}/redis.conf
port ${PORT}
daemonize yes
logfile /redis.log
dir /nodes/${PORT}
loadmodule /opt/redis-stack/lib/redisbloom.so
loadmodule /opt/redis-stack/lib/redisearch.so
loadmodule /opt/redis-stack/lib/redistimeseries.so
loadmodule /opt/redis-stack/lib/rejson.so
EOF

  if [ ! -z "${REPLICA}" ]; then
    cat << EOFR >> /nodes/${PORT}/redis.conf
replicaof 127.0.0.1 ${REPLICA}
EOFR
  fi
  
  cat /nodes/${PORT}/redis.conf
  set -x
  redis-server /nodes/${PORT}/redis.conf
  sleep 1
  if [ $? -ne 0 ]; then
    echo "Redis failed to start, exiting."
    continue
  fi
  echo 127.0.0.1:${PORT} >> /nodes/nodemap
}

mkdir -p /nodes
touch /nodes/nodemap

START_PORT=$1

if [ -z ${START_PORT} ]; then
    START_PORT=26379
fi
echo "STARTING: ${START_PORT}"
start_redis ${START_PORT}

echo "FIRST REPLICA"
start_redis $((${START_PORT}+1)) $START_PORT

echo "SECOND REPLICA"
start_redis $((${START_PORT}+2)) $START_PORT

echo "FIRST SENTINEL"
start_sentinel ${START_PORT} $((${START_PORT}+11))

echo "SECOND SENTINEL"
start_sentinel ${START_PORT} $((${START_PORT}+12))

echo "SECOND SENTINEL"
start_sentinel ${START_PORT} $((${START_PORT}+13))

 
tail -f /redis.log
