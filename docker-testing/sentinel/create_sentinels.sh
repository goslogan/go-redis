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
  redis-server /nodes/${PORT}/redis.conf
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
  if [[ -e /redis.conf ]]; then
    cp /redis.conf /nodes/$PORT/redis.conf
  else
    touch /nodes/$PORT/redis.conf
  fi

  cat << EOF >> /nodes/${PORT}/redis.conf
port ${PORT}
cluster-enabled yes
  daemonize yes
logfile /redis.log
dir /nodes/${PORT}
EOF

if [ ! -z "${REPLICA}" ]; do 
  cat << EOFR >> /nodes/${PORT}/redis.conf
replicaof 127.0.0.1 ${REPLICA}
EOF

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

if [ -z ${START_PORT} ]; then
    START_PORT=26379
fi
echo "STARTING: ${START_PORT}"
start_redis(${START_PORT})
start_redis($((${START_PORT}+1)), $START_PORT)
start_redis($((${START_PORT}+2)), $START_PORT)



tail -f /redis.log
