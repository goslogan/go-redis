#! /bin/bash

function start_redis () {
  local PORT=$1

  mkdir -p /nodes/${PORT}

  cat << EOF >> /nodes/${PORT}/redis.conf
port ${PORT}
cluster-enabled yes
daemonize yes
logfile /nodes/${PORT}/redis.log
dir /nodes/${PORT}
pidfile /nodes/${PORT}/redis.pid
protected-mode no
loadmodule /opt/redis-stack/lib/redisbloom.so
loadmodule /opt/redis-stack/lib/redisearch.so
loadmodule /opt/redis-stack/lib/redistimeseries.so
loadmodule /opt/redis-stack/lib/rejson.so
EOF
    
  set -x
  if [ ! -z "${ENABLE_DEBUG}" ]; then
    redis-server /nodes/${PORT}/redis.conf --enable-debug-command yes
  else 
      redis-server /nodes/${PORT}/redis.conf
  fi
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
ENABLE_DEBUG=$2

if [ -z ${START_PORT} ]; then
    START_PORT=9379
fi

END_PORT=$(( ${START_PORT} + 5 ))

for (( PORT = ${START_PORT}; PORT <= ${END_PORT}; PORT++ )) 
do  
  start_redis ${PORT}
done

echo yes | redis-cli --cluster create `seq -f 127.0.0.1:%g ${START_PORT} ${END_PORT}` --cluster-replicas 1
tail -f /nodes/*/redis.log
