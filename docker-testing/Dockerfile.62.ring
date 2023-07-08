FROM redis/redis-stack-server:6.2.6-v7

COPY ring/create_ring.sh /create_ring.sh

RUN chmod a+x /create_ring.sh

ENTRYPOINT [ "/create_ring.sh"]
