FROM redis/redis-stack-server:7.0.6-RC2

COPY ring/create_ring.sh /create_ring.sh

RUN chmod a+x /create_ring.sh

ENTRYPOINT [ "/create_ring.sh"]
