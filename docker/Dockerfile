FROM openjdk:8-jre

ARG srcjar

ENV BOOTSTRAP_SERVERS=localhost:9092
ENV ZOOKEEPER_CONNECT=localhost:2181
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV APPLICATION_ID=unique-plumber-id

RUN test -n "$srcjar" || (echo "*** srcjar is not specified" && false)

RUN curl -Lo /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.0/dumb-init_1.2.0_amd64
RUN chmod +x /usr/bin/dumb-init
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

COPY ${srcjar} /opt/plumber.jar
COPY plumber /opt/plumber

CMD ["/opt/plumber"]
