FROM openjdk:8-jre

ARG srcjar

ENV BOOTSTRAP_SERVERS=localhost:9092
ENV ZOOKEEPER_CONNECT=localhost:2181
ENV SCHEMA_REGISTRY_URL=http://localhost:8081
ENV APPLICATION_ID=unique-plumber-id

RUN test -n "$srcjar" || (echo "*** srcjar is not specified" && false)

COPY ${srcjar} /opt/plumber.jar
COPY plumber /opt/plumber

CMD /opt/plumber
