IMAGE=rollulus/kafka-streams-plumber
TAG=$(git describe --exact-match)

if [ "$TAG" ]; then
  docker login -u="$DOCKER_USER" -p="$DOCKER_PASS" && \
  cd docker && docker build -t $IMAGE --build-arg srcjar=../target/kafka-streams-plumber-0.1-jar-with-dependencies.jar . && cd .. && \
  docker tag $IMAGE $IMAGE:latest && \
  docker tag $IMAGE $IMAGE:$TAG && \
  docker push $IMAGE:latest && \
  docker push $IMAGE:$TAG && \
  echo published tagged commit $IMAGE:$TAG
else
  echo not publishing untagged commit.
fi
