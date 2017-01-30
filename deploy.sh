IMAGE=rollulus/kafka-streams-plumber
TAG=$(git describe --exact-match)

if [ "$TAG" ]; then
  docker login -u="$DOCKER_USER" -p="$DOCKER_PASS" && \
  cd docker && docker build -t $IMAGE . && cd .. && \
  docker tag $IMAGE $IMAGE:latest && \
  docker tag $IMAGE $IMAGE:$TAG && \
  docker push $IMAGE:latest && \
  docker push $IMAGE:$TAG && \
  echo published tagged commit $IMAGE:$TAG
else
  echo not publishing untagged commit.
fi
