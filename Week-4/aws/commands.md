docker build -t stream-model-duration:v1 .

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="us-east-1" \
    stream-model-duration:v1

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="us-east-1" \
    -e AWS_ACCESS_KEY_ID="" \
    -e AWS_SECRET_ACCESS_KEY="" \
    -v /home/taras/.aws:/root/.aws \
    stream-model-duration:v1

REMOTE_URI="340951083884.dkr.ecr.us-east-1.amazonaws.com/duration-model"
REMOTE_TAG="v1"
REMOTE_IMAGE=${REMOTE_URI}:${REMOTE_TAG}

LOCAL_IMAGE="stream-model-duration:v1"
docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}

aws ecr get-login-password | docker login --username AWS --password-stdin 340951083884.dkr.ecr.us-east-1.amazonaws.com/duration-model
