
ARG FUNCTION_DIR="/function"

FROM python:3.12 AS build-image

ARG FUNCTION_DIR

RUN mkdir -p ${FUNCTION_DIR}
WORKDIR ${FUNCTION_DIR}

RUN apt-get update && apt-get install -y \
    g++ \
    make \
    cmake \
    unzip \
    libcurl4-openssl-dev

COPY requirements.txt .


RUN pip install --target ${FUNCTION_DIR} awslambdaric


RUN pip install --target ${FUNCTION_DIR} -r requirements.txt


COPY . ${FUNCTION_DIR}


FROM python:3.12-slim


ARG FUNCTION_DIR
WORKDIR ${FUNCTION_DIR}


COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

ENV PYTHONPATH=${FUNCTION_DIR}

ENV HOME=/tmp

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

CMD [ "handler.lambda_handler" ]