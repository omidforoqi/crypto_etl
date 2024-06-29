FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

COPY pyproject.toml ${USER_CODE_PATH}/pyproject.toml

RUN pip3 install -e ${USER_CODE_PATH}/pyproject.toml
