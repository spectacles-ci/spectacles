FROM python:3.7-slim

# set the installation target
ARG WORKING_DIRECTORY=/data

# parameter-store-exec variables
ARG USERNAME=runner
ARG GROUPNAME=runner

# setup initial directories and add runner user as a non-privileged, system account
# hadolint ignore=DL3013
RUN mkdir -p ${WORKING_DIRECTORY} \
  && groupadd --system ${USERNAME} \
  && useradd --system --gid ${GROUPNAME} --home-dir ${WORKING_DIRECTORY} ${USERNAME} \
  && chown --recursive ${USERNAME}:${GROUPNAME} ${WORKING_DIRECTORY} \
  && pip install --no-cache-dir spectacles

# run as the non-privilged, system account
USER ${USERNAME}

# move to the working directory
WORKDIR ${WORKING_DIRECTORY}

# check spectacles version
RUN spectacles --version

# set the default command to run
ENTRYPOINT [ "spectacles" ]