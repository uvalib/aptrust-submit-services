#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.26.0-alpine3.23 AS builder

# update the packages
RUN apk update && apk upgrade && apk add --no-cache make

WORKDIR /build
COPY service-common ./service-common
#COPY aptrust-submit-bagger ./aptrust-submit-bagger
#RUN cd aptrust-submit-bagger; make linux
COPY aptrust-submit-reconciler ./aptrust-submit-reconciler
RUN cd aptrust-submit-reconciler; make linux
COPY aptrust-submit-submitter ./aptrust-submit-submitter
RUN cd aptrust-submit-submitter; make linux
COPY aptrust-submit-validator ./aptrust-submit-validator
RUN cd aptrust-submit-validator; make linux

#
# build the target container
#
FROM public.ecr.aws/docker/library/alpine:3.23

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup webservice && adduser webservice -G webservice -D

# set the timezone appropriatly
ENV TZ=UTC
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME=/aptrust-submit-services
WORKDIR ${APP_HOME}

# Create necessary directories
RUN mkdir -p ${APP_HOME} ${APP_HOME}/bin ${APP_HOME}/scripts
RUN chown -R webservice ${APP_HOME} && chgrp -R webservice ${APP_HOME}

# run command
#CMD ["scripts/entry.sh"]

# Move in necessary assets
COPY package/data/container_bash_profile /home/webservice/.profile
COPY package/scripts/bagger-entry.ksh ${APP_HOME}/scripts/bagger-entry.ksh
COPY package/scripts/reconciler-entry.ksh ${APP_HOME}/scripts/reconciler-entry.ksh
COPY package/scripts/submitter-entry.ksh ${APP_HOME}/scripts/submitter-entry.ksh
COPY package/scripts/validator-entry.ksh ${APP_HOME}/scripts/validator-entry.ksh
#COPY --from=builder /build/aptrust-submit-bagger/bin/aptrust-submit-bagger.linux ${APP_HOME}/bin/aptrust-submit-bagger
COPY --from=builder /build/aptrust-submit-reconciler/bin/aptrust-submit-reconciler.linux ${APP_HOME}/bin/aptrust-submit-reconciler
COPY --from=builder /build/aptrust-submit-submitter/bin/aptrust-submit-submitter.linux ${APP_HOME}/bin/aptrust-submit-submitter
COPY --from=builder /build/aptrust-submit-validator/bin/aptrust-submit-validator.linux ${APP_HOME}/bin/aptrust-submit-validator

# Ensure permissions are correct
RUN chown webservice:webservice /home/webservice/.profile ${APP_HOME}/scripts/*.ksh ${APP_HOME}/bin/aptrust-submit-* && chmod 755 /home/webservice/.profile ${APP_HOME}/scripts/*.ksh ${APP_HOME}/bin/aptrust-submit-*

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch ${APP_HOME}/buildtag.build-$BUILD_TAG || touch ${APP_HOME}/buildtag.build-0

# Specify the user
USER webservice

#
# end of file
#
