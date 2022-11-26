# Image for building application.
FROM golang:1.19.3-alpine3.16 AS builder

ARG STORAGE
ARG SERVICE

## Copying coge to image.
COPY ./ /code
WORKDIR /code

## Building of binary application.
RUN go build -o service ./${STORAGE}/${SERVICE}


# Runtime image.
FROM alpine:3.16.3

## Change user.
RUN addgroup -S runner && adduser -S runner -G runner
USER runner

## Copying of binary application.
COPY --from=builder --chown=runner:runner /code/service /usr/bin/service

## Start of application.
ENTRYPOINT ["service"]
EXPOSE 24190
STOPSIGNAL SIGQUIT
CMD [""]