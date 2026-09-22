### Default image is base. You can add other support by modifying BASE_IMAGE_TAG. The following parameters are supported: base (default), aria2, ffmpeg, aio
ARG BASE_IMAGE_TAG=base
ARG FRONTEND_REPO=invoker-kael/OpenList-Frontend
ARG FRONTEND_REF=c8f23446f4ea63ecbf19118fb440600c7075f338

FROM node:24-alpine AS frontend-builder
ARG FRONTEND_REPO
ARG FRONTEND_REF
RUN apk add --no-cache git
WORKDIR /frontend
RUN git clone --filter=blob:none --no-checkout "https://github.com/${FRONTEND_REPO}.git" . \
    && git checkout "${FRONTEND_REF}"
RUN corepack enable \
    && pnpm install --frozen-lockfile \
    && pnpm build

FROM alpine:edge AS builder
ARG FRONTEND_REPO
ARG FRONTEND_REF
LABEL stage=go-builder
WORKDIR /app/
RUN apk add --no-cache bash curl jq gcc git go musl-dev
COPY go.mod go.sum ./
RUN go mod download
COPY ./ ./
RUN rm -rf public/dist
COPY --from=frontend-builder /frontend/dist ./public/dist
ENV FRONTEND_REPO=${FRONTEND_REPO} \
    OPENLIST_FRONTEND_REF=${FRONTEND_REF} \
    OPENLIST_FRONTEND_PREBUILT=1
RUN bash build.sh release docker

FROM openlistteam/openlist-base-image:${BASE_IMAGE_TAG}
LABEL MAINTAINER="OpenList"
ARG INSTALL_FFMPEG=false
ARG INSTALL_ARIA2=false
ARG USER=openlist
ARG UID=1001
ARG GID=1001

WORKDIR /opt/openlist/

RUN addgroup -g ${GID} ${USER} && \
    adduser -D -u ${UID} -G ${USER} ${USER} && \
    mkdir -p /opt/openlist/data

COPY --from=builder --chmod=755 --chown=${UID}:${GID} /app/bin/openlist ./
COPY --chmod=755 --chown=${UID}:${GID} entrypoint.sh /entrypoint.sh

USER ${USER}
RUN /entrypoint.sh version

ENV UMASK=022 RUN_ARIA2=${INSTALL_ARIA2}
VOLUME /opt/openlist/data/
EXPOSE 5244 5245
CMD [ "/entrypoint.sh" ]
