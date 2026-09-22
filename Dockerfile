### Default image is base. You can add other support by modifying BASE_IMAGE_TAG. The following parameters are supported: base (default), aria2, ffmpeg, aio
ARG BASE_IMAGE_TAG=base
ARG FRONTEND_REPO=invoker-kael/OpenList-Frontend
ARG FRONTEND_REF=12bd89dfe7a144106259faaade80d1d997920df6
ARG FRONTEND_I18N_URL=https://github.com/OpenListTeam/OpenList-Frontend/releases/download/v4.2.6/i18n.tar.gz

FROM node:24-alpine AS frontend-builder
ARG FRONTEND_REPO
ARG FRONTEND_REF
ARG FRONTEND_I18N_URL
RUN apk add --no-cache git curl
WORKDIR /frontend
RUN git clone --filter=blob:none --no-checkout "https://github.com/${FRONTEND_REPO}.git" . \
    && git checkout "${FRONTEND_REF}"
RUN curl -fL "$FRONTEND_I18N_URL" -o /tmp/i18n.tar.gz \
    && tar -xzf /tmp/i18n.tar.gz -C src/lang \
    && rm -f /tmp/i18n.tar.gz \
    && corepack enable \
    && pnpm install --frozen-lockfile \
    && node ./scripts/i18n.mjs \
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
