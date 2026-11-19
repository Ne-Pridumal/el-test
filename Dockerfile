FROM node:22-alpine

ENV REQUEST_COUNT=10 \
    INACTIVITY_TIMEOUT_MS=15000

WORKDIR /app
COPY download-speed.mjs ./

USER node
ENTRYPOINT ["node", "/app/download-speed.mjs"]
