FROM node:26.1.0-alpine3.23 AS build

WORKDIR /app
COPY package.json package-lock.json ./
ARG NPM_CONFIG_STRICT_SSL=true
RUN npm ci --no-audit --no-fund

COPY index.html tsconfig.json vite.config.ts ./
COPY src ./src
RUN npm run build

FROM nginx:1.29.7-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
