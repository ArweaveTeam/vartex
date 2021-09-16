# This stage installs our modules
FROM mhart/alpine-node:16.4
WORKDIR /app
COPY package.json yarn.lock ./

RUN apk add --no-cache git
RUN yarn

COPY . .
CMD ["yarn", "start"]
