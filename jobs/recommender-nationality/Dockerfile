FROM node:18-slim
WORKDIR /usr/src/app
COPY package.json ./
RUN npm install --only=prod
COPY index.js ./
CMD ["node", "index.js"]
