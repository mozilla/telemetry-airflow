version: '2'

services:
  db:
    image: postgres:9.4

  redis:
    image: redis:3.2

  app:
    build:
      context: .
      dockerfile: Dockerfile.dev
    restart: always
    volumes:
      - $PWD:/app
    command: "true"

  web:
    extends:
      service: app
    ports:
      - "8000:8000"
    depends_on:
      - app
    links:
      - db
      - redis
    command: web

  worker:
    extends:
      service: app
    mem_limit: 4294967296
    ports:
      - "8793:8793"
    links:
      - db
      - redis
    command: worker

  scheduler:
    mem_limit: 4294967296
    extends:
      service: app
    links:
      - db
      - redis
    command: scheduler

  flower:
    extends:
      service: app
    ports:
      - "5555:5555"
    links:
      - redis
    command: flower
