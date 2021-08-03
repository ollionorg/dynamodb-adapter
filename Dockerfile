# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# start with Golang 1.13 image as builder
FROM golang:1.13 AS builder

WORKDIR /build

# Cache modules retrieval - those don't change so often
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code necessary to build the application
COPY . .

# Build the application
# And compile the project
RUN go build -o db-driver .

# Create the minimal runtime image
FROM ubuntu:18.04

RUN apt update && apt install ca-certificates -y
COPY --from=builder /build/db-driver .

ARG ACTIVE_ENV
ENV ACTIVE_ENV ${ACTIVE_ENV}

ARG GOOGLE_APPLICATION_CREDENTIALS
ENV GOOGLE_APPLICATION_CREDENTIALS ${GOOGLE_APPLICATION_CREDENTIALS}

ENV GIN_MODE=release
EXPOSE 9050

CMD ["/db-driver"]
