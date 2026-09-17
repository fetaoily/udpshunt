# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/udpshunt ./cmd/udpshunt

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/udpshunt /udpshunt
# The dashboard is embedded; no extra files needed.
EXPOSE 9155/udp 9155/tcp
USER nonroot:nonroot
ENTRYPOINT ["/udpshunt"]
CMD ["-c", "/etc/udpshunt/udpshunt.yaml"]
