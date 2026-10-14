# Build context is created by scripts/container-build.sh and contains only binaries.
FROM scratch AS memoryd
COPY memoryd /memoryd
USER 65532:65532
WORKDIR /
EXPOSE 8443
STOPSIGNAL SIGTERM
ENTRYPOINT ["/memoryd"]

FROM scratch AS memoryworker
COPY memoryworker /memoryworker
USER 65532:65532
WORKDIR /
STOPSIGNAL SIGTERM
ENTRYPOINT ["/memoryworker"]
