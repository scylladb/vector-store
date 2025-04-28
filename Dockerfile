FROM rust:1.86 AS build
WORKDIR /build/vector-store-build
RUN --mount=type=bind,target=.,rw cargo b -r && cp target/release/vector-store ..

FROM ubuntu:24.04
RUN mkdir /opt/vector-store
COPY --from=build /build/vector-store /opt/vector-store
CMD /opt/vector-store/vector-store
