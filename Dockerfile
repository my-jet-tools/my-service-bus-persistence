FROM ubuntu:22.04
COPY ./wwwroot ./wwwroot
COPY ./target/release/my-sb-persistence ./target/release/my-sb-persistence
ENTRYPOINT ["./target/release/my-sb-persistence"]
