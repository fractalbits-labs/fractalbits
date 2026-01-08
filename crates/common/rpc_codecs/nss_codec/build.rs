fn main() {
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["src/proto/nss_ops.proto"], &["src/proto/"])
        .unwrap();
}
