fn main() {
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["src/proto/rss_ops.proto"], &["src/proto/"])
        .unwrap();
}
