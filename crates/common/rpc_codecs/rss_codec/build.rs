fn main() {
    println!("cargo:rerun-if-changed=src/proto/rss_ops.proto");
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["src/proto/rss_ops.proto"], &["src/proto/"])
        .unwrap();
}
