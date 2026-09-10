fn main() {
    println!("cargo:rerun-if-changed=src/proto/fs_gateway_ops.proto");
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["src/proto/fs_gateway_ops.proto"], &["src/proto/"])
        .unwrap();
}
