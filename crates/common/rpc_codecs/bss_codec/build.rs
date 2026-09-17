fn main() {
    println!("cargo:rerun-if-changed=src/proto/bss_ops.proto");
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["src/proto/bss_ops.proto"], &["src/proto/"])
        .unwrap();
}
