fn main() {
    println!("cargo:rerun-if-changed=../../../../common/protos/bss_ops.proto");
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(
            &["../../../../common/protos/bss_ops.proto"],
            &["../../../../common/protos"],
        )
        .unwrap();
}
