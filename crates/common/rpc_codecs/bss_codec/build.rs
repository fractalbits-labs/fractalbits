fn main() {
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(
            &["../../../../common/protos/bss_ops.proto"],
            &["../../../../common/protos"],
        )
        .unwrap();
}
