use super::*;

// https://github.com/minio/warp/blob/master/yml-samples/mixed.yml
pub fn create_mixed_workload_config(
    warp_client_ips: &str,
    region: &str,
    service_endpoint: &str,
    duration: &str,
    size_kb: usize,
    concurrent_ops: usize,
) -> CmdResult {
    let config_content = format!(
        r##"warp:
  api: v1

  # Benchmark to run.
  # Corresponds to warp [benchmark] command.
  benchmark: mixed

  # Do not print any output.
  quiet: false

  # Disable terminal color output.
  no-color: false

  # Print results and errors as JSON.
  json: false

  # Output benchmark+profile data to this file.
  # By default a unique filename is generated.
  bench-data:

  # Connect to warp clients and run benchmarks there.
  # See https://github.com/minio/warp?tab=readme-ov-file#distributed-benchmarking
  # Can be a single value or a list.
  warp-client:
{warp_client_ips}
  # Run MinIO server profiling during benchmark;
  # possible values are 'cpu', 'cpuio', 'mem', 'block', 'mutex', 'threads' and 'trace'.
  # Can be single value or a list.
  server-profile:

  # Remote host parameters and connection info.
  remote:
    # Specify custom region
    region: {region}

    # Access key and Secret key
    access-key: test_api_key
    secret-key: test_api_secret

    # Specify one or more hosts.
    # The benchmark will be run against all hosts concurrently.
    # Multiple servers can be specified with ellipsis notation;
    # for example '10.0.0.{{1...10}}:9000' specifies 10 hosts.
    # See more at https://github.com/minio/warp?tab=readme-ov-file#multiple-hosts
    host: {service_endpoint}

    # Use TLS for calls.
    tls: false

    # Allow TLS with unverified certificates.
    insecure: true

    # Stream benchmark statistics to Influx DB instance.
    # See more at https://github.com/minio/warp?tab=readme-ov-file#influxdb-output
    influxdb: ''

    # Bucket to use for benchmark data.
    #
    #  CAREFUL:    ALL DATA WILL BE DELETED IN BUCKET!
    #
    # By default, 'warp-benchmark-bucket' will be created or used.
    bucket:

  # params specifies the benchmark parameters.
  # The fields here depend on the benchmark type.
  params:
    # Duration to run the benchmark.
    # Use 's' and 'm' to specify seconds and minutes.
    duration: {duration}

    # Concurrent operations to run per warp instance.
    concurrent: {concurrent_ops}

    # The number of objects to upload before starting the benchmark.
    # Upload enough objects to ensure that any remote caching is bypassed.
    # objects: 1000

    # Adjust the distribution of each operation type
    # The final distribution will be determined by the fraction of each value of the total.
    distribution:
      get: 45.0
      stat: 30.0
      put: 15.0
      delete: 10.0 # Must be same or lower than 'put'.

    # Properties of uploaded objects.
    obj:
      # Size of each uploaded object
      size: {size_kb}KiB

      # Randomize the size of each object within certain constraints.
      # See https://github.com/minio/warp?tab=readme-ov-file#random-file-sizes
      rand-size: false

      # Force specific size of each multipart part.
      # Must be '5MB' or bigger.
      part-size:

    # Use automatic termination when traffic stabilizes.
    # Can not be used with distributed warp setup.
    # See https://github.com/minio/warp?tab=readme-ov-file#automatic-termination
    autoterm:
      enabled: false
      dur: 10s
      pct: 7.5

    # Do not clear bucket before or after running benchmarks.
    no-clear: true

    # Leave benchmark data. Do not run cleanup after benchmark.
    # Bucket will still be cleaned prior to benchmark.
    keep-data: true


  # The io section specifies custom IO properties for uploaded objects.
  io:
    # Use a custom prefix
    prefix:

    # Do not use separate prefix for each thread
    no-prefix: false

    # Add MD5 sum to uploads
    md5: false

    # Disable multipart uploads
    disable-multipart: true

    # Disable calculating sha256 on client side for uploads
    disable-sha256-payload: true

    # Server-side sse-s3 encrypt/decrypt objects
    sse-s3-encrypt: false

    # Encrypt/decrypt objects (using server-side encryption with random keys)
    sse-c-encrypt: false

    # Override storage class.
    # Default storage class will be used unless specified.
    storage-class:

  analyze:
    # Display additional analysis data.
    verbose: true
    # Only output for this host.
    host: ''
    # Only output for this operation. Can be 'GET', 'PUT', 'DELETE', etc.
    filter-op: ''
    # Split analysis into durations of this length.
    # Can be '1s', '5s', '1m', etc.
    segment-duration:
    # Output aggregated data as to file.
    out:
    # Additional time duration to skip when analyzing data.
    skip-duration:
    # Max operations to load for analysis.
    limit:
    # Skip this number of operations before starting analysis.
    offset:

  advanced:
    # Stress test only and discard output.
    stress: false

    # Print requests.
    debug: false

    # Disable HTTP Keep-Alive
    disable-http-keepalive: false

    # Enable HTTP2 support if server supports it
    http2: false

    # Rate limit each instance to this number of requests per second
    rps-limit:

    # Host selection algorithm.
    # Can be 'weighed' or 'roundrobin'
    host-select: weighed

    # "Resolve the host(s) ip(s) (including multiple A/AAAA records).
    # This can break SSL certificates, use --insecure if so
    resolve-host: false

    # Specify custom write socket buffer size in bytes
    sndbuf: 32768

    # Specify custom read socket buffer size in bytes
    rcvbuf: 32768

    # When running benchmarks open a webserver to fetch results remotely, eg: localhost:7762
    serve:
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/bench_mixed_${size_kb}k.yml;
    }?;
    Ok(())
}
