Third party packages

These are used for local development:

1. [dynamodb local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
2. [minio](https://github.com/minio/minio)

These are used for benchmark in a deployed vpc environment:

1. [s3 benchmark tools](https://github.com/minio/warp/releases/tag/v1.3.0)

These are used for filesystem testing (fs_server FUSE mount):

1. [pjdfstest](https://github.com/pjd/pjdfstest) -- POSIX filesystem
   compliance test suite (`chmod`, `chown`, `link`, `mkdir`, `mkfifo`,
   `open`, `rename`, `rmdir`, `symlink`, `truncate`, `unlink`,
   `chflags`, `granular`). Driven via `cargo xtask run-tests pjdfstest`,
   which clones and builds the suite under `data/third_party/pjdfstest/`
   on first run, then runs `prove -r tests/` from the FUSE mount root
   in `mode=default` so the writeback queue is exercised.
