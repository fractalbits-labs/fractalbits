# fractal-s3

An AWS S3 CLI replacement for FractalBits, providing standard S3 operations
plus FractalBits-specific features like atomic folder rename.

## Configuration

Uses standard AWS SDK environment variables:

```bash
export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret
```

## Supported Commands

| Command | Description | Example |
|---------|-------------|---------|
| `ls` | List buckets or objects | `fractal-s3 ls s3://bucket/` |
| `mb` | Make bucket | `fractal-s3 mb s3://bucket` |
| `rb` | Remove bucket | `fractal-s3 rb s3://bucket` |
| `cp` | Copy files to/from S3 | `fractal-s3 cp file.txt s3://bucket/key` |
| `rm` | Remove objects | `fractal-s3 rm s3://bucket/key` |
| `mv` | Move/rename | `fractal-s3 mv s3://bucket/old/ s3://bucket/new/` |

## Key Features

- **Standard S3 Operations** - Compatible with `aws s3` CLI patterns
- **Atomic Folder Rename** - The `mv` command auto-detects folder-to-folder
  moves within the same bucket and uses FractalBits' native `renameFolder` API
- **Atomic Object Rename** - Object moves within the same bucket use the
  `renameObject` API for instant rename
- **Recursive Operations** - `cp -r` and `rm -r` for directory operations
- **SigV4 Signing** - Custom HTTP requests use proper AWS signature

## Atomic Rename Operations

FractalBits supports atomic rename for both folders and objects within the
same bucket.

### Atomic Folder Rename

When both source and destination are S3 paths in the same bucket ending
with `/`:

```bash
# Uses atomic renameFolder API (instant, regardless of folder size)
fractal-s3 mv s3://bucket/folder1/ s3://bucket/folder2/
```

### Atomic Object Rename

When moving an object within the same bucket:

```bash
# Uses atomic renameObject API (instant, no copy+delete)
fractal-s3 mv s3://bucket/old-name.txt s3://bucket/new-name.txt
```

### Cross-Bucket Moves

Cross-bucket moves use standard copy+delete:

```bash
# Uses copy+delete (not atomic)
fractal-s3 mv s3://bucket1/file.txt s3://bucket2/file.txt
```

## Usage Examples

```bash
# List all buckets
fractal-s3 ls

# Create a bucket
fractal-s3 mb s3://my-bucket

# Upload a file
fractal-s3 cp myfile.txt s3://my-bucket/myfile.txt

# Upload a directory recursively
fractal-s3 cp -r ./mydir s3://my-bucket/mydir/

# List objects in a bucket
fractal-s3 ls s3://my-bucket/

# Download a file
fractal-s3 cp s3://my-bucket/myfile.txt ./downloaded.txt

# Atomic folder rename (FractalBits-specific)
fractal-s3 mv s3://my-bucket/old-folder/ s3://my-bucket/new-folder/

# Delete an object
fractal-s3 rm s3://my-bucket/myfile.txt

# Delete a bucket
fractal-s3 rb s3://my-bucket
```

## Build

```bash
cargo build -p fractal-s3
```

## Performance Demo

A demo script is provided to demonstrate the performance advantage of atomic
rename operations:

```bash
# Run both demos with defaults (100MB object, 1000 files)
./crates/fractal-s3/demo/atomic_rename_demo.sh both

# Demo with 500MB object only
./crates/fractal-s3/demo/atomic_rename_demo.sh object 500

# Demo with 5000 files only
./crates/fractal-s3/demo/atomic_rename_demo.sh folder 5000

# Large demo (1GB object, 10k files)
./crates/fractal-s3/demo/atomic_rename_demo.sh large

# Extra large demo (5GB object, 100k files) - takes a while to upload
./crates/fractal-s3/demo/atomic_rename_demo.sh xlarge
```

### Example Output

```
Demo: Atomic Folder Rename (100 objects)
========================================

[INFO] Uploading 100 files to S3 (this may take a while)...
[INFO] Upload completed in 4.817s
[INFO] Performing atomic folder rename with fractal-s3...
[SUCCESS] Atomic folder rename completed in 0.120s

Results:
  - Number of objects: 100
  - Upload time: 4.817s
  - Rename time: 0.120s (instant, regardless of object count!)
```

**Key insight**: The rename operation takes ~0.1s regardless of whether you're
renaming 100 or 100,000 objects. Traditional copy+delete would scale linearly
with the number of objects.
