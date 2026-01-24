use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum S3Path {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

#[allow(dead_code)]
impl S3Path {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if let Some(rest) = s.strip_prefix("s3://") {
            let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                anyhow::bail!("Invalid S3 URI: bucket name is empty");
            }
            Ok(S3Path::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })
        } else {
            Ok(S3Path::Local(PathBuf::from(s)))
        }
    }

    pub fn is_s3(&self) -> bool {
        matches!(self, S3Path::S3 { .. })
    }

    pub fn is_local(&self) -> bool {
        matches!(self, S3Path::Local(_))
    }

    pub fn as_s3(&self) -> Option<(&str, &str)> {
        match self {
            S3Path::S3 { bucket, key } => Some((bucket.as_str(), key.as_str())),
            S3Path::Local(_) => None,
        }
    }

    pub fn as_local(&self) -> Option<&PathBuf> {
        match self {
            S3Path::Local(p) => Some(p),
            S3Path::S3 { .. } => None,
        }
    }

    pub fn ends_with_slash(&self) -> bool {
        match self {
            S3Path::S3 { key, .. } => key.ends_with('/'),
            S3Path::Local(p) => p.to_string_lossy().ends_with('/'),
        }
    }
}

impl std::fmt::Display for S3Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Path::Local(p) => write!(f, "{}", p.display()),
            S3Path::S3 { bucket, key } => {
                if key.is_empty() {
                    write!(f, "s3://{}", bucket)
                } else {
                    write!(f, "s3://{}/{}", bucket, key)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_local_path() {
        let path = S3Path::parse("/tmp/file.txt").unwrap();
        assert!(path.is_local());
        assert_eq!(path.as_local().unwrap(), &PathBuf::from("/tmp/file.txt"));
    }

    #[test]
    fn test_parse_s3_path() {
        let path = S3Path::parse("s3://bucket/key/file.txt").unwrap();
        assert!(path.is_s3());
        let (bucket, key) = path.as_s3().unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key/file.txt");
    }

    #[test]
    fn test_parse_s3_bucket_only() {
        let path = S3Path::parse("s3://bucket").unwrap();
        let (bucket, key) = path.as_s3().unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "");
    }

    #[test]
    fn test_ends_with_slash() {
        let path = S3Path::parse("s3://bucket/folder/").unwrap();
        assert!(path.ends_with_slash());

        let path = S3Path::parse("s3://bucket/file.txt").unwrap();
        assert!(!path.ends_with_slash());
    }
}
