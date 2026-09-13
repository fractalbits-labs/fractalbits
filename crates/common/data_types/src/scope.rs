//! Mount scope: the directory key a session is confined to.
//!
//! Shared by `fractalbits-mount`, which normalises its `prefix` at
//! startup, and `fs_gateway`, which does the same at `Mount`, so both
//! reject the same strings.

use std::fmt;

/// The whole bucket.
pub const ROOT_SCOPE: &str = "/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeError {
    NotAbsolute,
    NoTrailingSlash,
    EmptySegment,
    DotSegment,
}

impl fmt::Display for ScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "scope must start with '/'"),
            Self::NoTrailingSlash => write!(f, "scope must end with '/'"),
            Self::EmptySegment => write!(f, "scope has an empty segment"),
            Self::DotSegment => write!(f, "scope has a '.' or '..' segment"),
        }
    }
}

impl std::error::Error for ScopeError {}

/// Validate a scope. Empty means the root; anything else must start and
/// end with `/` and carry no empty, `.` or `..` segment. Nothing is fixed
/// up, so a misconfigured mount fails loudly instead of landing one
/// directory off.
pub fn normalize_scope(prefix: &str) -> Result<String, ScopeError> {
    if prefix.is_empty() || prefix == ROOT_SCOPE {
        return Ok(ROOT_SCOPE.to_string());
    }
    if !prefix.starts_with('/') {
        return Err(ScopeError::NotAbsolute);
    }
    if !prefix.ends_with('/') {
        return Err(ScopeError::NoTrailingSlash);
    }
    for segment in prefix[1..prefix.len() - 1].split('/') {
        match segment {
            "" => return Err(ScopeError::EmptySegment),
            "." | ".." => return Err(ScopeError::DotSegment),
            _ => {}
        }
    }
    Ok(prefix.to_string())
}

/// True when `key` lies under `scope` (both normalised). The scope's own
/// directory key is inside, and the trailing slash keeps `/repo/` from
/// matching `/repository/`.
pub fn in_scope(scope: &str, key: &str) -> bool {
    key.starts_with(scope)
}

/// Every directory key from the top down to `scope` itself, the ones
/// `mkdir -p` would create. Empty for the root scope.
pub fn scope_ancestors(scope: &str) -> Vec<String> {
    let mut out = Vec::new();
    if scope == ROOT_SCOPE {
        return out;
    }
    let mut end = 1;
    while let Some(next) = scope[end..].find('/') {
        end += next + 1;
        out.push(scope[..end].to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalise_rules() {
        assert_eq!(normalize_scope("").expect("empty"), "/", "empty is root");
        assert_eq!(normalize_scope("/").expect("root"), "/", "root");
        assert_eq!(normalize_scope("/a/b/").expect("nested"), "/a/b/", "nested");
        assert_eq!(
            normalize_scope("a/"),
            Err(ScopeError::NotAbsolute),
            "relative"
        );
        assert_eq!(
            normalize_scope("/a"),
            Err(ScopeError::NoTrailingSlash),
            "no slash"
        );
        assert_eq!(
            normalize_scope("/a//b/"),
            Err(ScopeError::EmptySegment),
            "double slash"
        );
        assert_eq!(
            normalize_scope("/a/../"),
            Err(ScopeError::DotSegment),
            "dot dot"
        );
        assert_eq!(normalize_scope("/./"), Err(ScopeError::DotSegment), "dot");
    }

    #[test]
    fn membership_and_ancestors() {
        assert!(in_scope("/", "/anything"), "root scope");
        assert!(in_scope("/repo/", "/repo/"), "scope dir itself");
        assert!(in_scope("/repo/", "/repo/src/x"), "inside");
        assert!(!in_scope("/repo/", "/repository/x"), "sibling prefix");
        assert!(!in_scope("/repo/", "/other/"), "outside");
        assert!(!in_scope("/repo/", "@orphan/x"), "hidden keyspace");
        assert_eq!(scope_ancestors("/"), Vec::<String>::new(), "root has none");
        assert_eq!(scope_ancestors("/a/b/"), vec!["/a/", "/a/b/"], "ancestors");
    }
}
