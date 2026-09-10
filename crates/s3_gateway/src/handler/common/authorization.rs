/// What kind of authorization is required to perform a given action
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    /// No authorization is required
    None,
    /// Having Read permission on bucket
    Read,
    /// Having Write permission on bucket
    Write,
    /// Having Owner permission on bucket
    Owner,
}
