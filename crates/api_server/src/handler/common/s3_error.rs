use std::{convert::From, str::Utf8Error};

use super::signature::SignatureError;
use crate::blob_storage::BlobStorageError;
use actix_web::{
    HttpResponse, ResponseError,
    http::{
        StatusCode,
        header::{InvalidHeaderValue, ToStrError},
        uri::InvalidUri,
    },
};
use data_types::TraceId;
use http_range::HttpRangeParseError;
use rpc_client_common::RpcError;
use strum::AsRefStr;
use thiserror::Error;

// From https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html (2025/02/28)

#[derive(Debug, Error, AsRefStr)]
pub enum S3Error {
    #[error("The bucket does not allow ACLs.")]
    AccessControlListNotSupported,

    #[error("Access Denied.")]
    AccessDenied,

    #[error("An access point with an identical name already exists in your account.")]
    AccessPointAlreadyOwnedByYou,

    #[error(
        "There is a problem with your account that prevents the operation from completing successfully."
    )]
    AccountProblem,

    #[error("All access to this S3 resource has been disabled.")]
    AllAccessDisabled,

    #[error("The email address that you provided is associated with more than one account.")]
    AmbiguousGrantByEmailAddress,

    #[error("The authorization header that you provided is not valid.")]
    AuthorizationHeaderMalformed,

    #[error("The authorization query parameters that you provided are not valid.")]
    AuthorizationQueryParametersError,

    #[error(
        "The Content-MD5 or checksum value that you specified did not match what the server received."
    )]
    BadDigest,

    #[error(
        "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Specify a different name and try again."
    )]
    BucketAlreadyExists,

    #[error("The bucket that you tried to create already exists, and you own it.")]
    BucketAlreadyOwnedByYou,

    #[error(
        "The bucket you tried to delete has access points attached. Delete your access points before deleting your bucket."
    )]
    BucketHasAccessPointsAttached,

    #[error("The bucket that you tried to delete is not empty.")]
    BucketNotEmpty,

    #[error(
        "Your Multi-Region Access Point idempotency token was already used for a different request."
    )]
    ClientTokenConflict,

    #[error(
        "Returned to the original caller when an error is encountered while reading the WriteGetObjectResponse body."
    )]
    ConnectionClosedByRequester,

    #[error(
        "A conflicting operation occurred. If using PutObject you can retry the request. If using multipart upload you should initiate another CreateMultipartUpload request and re-upload each part."
    )]
    ConditionalRequestConflict,

    #[error("This request does not support credentials.")]
    CredentialsNotSupported,

    #[error(
        "Cross-Region logging is not allowed. Buckets in one Region cannot log information to a bucket in another Region."
    )]
    CrossLocationLoggingProhibited,

    #[error("The device is not currently active.")]
    DeviceNotActiveError,

    #[error("Direct requests to the correct endpoint.")]
    EndpointNotFound,

    #[error("Your proposed upload is smaller than the minimum allowed object size.")]
    EntityTooSmall,

    #[error("Your proposed upload exceeds the maximum allowed object size.")]
    EntityTooLarge,

    #[error("The provided token has expired.")]
    ExpiredToken,

    #[error(
        "You are trying to access a bucket from a different Region than where the bucket exists."
    )]
    #[strum(serialize = "IllegalLocationConstraintException")]
    IllegalLocationConstraintException0,

    #[error(
        "You attempt to create a bucket with a location constraint that corresponds to a different region than the regional endpoint the request was sent to."
    )]
    #[strum(serialize = "IllegalLocationConstraintException")]
    IllegalLocationConstraintException1,

    #[error("The versioning configuration specified in the request is not valid.")]
    IllegalVersioningConfigurationException,

    #[error("You did not provide the number of bytes specified by the Content-Length HTTP header.")]
    IncompleteBody,

    #[error(
        "The specified bucket exists in another Region. Direct requests to the correct endpoint."
    )]
    IncorrectEndpoint,

    #[error("POST requires exactly one file upload per request.")]
    IncorrectNumberOfFilesInPostRequest,

    #[error("The inline data exceeds the maximum allowed size.")]
    InlineDataTooLarge,

    #[error("An internal error occurred. Try again.")]
    InternalError,

    #[error("The access key ID that you provided does not exist in our records.")]
    InvalidAccessKeyId,

    #[error("The specified access point name or account is not valid.")]
    InvalidAccessPoint,

    #[error("The specified access point alias name is not valid.")]
    InvalidAccessPointAliasError,

    #[error("You must specify the Anonymous role.")]
    InvalidAddressingHeader,

    #[error(
        "A ListBuckets request is made to a Regional endpoint that is different from the Region specified in the bucket-region parameter."
    )]
    #[strum(serialize = "InvalidArgument")]
    InvalidArgument0,

    #[error("The specified argument was not valid.")]
    #[strum(serialize = "InvalidArgument")]
    InvalidArgument1,

    #[error("The request was missing a required header.")]
    #[strum(serialize = "InvalidArgument")]
    InvalidArgument2,

    #[error("The specified argument was incomplete or in the wrong format.")]
    #[strum(serialize = "InvalidArgument")]
    InvalidArgument3,

    #[error("The specified argument must have a length greater than or equal to 3.")]
    #[strum(serialize = "InvalidArgument")]
    InvalidArgument4,

    #[error("Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting.")]
    InvalidBucketAclWithObjectOwnership,

    #[error("The specified bucket is not valid.")]
    InvalidBucketName,

    #[error("The value of the expected bucket owner parameter must be an account ID.")]
    InvalidBucketOwnerAccountID,

    #[error("The request is not valid for the current state of the bucket.")]
    InvalidBucketState,

    #[error("The Content-MD5 or checksum value that you specified is not valid.")]
    InvalidDigest,

    #[error("The encryption request that you specified is not valid. The valid value is AES256.")]
    InvalidEncryptionAlgorithmError,

    #[error("The host headers provided in the request used the incorrect style addressing.")]
    InvalidHostHeader,

    #[error("The request is made using an unexpected HTTP method.")]
    InvalidHttpMethod,

    #[error("The specified location (Region) constraint is not valid.")]
    InvalidLocationConstraint,

    #[error("The operation is not valid for the current state of the object.")]
    InvalidObjectState,

    #[error(
        "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
    )]
    InvalidPart,

    #[error(
        "The list of parts was not in ascending order. The parts list must be specified in order by part number."
    )]
    InvalidPartOrder,

    #[error("All access to this object has been disabled.")]
    InvalidPayer,

    #[error(
        "The content of the form does not meet the conditions specified in the policy document."
    )]
    InvalidPolicyDocument,

    #[error("The requested range is not valid for the request. Try another range.")]
    InvalidRange,

    #[error(
        "An unpaginated ListBuckets request is made from an account that has an approved general purpose bucket quota higher than 10,000. You must make paginated requests to list the buckets in an account with more than 10,000 buckets."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest0,

    #[error(
        "The request is using the wrong signature version. Use AWS4-HMAC-SHA256 (Signature Version 4)."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest1,

    #[error("An access point can be created only for an existing bucket.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest2,

    #[error("The access point is not in a state where it can be deleted.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest3,

    #[error("An access point can be listed only for an existing bucket.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest4,

    #[error("The next token is not valid.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest5,

    #[error("At least one action must be specified in a lifecycle rule.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest6,

    #[error("At least one lifecycle rule must be specified.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest7,

    #[error("The number of lifecycle rules must not exceed the allowed limit of 1000 rules.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest8,

    #[error("The range for the MaxResults parameter is not valid.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest9,

    #[error("SOAP requests must be made over an HTTPS connection.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest10,

    #[error(
        "Amazon S3 Transfer Acceleration is not supported for buckets with non-DNS compliant names."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest11,

    #[error(
        "Amazon S3 Transfer Acceleration is not supported for buckets with periods (.) in their names."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest12,

    #[error("The Amazon S3 Transfer Acceleration endpoint supports only virtual style requests.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest13,

    #[error("Amazon S3 Transfer Acceleration is not configured on this bucket.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest14,

    #[error("Amazon S3 Transfer Acceleration is disabled on this bucket.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest15,

    #[error(
        "Amazon S3 Transfer Acceleration is not supported on this bucket. For assistance, contact Support."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest16,

    #[error(
        "Amazon S3 Transfer Acceleration cannot be enabled on this bucket. For assistance, contact Support."
    )]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest17,

    #[error("Conflicting values provided in HTTP headers and query parameters.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest18,

    #[error("Conflicting values provided in HTTP headers and POST form fields.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest19,

    #[error("CopyObject request made on objects larger than 5GB in size.")]
    #[strum(serialize = "InvalidRequest")]
    InvalidRequest20,

    #[error("Returned if the session doesn't exist anymore because it timed out or expired.")]
    InvalidSessionException,

    #[error(
        "The request signature that the server calculated does not match the signature that you provided. Check your secret access key and signing method. For more information, see Signing and authenticating REST requests."
    )]
    InvalidSignature,

    #[error("The provided security credentials are not valid.")]
    InvalidSecurity,

    #[error("The SOAP request body is not valid.")]
    InvalidSOAPRequest,

    #[error("The storage class that you specified is not valid.")]
    InvalidStorageClass,

    #[error(
        "The target bucket for logging either does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group."
    )]
    InvalidTargetBucketForLogging,

    #[error("The provided token is malformed or otherwise not valid.")]
    InvalidToken,

    #[error("The specified URI couldn't be parsed.")]
    InvalidURI,

    #[error("Your key is too long.")]
    KeyTooLongError,

    #[error("Your key contains unsupported character(s).")]
    KeyUnsupported,

    #[error("The request was rejected because the specified KMS key is not enabled.")]
    #[strum(serialize = "KMS.DisabledException")]
    KMSDisabledException,

    #[error("The KeyUsage value of the KMS key is incompatible with the API operation.")]
    #[strum(serialize = "KMS.InvalidKeyUsageException")]
    KMSInvalidKeyUsageException0,

    #[error(
        "The encryption algorithm or signing algorithm specified for the operation is incompatible with the type of key material in the KMS key (KeySpec)."
    )]
    #[strum(serialize = "KMS.InvalidKeyUsageException")]
    KMSInvalidKeyUsageException1,

    #[error(
        "The request was rejected because the state of the specified resource is not valid for this request."
    )]
    #[strum(serialize = "KMS.KMSINvalidStateException")]
    KMSKMSInvalidStateException,

    #[error(
        "The request was rejected because the specified entity or resource could not be found."
    )]
    #[strum(serialize = "KMS.NotFoundException")]
    KMSNotFoundException,

    #[error(
        "The ACL that you provided was not well formed or did not validate against our published schema."
    )]
    MalformedACLError,

    #[error("The body of your POST request is not well-formed multipart/form-data.")]
    MalformedPOSTRequest,

    #[error(
        "The XML that you provided was not well formed or did not validate against our published schema."
    )]
    MalformedXML,

    #[error("Your request was too large.")]
    MaxMessageLengthExceeded,

    #[error("Your POST request fields preceding the upload file were too large.")]
    MaxPostPreDataLengthExceededError,

    #[error("Your metadata headers exceed the maximum allowed metadata size.")]
    MetadataTooLarge,

    #[error("The specified method is not allowed against this resource.")]
    MethodNotAllowed,

    #[error("A SOAP attachment was expected, but none was found.")]
    MissingAttachment,

    #[error("The request was not signed.")]
    MissingAuthenticationToken,

    #[error("You must provide the Content-Length HTTP header.")]
    MissingContentLength,

    #[error("You sent an empty XML document as a request.")]
    MissingRequestBodyError,

    #[error("The SOAP 1.1 request is missing a security element.")]
    MissingSecurityElement,

    #[error("Your request is missing a required header.")]
    MissingSecurityHeader,

    #[error("There is no such thing as a logging status subresource for a key.")]
    NoLoggingStatusForKey,

    #[error("The specified request was not found.")]
    NoSuchAsyncRequest,

    #[error("The specified bucket does not exist.")]
    NoSuchBucket,

    #[error("The specified bucket does not have a bucket policy.")]
    NoSuchBucketPolicy,

    #[error("The specified bucket does not have a CORS configuration.")]
    NoSuchCORSConfiguration,

    #[error("The specified key does not exist.")]
    NoSuchKey,

    #[error("The specified lifecycle configuration does not exist.")]
    NoSuchLifecycleConfiguration,

    #[error("The specified Multi-Region Access Point does not exist.")]
    NoSuchMultiRegionAccessPoint,

    #[error("The specified object does not have an ObjectLock configuration.")]
    NoSuchObjectLockConfiguration,

    #[error("The specified bucket does not have a website configuration.")]
    NoSuchWebsiteConfiguration,

    #[error("The specified tag does not exist.")]
    NoSuchTagSet,

    #[error("The specified multipart upload does not exist.")]
    NoSuchUpload,

    #[error("The version ID specified in the request does not match an existing version.")]
    NoSuchVersion,

    #[error("The device that generated the token is not owned by the authenticated user.")]
    NotDeviceOwnerError,

    #[error("A header that you provided implies functionality that is not implemented.")]
    NotImplemented,

    #[error("The resource was not changed.")]
    NotModified,

    #[error("No transformation found for this Object Lambda Access Point.")]
    NoTransformationDefined,

    #[error(
        "Your account is not signed up for the fractalbits S3 service. You must sign up before you can use S3."
    )]
    NotSignedUp,

    #[error("The Object Lock configuration does not exist for this bucket.")]
    ObjectLockConfigurationNotFoundError,

    #[error("The bucket ownership controls were not found.")]
    OwnershipControlsNotFoundError,

    #[error(
        "A conflicting conditional operation is currently in progress against this resource. Try again."
    )]
    OperationAborted,

    #[error(
        "The bucket that you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint."
    )]
    PermanentRedirect,

    #[error(
        "The API operation you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint."
    )]
    PermanentRedirectControlError,

    #[error("At least one of the preconditions that you specified did not hold.")]
    PreconditionFailed,

    #[error(
        "Temporary redirect. You are being redirected to the bucket while the Domain Name System (DNS) server is being updated."
    )]
    Redirect,

    #[error(
        "The request header and query parameters used to make the request exceed the maximum allowed size."
    )]
    RequestHeaderSectionTooLarge,

    #[error("A bucket POST request must be of the enclosure-type multipart/form-data.")]
    RequestIsNotMultiPartContent,

    #[error(
        "Your socket connection to the server was not read from or written to within the timeout period."
    )]
    RequestTimeout,

    #[error("The difference between the request time and the server's time is too large.")]
    RequestTimeTooSkewed,

    #[error("Requesting the torrent file of a bucket is not permitted.")]
    RequestTorrentOfBucketError,

    #[error(
        "Returned to the original caller when an error is encountered while reading the WriteGetObjectResponse body."
    )]
    ResponseInterrupted,

    #[error("The object restore is already in progress.")]
    RestoreAlreadyInProgress,

    #[error("The server-side encryption configuration was not found.")]
    ServerSideEncryptionConfigurationNotFoundError,

    #[error("Service is unable to handle request.")]
    ServiceUnavailable,

    #[error(
        "The request signature that the server calculated does not match the signature that you provided. Check your AWS secret access key and signing method."
    )]
    SignatureDoesNotMatch,

    #[error("Please reduce your request rate.")]
    SlowDown,

    #[error("Slow Down")]
    #[strum(serialize = "503 SlowDown")]
    SlowDown503,

    #[error(
        "You are being redirected to the bucket while the Domain Name System (DNS) server is being updated."
    )]
    TemporaryRedirect,

    #[error("The serial number and/or token code you provided is not valid.")]
    TokenCodeInvalidError,

    #[error("The provided token must be refreshed.")]
    TokenRefreshRequired,

    #[error("You have attempted to create more access points than are allowed for an account.")]
    TooManyAccessPoints,

    #[error("You have attempted to create more buckets than are allowed for an account.")]
    TooManyBuckets,

    #[error(
        "You have attempted to create a Multi-Region Access Point with more Regions than are allowed for an account."
    )]
    TooManyMultiRegionAccessPointregionsError,

    #[error(
        "You have attempted to create more Multi-Region Access Points than are allowed for an account."
    )]
    TooManyMultiRegionAccessPoints,

    #[error(
        "Applicable in China Regions only. Returned when a request is made to a bucket that doesn't have an ICP license."
    )]
    UnauthorizedAccessError,

    #[error("This request contains unsupported content.")]
    UnexpectedContent,

    #[error(
        "Applicable in China Regions only. This request was rejected because the IP was unexpected."
    )]
    UnexpectedIPError,

    #[error("The request contained an unsupported argument.")]
    UnsupportedArgument,

    #[error(
        "The provided request is signed with an unsupported STS Token version or the signature version is not supported."
    )]
    UnsupportedSignature,

    #[error("The email address that you provided does not match any account on record.")]
    UnresolvableGrantByEmailAddress,

    #[error(
        "The bucket POST request must contain the specified field name. If it is specified, check the order of the fields."
    )]
    UserKeyMustBeSpecified,

    #[error("The specified access point does not exist.")]
    NoSuchAccessPoint,

    #[error(
        "Your request contains tag input that is not valid. For example, your request might contain duplicate keys, keys or values that are too long, or system tags."
    )]
    InvalidTag,

    #[error("Your policy contains a principal that is not valid.")]
    MalformedPolicy,
}

impl S3Error {
    #[inline]
    pub fn http_status_code(&self) -> StatusCode {
        match self {
            S3Error::AccessControlListNotSupported => StatusCode::BAD_REQUEST,
            S3Error::AccessDenied => StatusCode::FORBIDDEN,
            S3Error::AccessPointAlreadyOwnedByYou => StatusCode::CONFLICT,
            S3Error::AccountProblem => StatusCode::FORBIDDEN,
            S3Error::AllAccessDisabled => StatusCode::FORBIDDEN,
            S3Error::AmbiguousGrantByEmailAddress => StatusCode::BAD_REQUEST,
            S3Error::AuthorizationHeaderMalformed => StatusCode::BAD_REQUEST,
            S3Error::AuthorizationQueryParametersError => StatusCode::BAD_REQUEST,
            S3Error::BadDigest => StatusCode::BAD_REQUEST,
            S3Error::BucketAlreadyExists => StatusCode::CONFLICT,
            S3Error::BucketAlreadyOwnedByYou => StatusCode::CONFLICT,
            S3Error::BucketHasAccessPointsAttached => StatusCode::BAD_REQUEST,
            S3Error::BucketNotEmpty => StatusCode::CONFLICT,
            S3Error::ClientTokenConflict => StatusCode::CONFLICT,
            S3Error::ConnectionClosedByRequester => StatusCode::BAD_REQUEST,
            S3Error::ConditionalRequestConflict => StatusCode::CONFLICT,
            S3Error::CredentialsNotSupported => StatusCode::BAD_REQUEST,
            S3Error::CrossLocationLoggingProhibited => StatusCode::FORBIDDEN,
            S3Error::DeviceNotActiveError => StatusCode::BAD_REQUEST,
            S3Error::EndpointNotFound => StatusCode::BAD_REQUEST,
            S3Error::EntityTooSmall => StatusCode::BAD_REQUEST,
            S3Error::EntityTooLarge => StatusCode::BAD_REQUEST,
            S3Error::ExpiredToken => StatusCode::BAD_REQUEST,
            S3Error::IllegalLocationConstraintException0 => StatusCode::BAD_REQUEST,
            S3Error::IllegalLocationConstraintException1 => StatusCode::BAD_REQUEST,
            S3Error::IllegalVersioningConfigurationException => StatusCode::BAD_REQUEST,
            S3Error::IncompleteBody => StatusCode::BAD_REQUEST,
            S3Error::IncorrectEndpoint => StatusCode::BAD_REQUEST,
            S3Error::IncorrectNumberOfFilesInPostRequest => StatusCode::BAD_REQUEST,
            S3Error::InlineDataTooLarge => StatusCode::BAD_REQUEST,
            S3Error::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            S3Error::InvalidAccessKeyId => StatusCode::FORBIDDEN,
            S3Error::InvalidAccessPoint => StatusCode::BAD_REQUEST,
            S3Error::InvalidAccessPointAliasError => StatusCode::BAD_REQUEST,
            S3Error::InvalidAddressingHeader => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument0 => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument1 => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument2 => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument3 => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument4 => StatusCode::BAD_REQUEST,
            S3Error::InvalidBucketAclWithObjectOwnership => StatusCode::BAD_REQUEST,
            S3Error::InvalidBucketName => StatusCode::BAD_REQUEST,
            S3Error::InvalidBucketOwnerAccountID => StatusCode::BAD_REQUEST,
            S3Error::InvalidBucketState => StatusCode::CONFLICT,
            S3Error::InvalidDigest => StatusCode::BAD_REQUEST,
            S3Error::InvalidEncryptionAlgorithmError => StatusCode::BAD_REQUEST,
            S3Error::InvalidHostHeader => StatusCode::BAD_REQUEST,
            S3Error::InvalidHttpMethod => StatusCode::BAD_REQUEST,
            S3Error::InvalidLocationConstraint => StatusCode::BAD_REQUEST,
            S3Error::InvalidObjectState => StatusCode::FORBIDDEN,
            S3Error::InvalidPart => StatusCode::BAD_REQUEST,
            S3Error::InvalidPartOrder => StatusCode::BAD_REQUEST,
            S3Error::InvalidPayer => StatusCode::FORBIDDEN,
            S3Error::InvalidPolicyDocument => StatusCode::BAD_REQUEST,
            S3Error::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            S3Error::InvalidRequest0 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest1 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest2 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest3 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest4 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest5 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest6 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest7 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest8 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest9 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest10 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest11 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest12 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest13 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest14 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest15 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest16 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest17 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest18 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest19 => StatusCode::BAD_REQUEST,
            S3Error::InvalidRequest20 => StatusCode::BAD_REQUEST,
            S3Error::InvalidSessionException => StatusCode::BAD_REQUEST,
            S3Error::InvalidSignature => StatusCode::BAD_REQUEST,
            S3Error::InvalidSecurity => StatusCode::FORBIDDEN,
            S3Error::InvalidSOAPRequest => StatusCode::BAD_REQUEST,
            S3Error::InvalidStorageClass => StatusCode::BAD_REQUEST,
            S3Error::InvalidTargetBucketForLogging => StatusCode::BAD_REQUEST,
            S3Error::InvalidToken => StatusCode::BAD_REQUEST,
            S3Error::InvalidURI => StatusCode::BAD_REQUEST,
            S3Error::KeyTooLongError => StatusCode::BAD_REQUEST,
            S3Error::KeyUnsupported => StatusCode::BAD_REQUEST,
            S3Error::KMSDisabledException => StatusCode::BAD_REQUEST,
            S3Error::KMSInvalidKeyUsageException0 => StatusCode::BAD_REQUEST,
            S3Error::KMSInvalidKeyUsageException1 => StatusCode::BAD_REQUEST,
            S3Error::KMSKMSInvalidStateException => StatusCode::BAD_REQUEST,
            S3Error::KMSNotFoundException => StatusCode::BAD_REQUEST,
            S3Error::MalformedACLError => StatusCode::BAD_REQUEST,
            S3Error::MalformedPOSTRequest => StatusCode::BAD_REQUEST,
            S3Error::MalformedXML => StatusCode::BAD_REQUEST,
            S3Error::MaxMessageLengthExceeded => StatusCode::BAD_REQUEST,
            S3Error::MaxPostPreDataLengthExceededError => StatusCode::BAD_REQUEST,
            S3Error::MetadataTooLarge => StatusCode::BAD_REQUEST,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::MissingAttachment => StatusCode::BAD_REQUEST,
            S3Error::MissingAuthenticationToken => StatusCode::FORBIDDEN,
            S3Error::MissingContentLength => StatusCode::LENGTH_REQUIRED,
            S3Error::MissingRequestBodyError => StatusCode::BAD_REQUEST,
            S3Error::MissingSecurityElement => StatusCode::BAD_REQUEST,
            S3Error::MissingSecurityHeader => StatusCode::BAD_REQUEST,
            S3Error::NoLoggingStatusForKey => StatusCode::BAD_REQUEST,
            S3Error::NoSuchAsyncRequest => StatusCode::NOT_FOUND,
            S3Error::NoSuchBucket => StatusCode::NOT_FOUND,
            S3Error::NoSuchBucketPolicy => StatusCode::NOT_FOUND,
            S3Error::NoSuchCORSConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey => StatusCode::NOT_FOUND,
            S3Error::NoSuchLifecycleConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchMultiRegionAccessPoint => StatusCode::NOT_FOUND,
            S3Error::NoSuchObjectLockConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchWebsiteConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchTagSet => StatusCode::NOT_FOUND,
            S3Error::NoSuchUpload => StatusCode::NOT_FOUND,
            S3Error::NoSuchVersion => StatusCode::NOT_FOUND,
            S3Error::NotDeviceOwnerError => StatusCode::BAD_REQUEST,
            S3Error::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            S3Error::NotModified => StatusCode::NOT_MODIFIED,
            S3Error::NoTransformationDefined => StatusCode::NOT_FOUND,
            S3Error::NotSignedUp => StatusCode::FORBIDDEN,
            S3Error::ObjectLockConfigurationNotFoundError => StatusCode::NOT_FOUND,
            S3Error::OwnershipControlsNotFoundError => StatusCode::NOT_FOUND,
            S3Error::OperationAborted => StatusCode::CONFLICT,
            S3Error::PermanentRedirect => StatusCode::MOVED_PERMANENTLY,
            S3Error::PermanentRedirectControlError => StatusCode::MOVED_PERMANENTLY,
            S3Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            S3Error::Redirect => StatusCode::TEMPORARY_REDIRECT,
            S3Error::RequestHeaderSectionTooLarge => StatusCode::BAD_REQUEST,
            S3Error::RequestIsNotMultiPartContent => StatusCode::PRECONDITION_FAILED,
            S3Error::RequestTimeout => StatusCode::BAD_REQUEST,
            S3Error::RequestTimeTooSkewed => StatusCode::FORBIDDEN,
            S3Error::RequestTorrentOfBucketError => StatusCode::BAD_REQUEST,
            S3Error::ResponseInterrupted => StatusCode::BAD_REQUEST,
            S3Error::RestoreAlreadyInProgress => StatusCode::CONFLICT,
            S3Error::ServerSideEncryptionConfigurationNotFoundError => StatusCode::BAD_REQUEST,
            S3Error::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            S3Error::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            S3Error::SlowDown => StatusCode::SERVICE_UNAVAILABLE,
            S3Error::SlowDown503 => StatusCode::SERVICE_UNAVAILABLE,
            S3Error::TemporaryRedirect => StatusCode::TEMPORARY_REDIRECT,
            S3Error::TokenCodeInvalidError => StatusCode::BAD_REQUEST,
            S3Error::TokenRefreshRequired => StatusCode::BAD_REQUEST,
            S3Error::TooManyAccessPoints => StatusCode::BAD_REQUEST,
            S3Error::TooManyBuckets => StatusCode::BAD_REQUEST,
            S3Error::TooManyMultiRegionAccessPointregionsError => StatusCode::BAD_REQUEST,
            S3Error::TooManyMultiRegionAccessPoints => StatusCode::BAD_REQUEST,
            S3Error::UnauthorizedAccessError => StatusCode::FORBIDDEN,
            S3Error::UnexpectedContent => StatusCode::BAD_REQUEST,
            S3Error::UnexpectedIPError => StatusCode::FORBIDDEN,
            S3Error::UnsupportedArgument => StatusCode::BAD_REQUEST,
            S3Error::UnsupportedSignature => StatusCode::BAD_REQUEST,
            S3Error::UnresolvableGrantByEmailAddress => StatusCode::BAD_REQUEST,
            S3Error::UserKeyMustBeSpecified => StatusCode::BAD_REQUEST,
            S3Error::NoSuchAccessPoint => StatusCode::NOT_FOUND,
            S3Error::InvalidTag => StatusCode::BAD_REQUEST,
            S3Error::MalformedPolicy => StatusCode::BAD_REQUEST,
        }
    }
}

impl S3Error {
    pub fn error_response_with_resource(
        &self,
        resource: &str,
        request_id: TraceId,
    ) -> actix_web::HttpResponse {
        let host_id = std::env::var("HOST_ID")
            .map(|id| format!("    <HostId>{}</HostId>\n", id))
            .unwrap_or_default();

        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
    <Resource>{}</Resource>
    <RequestId>{}</RequestId>
{}</Error>"#,
            self.as_ref(),
            self,
            resource,
            request_id,
            host_id.trim_end()
        );

        actix_web::HttpResponse::build(self.http_status_code())
            .content_type("application/xml")
            .body(body)
    }
}

impl ResponseError for S3Error {
    fn error_response(&self) -> HttpResponse {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
    <RequestId>{}</RequestId>
</Error>"#,
            self.as_ref(),
            self,
            TraceId::new(),
        );

        HttpResponse::build(self.http_status_code())
            .insert_header(("Content-Type", "application/xml"))
            .body(body)
    }
}

impl From<InvalidUri> for S3Error {
    fn from(value: InvalidUri) -> Self {
        tracing::error!("InvalidUri: {value}");
        Self::InvalidURI
    }
}

impl From<RpcError> for S3Error {
    fn from(value: RpcError) -> Self {
        tracing::error!("RpcError: {value}");
        // Connection-related errors should return 503 (ServiceUnavailable)
        // to indicate the client should retry
        if value.retryable() {
            Self::ServiceUnavailable
        } else {
            Self::InternalError
        }
    }
}

impl From<rkyv::rancor::Error> for S3Error {
    fn from(value: rkyv::rancor::Error) -> Self {
        tracing::error!("rkyv rancor::Error: {value}");
        Self::InternalError
    }
}

impl From<quick_xml::DeError> for S3Error {
    fn from(value: quick_xml::DeError) -> Self {
        tracing::error!("quick_xml::DeError: {value}");
        Self::UnexpectedContent
    }
}

impl From<ToStrError> for S3Error {
    fn from(value: ToStrError) -> Self {
        tracing::error!("ToStrError: {value}");
        Self::UnexpectedContent
    }
}

impl From<SignatureError> for S3Error {
    fn from(value: SignatureError) -> Self {
        tracing::error!("SignatureError: {value}");
        Self::InvalidSignature
    }
}

impl From<InvalidHeaderValue> for S3Error {
    fn from(value: InvalidHeaderValue) -> Self {
        tracing::error!("InvalidHeaderValue: {value}");
        Self::InvalidURI
    }
}

impl From<Utf8Error> for S3Error {
    fn from(value: Utf8Error) -> Self {
        tracing::error!("Utf8Error: {value}");
        Self::UnexpectedContent
    }
}

impl From<HttpRangeParseError> for S3Error {
    fn from(value: HttpRangeParseError) -> Self {
        tracing::error!("HttpRangeParseError: {:?}", value);
        Self::InvalidRange
    }
}

impl From<actix_web::Error> for S3Error {
    fn from(value: actix_web::Error) -> Self {
        tracing::error!("actix_web::Error: {}", value);
        Self::InternalError
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for S3Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        tracing::error!("box error: {}", err);
        S3Error::InternalError
    }
}

impl From<data_types::object_layout::ObjectLayoutError> for S3Error {
    fn from(value: data_types::object_layout::ObjectLayoutError) -> Self {
        tracing::error!("ObjectLayoutError: {value}");
        Self::InvalidObjectState
    }
}

impl From<BlobStorageError> for S3Error {
    fn from(err: BlobStorageError) -> Self {
        tracing::error!("blob storage error: {}", err);
        match err {
            BlobStorageError::BssRpc(e) => S3Error::from(e),
            BlobStorageError::DataVg(_) => S3Error::InternalError,
            BlobStorageError::S3(_) => S3Error::InternalError,
            BlobStorageError::Config(_) => S3Error::InternalError,
            BlobStorageError::Internal(_) => S3Error::InternalError,
            BlobStorageError::InitializationError(_) => S3Error::InternalError,
            BlobStorageError::QuorumFailure(_) => S3Error::InternalError,
        }
    }
}
