use actix_web::http::header::HeaderName;

// ---- Constants used in AWSv4 signatures ----
pub const X_AMZ_ALGORITHM: HeaderName = HeaderName::from_static("x-amz-algorithm");
pub const X_AMZ_CREDENTIAL: HeaderName = HeaderName::from_static("x-amz-credential");
pub const X_AMZ_DATE: HeaderName = HeaderName::from_static("x-amz-date");
pub const X_AMZ_EXPIRES: HeaderName = HeaderName::from_static("x-amz-expires");
pub const X_AMZ_SIGNEDHEADERS: HeaderName = HeaderName::from_static("x-amz-signedheaders");
pub const X_AMZ_SIGNATURE: HeaderName = HeaderName::from_static("x-amz-signature");
pub const X_AMZ_CONTENT_SHA256: HeaderName = HeaderName::from_static("x-amz-content-sha256");
pub const X_AMZ_TRAILER: HeaderName = HeaderName::from_static("x-amz-trailer");

pub const X_AMZ_CHECKSUM_ALGORITHM: HeaderName =
    HeaderName::from_static("x-amz-checksum-algorithm");
pub const X_AMZ_SDK_CHECKSUM_ALGORITHM: HeaderName =
    HeaderName::from_static("x-amz-sdk-checksum-algorithm");
pub const X_AMZ_CHECKSUM_MODE: HeaderName = HeaderName::from_static("x-amz-checksum-mode");
pub const X_AMZ_CHECKSUM_CRC32: HeaderName = HeaderName::from_static("x-amz-checksum-crc32");
pub const X_AMZ_CHECKSUM_CRC32C: HeaderName = HeaderName::from_static("x-amz-checksum-crc32c");
pub const X_AMZ_CHECKSUM_CRC64NVME: HeaderName =
    HeaderName::from_static("x-amz-checksum-crc64nvme");
pub const X_AMZ_CHECKSUM_SHA1: HeaderName = HeaderName::from_static("x-amz-checksum-sha1");
pub const X_AMZ_CHECKSUM_SHA256: HeaderName = HeaderName::from_static("x-amz-checksum-sha256");

pub const X_AMZ_SERVER_SIDE_ENCRYPTION: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-customer-key");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-aws-kms-key-id");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-context");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_BUCKET_KEY_ENABLED: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-bucket-key-enabled");

pub const X_AMZ_REQUEST_PAYER: HeaderName = HeaderName::from_static("x-amz-request-payer");
pub const X_AMZ_EXPECTED_BUCKET_OWNER: HeaderName =
    HeaderName::from_static("x-amz-expected-bucket-owner");

pub const X_AMZ_MAX_PARTS: HeaderName = HeaderName::from_static("x-amz-max-parts");
pub const X_AMZ_PART_NUMBER_MARKER: HeaderName =
    HeaderName::from_static("x-amz-part-number-marker");
pub const X_AMZ_OBJECT_ATTRIBUTES: HeaderName = HeaderName::from_static("x-amz-object-attributes");
pub const X_AMZ_OBJECT_SIZE: HeaderName = HeaderName::from_static("x-amz-object-size");

pub const X_AMZ_WEBSITE_REDIRECT_LOCATION: HeaderName =
    HeaderName::from_static("x-amz-website-redirect-location");

pub const X_AMZ_ACL: HeaderName = HeaderName::from_static("x-amz-acl");
pub const X_AMZ_COPY_SOURCE: HeaderName = HeaderName::from_static("x-amz-copy-source");
pub const X_AMZ_COPY_SOURCE_IF_MATCH: HeaderName =
    HeaderName::from_static("x-amz-copy-source-if-match");
pub const X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: HeaderName =
    HeaderName::from_static("x-amz-copy-source-if-modified-since");
pub const X_AMZ_COPY_SOURCE_IF_NONE_MATCH: HeaderName =
    HeaderName::from_static("x-amz-copy-source-if-none-match");
pub const X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: HeaderName =
    HeaderName::from_static("x-amz-copy-source-if-unmodified-since");
pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: HeaderName =
    HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-algorithm");
pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: HeaderName =
    HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-key");
pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: HeaderName =
    HeaderName::from_static("x-amz-copy-source-server-side-encryption-customer-key-md5");
pub const X_AMZ_GRANT_FULL_CONTROL: HeaderName =
    HeaderName::from_static("x-amz-grant-full-control");
pub const X_AMZ_GRANT_READ: HeaderName = HeaderName::from_static("x-amz-grant-read");
pub const X_AMZ_GRANT_READ_ACP: HeaderName = HeaderName::from_static("x-amz-grant-read-acp");
pub const X_AMZ_GRANT_WRITE_ACP: HeaderName = HeaderName::from_static("x-amz-grant-write-acp");
pub const X_AMZ_METADATA_DIRECTIVE: HeaderName =
    HeaderName::from_static("x-amz-metadata-directive");
pub const X_AMZ_TAGGING: HeaderName = HeaderName::from_static("x-amz-tagging");
pub const X_AMZ_TAGGING_DIRECTIVE: HeaderName = HeaderName::from_static("x-amz-tagging-directive");
pub const X_AMZ_STORAGE_CLASS: HeaderName = HeaderName::from_static("x-amz-storage-class");
pub const X_AMZ_STORAGE_OBJECT_LOCK_MODE: HeaderName =
    HeaderName::from_static("x-amz-object-lock-mode");
pub const X_AMZ_STORAGE_OBJECT_LOCK_RETAIN_UNTIL_DATE: HeaderName =
    HeaderName::from_static("x-amz-object-lock-retain-until-date");
pub const X_AMZ_STORAGE_OBJECT_LOCK_LEGAL_HOLD: HeaderName =
    HeaderName::from_static("x-amz-object-lock-legal-hold");
pub const X_AMZ_STORAGE_EXPECTED_BUCKET_OWNER: HeaderName =
    HeaderName::from_static("x-amz-expected-bucket-owner");
pub const X_AMZ_STORAGE_SOURCE_EXPECTED_BUCKET_OWNER: HeaderName =
    HeaderName::from_static("x-amz-source-expected-bucket-owner");
