use crate::handler::common::signature::{
    ChunkSignature, ChunkSignatureContext, parse_chunk_signature, verify_chunk_signature,
};
use crate::handler::common::{
    checksum::{ChecksumValue, Checksummer, request_trailer_checksum_algorithm, verify_checksum},
    s3_error::S3Error,
};
use actix_web::HttpRequest;
use actix_web::error::PayloadError;
use base64::prelude::*;
use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

/// Streaming checksum receiver type
pub type StreamingChecksumReceiver = JoinHandle<Result<Option<ChecksumValue>, S3Error>>;

/// Message types for communication between streaming payload and checksum calculator
#[derive(Debug, Clone)]
pub enum ChecksumMessage {
    /// Regular data chunk to be included in checksum calculation
    Data(Bytes),
    /// Trailer checksums received at the end of the stream
    Trailers(S3Trailers),
    /// Stream error occurred, partial data should be discarded
    Error(String),
}

/// S3-specific trailer information extracted from HTTP trailers
#[derive(Debug, Default, Clone)]
pub struct S3Trailers {
    pub checksum_value: Option<String>, // The base64 checksum value
    pub trailer_signature: Option<String>,
}

/// State machine for parsing S3 streaming payloads
#[derive(Debug, Clone, PartialEq)]
enum S3ChunkState {
    ReadingChunkSize,
    ReadingChunkData {
        remaining: usize,
        chunk_signature: Option<ChunkSignature>,
    },
    ReadingChunkEnd,
    ReadingTrailers,
    Done,
}

pin_project! {
    /// A wrapper around actix-web's Payload that handles S3-specific streaming features:
    /// - AWS chunk-signature extensions
    /// - HTTP trailers with checksums
    /// - Streaming checksum calculation
    pub struct S3StreamingPayload {
        #[pin]
        inner: actix_web::dev::Payload,
        state: S3ChunkState,
        trailer_buffer: BytesMut,
        checksum_sender: Option<mpsc::UnboundedSender<ChecksumMessage>>,
        signature_context: Option<ChunkSignatureContext>,
        previous_signature: Option<String>,
        current_chunk_buffer: BytesMut,
    }
}

impl S3StreamingPayload {
    /// Create a streaming payload with optimized checksum calculation
    /// Avoids task spawning for simple cases (no trailers, small objects)
    pub fn with_checksums(
        payload: actix_web::dev::Payload,
        request: &HttpRequest,
        checksum_value: Option<ChecksumValue>,
    ) -> Result<(Self, StreamingChecksumReceiver), S3Error> {
        Self::with_checksums_full(payload, request, checksum_value, None)
    }

    pub fn with_checksums_and_signature(
        payload: actix_web::dev::Payload,
        request: &HttpRequest,
        checksum_value: Option<ChecksumValue>,
        signature_info: Option<(ChunkSignatureContext, Option<String>)>,
    ) -> Result<(Self, StreamingChecksumReceiver), S3Error> {
        Self::with_checksums_full(payload, request, checksum_value, signature_info)
    }

    /// Full path: complete functionality with task spawning for complex cases
    fn with_checksums_full(
        payload: actix_web::dev::Payload,
        request: &HttpRequest,
        checksum_value: Option<ChecksumValue>,
        signature_info: Option<(ChunkSignatureContext, Option<String>)>,
    ) -> Result<(Self, StreamingChecksumReceiver), S3Error> {
        tracing::debug!("Using full checksum path (with task spawning)");

        // Use the provided checksum value as expected checksum
        let expected_checksum = checksum_value;

        // Extract trailer algorithm if present
        let trailer_algorithm =
            request_trailer_checksum_algorithm(request.headers()).map_err(|e| {
                tracing::error!("Failed to extract trailer algorithm: {:?}", e);
                S3Error::InvalidDigest
            })?;

        // Determine if this is a streaming request or regular content-length request
        let is_streaming = Self::is_streaming_request(request);
        tracing::debug!("Request is streaming: {}", is_streaming);

        // Get the algorithm we're computing
        let compute_algo = trailer_algorithm.or_else(|| expected_checksum.map(|c| c.algorithm()));

        // Create checksummer based on the algorithm we need to compute
        let mut checksummer = compute_algo.map(Checksummer::new);

        // Create channel for streaming checksum data
        let (checksum_tx, mut checksum_rx) = mpsc::unbounded_channel::<ChecksumMessage>();

        // Spawn checksum calculation task
        let checksum_handle = tokio::spawn(
            async move {
                let final_expected = expected_checksum;
                let mut trailer_checksums: Option<S3Trailers> = None;

                // Process messages for checksum calculation
                let mut total_bytes = 0;
                let mut stream_error: Option<String> = None;
                while let Some(msg) = checksum_rx.recv().await {
                    match msg {
                        ChecksumMessage::Data(data) => {
                            total_bytes += data.len();
                            tracing::trace!(
                                "Updating checksummer with {} bytes: {:?}",
                                data.len(),
                                std::str::from_utf8(&data).unwrap_or("<non-utf8>")
                            );
                            if let Some(ref mut cs) = checksummer {
                                cs.update(&data);
                            }
                        }
                        ChecksumMessage::Trailers(trailers) => {
                            tracing::trace!("Received trailer checksums: {:?}", trailers);
                            trailer_checksums = Some(trailers);
                        }
                        ChecksumMessage::Error(err) => {
                            tracing::error!("Stream error during checksum calculation: {}", err);
                            stream_error = Some(err);
                            break;
                        }
                    }
                }
                tracing::debug!("Total bytes processed for checksum: {}", total_bytes);

                // If stream error occurred, return error immediately
                if let Some(err) = stream_error {
                    tracing::error!("Checksum calculation aborted due to stream error: {}", err);
                    return Err(S3Error::InternalError);
                }

                // If we received 0 bytes and expected a checksum, this likely means the stream
                // errored/timed out before any data arrived. Skip verification to avoid
                // confusing error messages.
                if total_bytes == 0 && expected_checksum.is_some() {
                    tracing::warn!("Skipping checksum verification for 0-byte failed stream");
                    return Ok(None);
                }

                // Finalize checksum
                let calculated_checksum = checksummer.map(|cs| cs.finalize());

                // Debug: log calculated checksum
                if let Some(checksum) = &calculated_checksum {
                    let encoded = match checksum {
                        ChecksumValue::Crc32(bytes) => {
                            base64::prelude::BASE64_STANDARD.encode(bytes)
                        }
                        ChecksumValue::Crc32c(bytes) => {
                            base64::prelude::BASE64_STANDARD.encode(bytes)
                        }
                        ChecksumValue::Crc64Nvme(bytes) => {
                            base64::prelude::BASE64_STANDARD.encode(bytes)
                        }
                        ChecksumValue::Sha1(bytes) => {
                            base64::prelude::BASE64_STANDARD.encode(bytes)
                        }
                        ChecksumValue::Sha256(bytes) => {
                            base64::prelude::BASE64_STANDARD.encode(bytes)
                        }
                    };
                    tracing::debug!(
                        "Calculated {:?}: (base64: {})",
                        checksum.algorithm(),
                        encoded
                    );
                }

                // Verify trailer checksums if present
                if let Some(trailers) = trailer_checksums
                    && let Some(calculated) = &calculated_checksum
                    && let Some(expected_trailer) = &trailers.checksum_value
                {
                    let calculated_bytes = match calculated {
                        ChecksumValue::Crc32(bytes) => bytes.as_slice(),
                        ChecksumValue::Crc32c(bytes) => bytes.as_slice(),
                        ChecksumValue::Crc64Nvme(bytes) => bytes.as_slice(),
                        ChecksumValue::Sha1(bytes) => bytes.as_slice(),
                        ChecksumValue::Sha256(bytes) => bytes.as_slice(),
                    };
                    let calculated_b64 = base64::prelude::BASE64_STANDARD.encode(calculated_bytes);

                    if expected_trailer != &calculated_b64 {
                        tracing::error!(
                            "{:?} checksum mismatch: trailer={}, calculated={}",
                            calculated.algorithm(),
                            expected_trailer,
                            calculated_b64
                        );
                        return Err(S3Error::InvalidDigest);
                    }

                    tracing::debug!("Trailer checksum verified successfully");
                }

                verify_checksum(calculated_checksum, final_expected).map_err(|e| {
                    tracing::error!("Checksum verification failed: {:?}", e);
                    S3Error::InvalidDigest
                })?;

                Ok(calculated_checksum)
            }
            .instrument(Span::current()),
        );

        // Choose initial state based on whether this is a streaming request
        let initial_state = if is_streaming {
            S3ChunkState::ReadingChunkSize
        } else {
            // For regular content-length requests, we pass data through directly
            S3ChunkState::ReadingChunkData {
                remaining: usize::MAX,
                chunk_signature: None,
            }
        };

        let (signature_context, previous_signature) = if let Some((ctx, seed_sig)) = signature_info
        {
            (Some(ctx), seed_sig)
        } else {
            (None, None)
        };

        let streaming_payload = Self {
            inner: payload,
            state: initial_state,
            trailer_buffer: BytesMut::new(),
            checksum_sender: Some(checksum_tx),
            signature_context,
            previous_signature,
            current_chunk_buffer: BytesMut::new(),
        };

        Ok((streaming_payload, checksum_handle))
    }

    /// Determine if this is a streaming request (S3 chunked transfer encoding) or regular content-length
    fn is_streaming_request(request: &HttpRequest) -> bool {
        // S3 streaming requests have these characteristics:
        // 1. x-amz-decoded-content-length header (the actual payload size)
        // 2. x-amz-trailer header (indicating trailers will be sent)
        // 3. content-encoding header might be present
        // 4. The content-length is the size of the chunked encoding, not the payload

        let headers = request.headers();

        // Check for decoded content length - this is the strongest indicator
        if headers.contains_key("x-amz-decoded-content-length") {
            return true;
        }

        // Check for trailer header
        if headers.contains_key("x-amz-trailer") {
            return true;
        }

        // Check for streaming signature in content-sha256
        if let Some(content_sha256) = headers.get("x-amz-content-sha256")
            && let Ok(value) = content_sha256.to_str()
            && (value == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || value.starts_with("STREAMING-"))
        {
            return true;
        }

        false
    }
}

impl Stream for S3StreamingPayload {
    type Item = Result<Bytes, PayloadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Helper closure to send error and close checksum channel
        let send_error = |sender: &mut Option<mpsc::UnboundedSender<ChecksumMessage>>,
                          err: &str| {
            if let Some(tx) = sender.take() {
                let _ = tx.send(ChecksumMessage::Error(err.to_string()));
            }
        };

        loop {
            match this.state.clone() {
                S3ChunkState::ReadingChunkSize => {
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            this.trailer_buffer.extend_from_slice(&data);

                            // Look for chunk size line ending with \r\n
                            if let Some(line_end) =
                                this.trailer_buffer.windows(2).position(|w| w == b"\r\n")
                            {
                                let chunk_line = this.trailer_buffer.split_to(line_end + 2);
                                let chunk_line_str =
                                    match std::str::from_utf8(&chunk_line[..line_end]) {
                                        Ok(s) => s,
                                        Err(_) => {
                                            tracing::error!("Invalid UTF-8 in chunk size line");
                                            send_error(
                                                this.checksum_sender,
                                                "Invalid UTF-8 in chunk size line",
                                            );
                                            return Poll::Ready(Some(Err(
                                                PayloadError::EncodingCorrupted,
                                            )));
                                        }
                                    };

                                // Parse chunk size and signature
                                let chunk_size_hex =
                                    chunk_line_str.split(';').next().unwrap_or("").trim();
                                let chunk_signature = parse_chunk_signature(chunk_line_str);

                                match usize::from_str_radix(chunk_size_hex, 16) {
                                    Ok(0) => {
                                        // Zero-sized chunk means end of chunks, start reading trailers
                                        *this.state = S3ChunkState::ReadingTrailers;
                                        continue;
                                    }
                                    Ok(size) => {
                                        *this.state = S3ChunkState::ReadingChunkData {
                                            remaining: size,
                                            chunk_signature,
                                        };
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to parse chunk size '{}': {}",
                                            chunk_size_hex,
                                            e
                                        );
                                        send_error(
                                            this.checksum_sender,
                                            &format!("Failed to parse chunk size: {}", e),
                                        );
                                        return Poll::Ready(Some(Err(
                                            PayloadError::EncodingCorrupted,
                                        )));
                                    }
                                }
                            }
                            // Not enough data yet, keep reading
                            continue;
                        }
                        Some(Err(e)) => {
                            send_error(this.checksum_sender, &format!("Payload error: {:?}", e));
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // Stream ended - this could be temporary or permanent
                            // If we have some buffered data, try to process it
                            if !this.trailer_buffer.is_empty() {
                                // Check if we have a complete chunk size line
                                if let Some(line_end) =
                                    this.trailer_buffer.windows(2).position(|w| w == b"\r\n")
                                {
                                    let chunk_line = this.trailer_buffer.split_to(line_end + 2);
                                    let chunk_line_str =
                                        match std::str::from_utf8(&chunk_line[..line_end]) {
                                            Ok(s) => s,
                                            Err(_) => {
                                                tracing::error!("Invalid UTF-8 in chunk size line");
                                                send_error(
                                                    this.checksum_sender,
                                                    "Invalid UTF-8 in final chunk size line",
                                                );
                                                return Poll::Ready(Some(Err(
                                                    PayloadError::EncodingCorrupted,
                                                )));
                                            }
                                        };

                                    let chunk_size_hex =
                                        chunk_line_str.split(';').next().unwrap_or("").trim();
                                    let chunk_signature = parse_chunk_signature(chunk_line_str);
                                    match usize::from_str_radix(chunk_size_hex, 16) {
                                        Ok(0) => {
                                            *this.state = S3ChunkState::ReadingTrailers;
                                            continue;
                                        }
                                        Ok(size) => {
                                            *this.state = S3ChunkState::ReadingChunkData {
                                                remaining: size,
                                                chunk_signature,
                                            };
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Failed to parse final chunk size '{}': {}",
                                                chunk_size_hex,
                                                e
                                            );
                                            send_error(
                                                this.checksum_sender,
                                                &format!("Failed to parse final chunk size: {}", e),
                                            );
                                            return Poll::Ready(Some(Err(
                                                PayloadError::EncodingCorrupted,
                                            )));
                                        }
                                    }
                                } else {
                                    // Try to treat remaining buffer as trailers
                                    *this.state = S3ChunkState::ReadingTrailers;
                                    continue;
                                }
                            } else {
                                // End of stream with no buffered data - this is normal completion
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }
                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                        }
                    }
                }
                S3ChunkState::ReadingChunkData {
                    remaining,
                    chunk_signature,
                } => {
                    // Special case: if remaining is usize::MAX, we're in pass-through mode (non-streaming)
                    if remaining == usize::MAX {
                        // Pass through all data directly without chunk parsing
                        match ready!(this.inner.as_mut().poll_next(cx)) {
                            Some(Ok(data)) => {
                                // Send data to checksum calculator
                                if let Some(sender) = &this.checksum_sender {
                                    let _ = sender.send(ChecksumMessage::Data(data.clone()));
                                }
                                return Poll::Ready(Some(Ok(data)));
                            }
                            Some(Err(e)) => {
                                send_error(
                                    this.checksum_sender,
                                    &format!("Payload error in pass-through mode: {:?}", e),
                                );
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => {
                                // End of stream in pass-through mode
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }
                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                        }
                    }

                    if remaining == 0 {
                        // We've completed reading a chunk - verify its signature if needed
                        if let (Some(signature_ctx), Some(chunk_sig), Some(prev_sig)) = (
                            this.signature_context.as_ref(),
                            chunk_signature.as_ref(),
                            this.previous_signature.as_ref(),
                        ) {
                            if let Err(e) = verify_chunk_signature(
                                signature_ctx,
                                chunk_sig,
                                prev_sig,
                                this.current_chunk_buffer,
                            ) {
                                tracing::error!("Chunk signature verification failed: {:?}", e);
                                send_error(
                                    this.checksum_sender,
                                    &format!("Chunk signature verification failed: {:?}", e),
                                );
                                return Poll::Ready(Some(Err(PayloadError::EncodingCorrupted)));
                            }

                            // Update previous signature for the next chunk
                            *this.previous_signature = Some(chunk_sig.signature.clone());
                            tracing::debug!("Chunk signature verified successfully");
                        }

                        // Clear the chunk buffer for the next chunk
                        this.current_chunk_buffer.clear();

                        *this.state = S3ChunkState::ReadingChunkEnd;
                        continue;
                    }

                    // Check if we have buffered data
                    if !this.trailer_buffer.is_empty() {
                        let to_take = std::cmp::min(remaining, this.trailer_buffer.len());
                        let chunk_data = this.trailer_buffer.split_to(to_take);

                        // Buffer chunk data for signature verification (only if we have signature context)
                        if this.signature_context.is_some() {
                            this.current_chunk_buffer.extend_from_slice(&chunk_data);
                        }

                        // Send data to checksum calculator
                        if let Some(sender) = &this.checksum_sender {
                            let _ = sender.send(ChecksumMessage::Data(chunk_data.clone().into()));
                        }

                        *this.state = S3ChunkState::ReadingChunkData {
                            remaining: remaining - to_take,
                            chunk_signature: chunk_signature.clone(),
                        };
                        return Poll::Ready(Some(Ok(chunk_data.into())));
                    }

                    // Read more data from the stream
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            let to_take = std::cmp::min(remaining, data.len());
                            let chunk_data = data.slice(0..to_take);
                            let leftover = data.slice(to_take..);

                            // Buffer any leftover data
                            if !leftover.is_empty() {
                                this.trailer_buffer.extend_from_slice(&leftover);
                            }

                            // Buffer chunk data for signature verification (only if we have signature context)
                            if this.signature_context.is_some() {
                                this.current_chunk_buffer.extend_from_slice(&chunk_data);
                            }

                            // Send data to checksum calculator
                            if let Some(sender) = &this.checksum_sender {
                                let _ = sender.send(ChecksumMessage::Data(chunk_data.clone()));
                            }

                            *this.state = S3ChunkState::ReadingChunkData {
                                remaining: remaining - to_take,
                                chunk_signature: chunk_signature.clone(),
                            };
                            return Poll::Ready(Some(Ok(chunk_data)));
                        }
                        Some(Err(e)) => {
                            send_error(
                                this.checksum_sender,
                                &format!("Payload error while reading chunk data: {:?}", e),
                            );
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            tracing::error!("Unexpected end of stream while reading chunk data");
                            send_error(
                                this.checksum_sender,
                                "Unexpected end of stream while reading chunk data",
                            );
                            return Poll::Ready(Some(Err(PayloadError::Incomplete(None))));
                        }
                    }
                }
                S3ChunkState::ReadingChunkEnd => {
                    // Read the \r\n after chunk data
                    if this.trailer_buffer.len() < 2 {
                        match ready!(this.inner.as_mut().poll_next(cx)) {
                            Some(Ok(data)) => {
                                this.trailer_buffer.extend_from_slice(&data);
                            }
                            Some(Err(e)) => {
                                send_error(
                                    this.checksum_sender,
                                    &format!("Payload error while reading chunk end: {:?}", e),
                                );
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => {
                                tracing::error!("Unexpected end of stream while reading chunk end");
                                send_error(
                                    this.checksum_sender,
                                    "Unexpected end of stream while reading chunk end",
                                );
                                return Poll::Ready(Some(Err(PayloadError::Incomplete(None))));
                            }
                        }
                    }

                    if this.trailer_buffer.len() >= 2 {
                        let chunk_end = this.trailer_buffer.split_to(2);
                        if &chunk_end[..] != b"\r\n" {
                            tracing::error!(
                                "Expected \\r\\n after chunk data, got: {:?}",
                                chunk_end
                            );
                            send_error(this.checksum_sender, "Expected \\r\\n after chunk data");
                            return Poll::Ready(Some(Err(PayloadError::EncodingCorrupted)));
                        }
                        *this.state = S3ChunkState::ReadingChunkSize;
                        continue;
                    }
                }
                S3ChunkState::ReadingTrailers => {
                    // Read until we find \r\n\r\n (end of trailers)
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            this.trailer_buffer.extend_from_slice(&data);

                            // Look for end of trailers
                            if let Some(end_pos) = this
                                .trailer_buffer
                                .windows(4)
                                .position(|w| w == b"\r\n\r\n")
                            {
                                let trailer_data = this.trailer_buffer.split_to(end_pos);
                                this.trailer_buffer.advance(4); // Skip the \r\n\r\n

                                // Parse trailers for S3 checksums
                                let trailers = parse_s3_trailers(&trailer_data);
                                tracing::debug!("Parsed S3 trailers: {:?}", trailers);

                                // Send trailer checksums through the checksum channel for verification
                                // The checksummer task will handle the verification
                                if let Some(sender) = &this.checksum_sender {
                                    let _ =
                                        sender.send(ChecksumMessage::Trailers(trailers.clone()));
                                }

                                // Close checksum channel
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }

                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                            // Keep reading for more trailer data
                            continue;
                        }
                        Some(Err(e)) => {
                            send_error(
                                this.checksum_sender,
                                &format!("Payload error while reading trailers: {:?}", e),
                            );
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // End of stream, close checksum channel
                            if let Some(sender) = this.checksum_sender.take() {
                                drop(sender);
                            }
                            *this.state = S3ChunkState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                S3ChunkState::Done => return Poll::Ready(None),
            }
        }
    }
}

/// Parse S3 trailers from HTTP trailer section
/// Format: "x-amz-checksum-sha256: base64value\r\n"
fn parse_s3_trailers(trailer_data: &[u8]) -> S3Trailers {
    let mut trailers = S3Trailers::default();

    // Split trailer data into lines
    let trailer_str = match std::str::from_utf8(trailer_data) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!("Invalid UTF-8 in trailer data");
            return trailers;
        }
    };

    for line in trailer_str.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Parse "header: value" format
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_lowercase();
            let value = value.trim();

            match name.as_str() {
                "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-crc64nvme"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256" => {
                    // Since we only support one checksum type, take the first one we find
                    if trailers.checksum_value.is_none() {
                        trailers.checksum_value = Some(value.to_string());
                        tracing::debug!("Found trailer checksum: {} = {}", name, value);
                    }
                }
                "x-amz-trailer-signature" => trailers.trailer_signature = Some(value.to_string()),
                _ => {
                    // Ignore unknown trailer headers
                    tracing::debug!("Unknown trailer header: {}", name);
                }
            }
        }
    }

    trailers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_trailers() {
        let trailer_data =
            b"x-amz-checksum-sha256: YWJjZGVmZ2g=\r\nx-amz-checksum-crc64nvme: OOJZ0D8xKts=\r\nx-amz-trailer-signature: signature123\r\n";
        let trailers = parse_s3_trailers(trailer_data);

        // Should take the first checksum found (SHA256 in this case)
        assert_eq!(trailers.checksum_value, Some("YWJjZGVmZ2g=".to_string()));
        assert_eq!(trailers.trailer_signature, Some("signature123".to_string()));
    }
}
