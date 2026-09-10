use test_common::*;

const KEYS: [&str; 8] = ["a", "a/a", "a/b", "a/c", "a/d/a", "a/é", "b", "c"];
const KEYS_MULTIPART: [&str; 5] = ["a", "a", "c", "c/a", "c/b"];

#[tokio::test]
async fn test_listobjectsv2() {
    let ctx = context();
    let bucket = ctx.create_bucket("listobjectsv2").await;

    for k in KEYS {
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(k)
            .send()
            .await
            .unwrap();
    }

    {
        // Scoping the variable to avoid reusing it
        // in a following assert due to copy paste
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 8);
        assert!(r.common_prefixes.is_none());
    }

    {
        // With 2 elements
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(2)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 2);
        assert!(r.common_prefixes.is_none());
        assert!(r.next_continuation_token.is_some());
    }

    {
        // With pagination
        let mut cnt = 0;
        let mut next = None;
        let last_idx = KEYS.len() - 1;

        for i in 0..KEYS.len() {
            let r = ctx
                .client
                .list_objects_v2()
                .bucket(&bucket)
                .set_continuation_token(next)
                .max_keys(1)
                .send()
                .await
                .unwrap();

            cnt += 1;
            next = r.next_continuation_token;

            assert_eq!(r.contents.unwrap().len(), 1);
            assert!(r.common_prefixes.is_none());
            if i != last_idx {
                assert!(next.is_some());
            }
        }
        assert_eq!(cnt, KEYS.len());
    }

    {
        // With a delimiter
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 3);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }

    {
        // With a delimiter and pagination
        let mut cnt_pfx = 0;
        let mut cnt_key = 0;
        let mut next = None;

        for _i in 0..KEYS.len() {
            let r = ctx
                .client
                .list_objects_v2()
                .bucket(&bucket)
                .set_continuation_token(next)
                .delimiter("/")
                .max_keys(1)
                .send()
                .await
                .unwrap();

            next = r.next_continuation_token;
            match (r.contents, r.common_prefixes) {
                (Some(k), None) if k.len() == 1 => cnt_key += 1,
                (None, Some(pfx)) if pfx.len() == 1 => cnt_pfx += 1,
                (None, None) => {}
                _ => unreachable!("logic error"),
            };
            if next.is_none() {
                break;
            }
        }
        assert_eq!(cnt_key, 3);
        assert_eq!(cnt_pfx, 1);
    }

    {
        // With a prefix
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("a/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 5);
        assert!(r.common_prefixes.is_none());
    }

    {
        // With a prefix and a delimiter
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("a/")
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 4);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }

    {
        // With a prefix, a delimiter and max_key
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("a/")
            .delimiter("/")
            .max_keys(1)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.as_ref().unwrap().len(), 1);
        assert_eq!(
            r.contents
                .unwrap()
                .first()
                .unwrap()
                .key
                .as_ref()
                .unwrap()
                .as_str(),
            "a/a"
        );
        assert!(r.common_prefixes.is_none());
    }
    {
        // With start_after before all keys
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .start_after("Z")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 8);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With start_after after all keys
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .start_after("c")
            .send()
            .await
            .unwrap();

        assert!(r.contents.is_none());
        assert!(r.common_prefixes.is_none());
    }
}

#[tokio::test]
async fn test_listobjectsv1() {
    let ctx = context();
    let bucket = ctx.create_bucket("listobjects").await;

    for k in KEYS {
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(k)
            .send()
            .await
            .unwrap();
    }

    {
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 8);
        assert!(r.common_prefixes.is_none());
    }

    {
        // With 2 elements
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .max_keys(2)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 2);
        assert!(r.common_prefixes.is_none());
        assert!(r.next_marker.is_some());
    }

    {
        // With pagination
        let mut cnt = 0;
        let mut next = None;
        let last_idx = KEYS.len() - 1;

        for i in 0..KEYS.len() {
            let r = ctx
                .client
                .list_objects()
                .bucket(&bucket)
                .set_marker(next)
                .max_keys(1)
                .send()
                .await
                .unwrap();

            cnt += 1;
            next = r.next_marker;

            assert_eq!(r.contents.unwrap().len(), 1);
            assert!(r.common_prefixes.is_none());
            if i != last_idx {
                assert!(next.is_some());
            }
        }
        assert_eq!(cnt, KEYS.len());
    }

    {
        // With a delimiter
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 3);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }

    {
        // With a delimiter and pagination
        let mut cnt_pfx = 0;
        let mut cnt_key = 0;
        let mut next = None;

        for _i in 0..KEYS.len() {
            let r = ctx
                .client
                .list_objects()
                .bucket(&bucket)
                .delimiter("/")
                .set_marker(next)
                .max_keys(1)
                .send()
                .await
                .unwrap();

            next = r.next_marker;
            match (r.contents, r.common_prefixes) {
                (Some(k), None) if k.len() == 1 => cnt_key += 1,
                (None, Some(pfx)) if pfx.len() == 1 => cnt_pfx += 1,
                (None, None) => {}
                _ => unreachable!("logic error"),
            };
            if next.is_none() {
                break;
            }
        }
        assert_eq!(cnt_key, 3);
        assert_eq!(cnt_pfx, 1);
    }

    {
        // With a prefix
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .prefix("a/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 5);
        assert!(r.common_prefixes.is_none());
    }

    {
        // With a prefix and a delimiter
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .prefix("a/")
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 4);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }

    {
        // With a prefix, a delimiter and max_key
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .prefix("a/")
            .delimiter("/")
            .max_keys(1)
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.as_ref().unwrap().len(), 1);
        assert_eq!(
            r.contents
                .unwrap()
                .first()
                .unwrap()
                .key
                .as_ref()
                .unwrap()
                .as_str(),
            "a/a"
        );
        assert!(r.common_prefixes.is_none());
    }
    {
        // With marker before all keys
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .marker("Z")
            .send()
            .await
            .unwrap();

        assert_eq!(r.contents.unwrap().len(), 8);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With start_after after all keys
        let r = ctx
            .client
            .list_objects()
            .bucket(&bucket)
            .marker("c")
            .send()
            .await
            .unwrap();

        assert!(r.contents.is_none());
        assert!(r.common_prefixes.is_none());
    }
}

#[allow(dead_code)]
async fn test_listmultipart() {
    let ctx = context();
    let bucket = ctx.create_bucket("listmultipartuploads").await;

    for k in KEYS_MULTIPART {
        ctx.client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(k)
            .send()
            .await
            .unwrap();
    }

    {
        // Default
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 5);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With pagination
        let mut next = None;
        let mut upnext = None;
        let last_idx = KEYS_MULTIPART.len() - 1;

        for i in 0..KEYS_MULTIPART.len() {
            let r = ctx
                .client
                .list_multipart_uploads()
                .bucket(&bucket)
                .set_key_marker(next)
                .set_upload_id_marker(upnext)
                .max_uploads(1)
                .send()
                .await
                .unwrap();

            next = r.next_key_marker;
            upnext = r.next_upload_id_marker;

            assert_eq!(r.uploads.unwrap().len(), 1);
            assert!(r.common_prefixes.is_none());
            if i != last_idx {
                assert!(next.is_some());
            }
        }
    }
    {
        // With delimiter
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 3);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }
    {
        // With delimiter and pagination
        let mut next = None;
        let mut upnext = None;
        let mut upcnt = 0;
        let mut pfxcnt = 0;
        let mut loopcnt = 0;

        while loopcnt < KEYS_MULTIPART.len() {
            let r = ctx
                .client
                .list_multipart_uploads()
                .bucket(&bucket)
                .delimiter("/")
                .max_uploads(1)
                .set_key_marker(next)
                .set_upload_id_marker(upnext)
                .send()
                .await
                .unwrap();

            next = r.next_key_marker;
            upnext = r.next_upload_id_marker;

            loopcnt += 1;
            upcnt += r.uploads.unwrap_or_default().len();
            pfxcnt += r.common_prefixes.unwrap_or_default().len();

            if next.is_none() {
                break;
            }
        }

        assert_eq!(upcnt + pfxcnt, loopcnt);
        assert_eq!(upcnt, 3);
        assert_eq!(pfxcnt, 1);
    }
    {
        // With prefix
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("c")
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 3);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With prefix and delimiter
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("c")
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 1);
        assert_eq!(r.common_prefixes.unwrap().len(), 1);
    }
    {
        // With prefix, delimiter and max keys
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("c")
            .delimiter("/")
            .max_uploads(1)
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 1);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With starting token before the first element
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker("ZZZZZ")
            .send()
            .await
            .unwrap();

        assert_eq!(r.uploads.unwrap().len(), 5);
        assert!(r.common_prefixes.is_none());
    }
    {
        // With starting token after the last element
        let r = ctx
            .client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker("d")
            .send()
            .await
            .unwrap();

        assert!(r.uploads.is_none());
        assert!(r.common_prefixes.is_none());
    }
}

#[tokio::test]
async fn test_list_buckets() {
    let ctx = context();

    // Create multiple buckets
    let bucket1 = ctx.create_bucket("list-test-bucket-1").await;
    let bucket2 = ctx.create_bucket("list-test-bucket-2").await;
    let bucket3 = ctx.create_bucket("list-test-bucket-3").await;

    // List all buckets
    let result = ctx.client.list_buckets().send().await.unwrap();

    // Verify all buckets are returned
    let buckets = result.buckets.unwrap();
    let bucket_names: Vec<String> = buckets
        .iter()
        .map(|b| b.name.as_ref().unwrap().clone())
        .collect();

    assert!(bucket_names.contains(&bucket1));
    assert!(bucket_names.contains(&bucket2));
    assert!(bucket_names.contains(&bucket3));

    // Verify bucket count is at least 3 (there might be other buckets from other tests)
    assert!(bucket_names.len() >= 3);

    // Clean up created buckets
    ctx.delete_bucket(&bucket1).await;
    ctx.delete_bucket(&bucket2).await;
    ctx.delete_bucket(&bucket3).await;
}

#[allow(dead_code)]
async fn test_multichar_delimiter() {
    // Test case from dpape from issue #692 with reference results from Amazon

    let ctx = context();
    let bucket = ctx.create_bucket("multichardelim").await;

    for k in [
        "a/", "a/b/", "a/b/c/", "a/b/c/d", "a/c/", "a/c/b/", "a/c/b/e",
    ] {
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(k)
            .send()
            .await
            .unwrap();
    }

    // With delimiter /
    {
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert!(r.contents.is_none());

        let common_prefixes = r.common_prefixes.unwrap();
        assert_eq!(common_prefixes.len(), 1);
        assert_eq!(common_prefixes[0].prefix.as_deref().unwrap(), "a/");
    }

    // With delimiter b/
    {
        let r = ctx
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("b/")
            .send()
            .await
            .unwrap();

        let contents = r.contents.unwrap();
        assert_eq!(contents.len(), 2);
        assert_eq!(contents[0].key.as_deref().unwrap(), "a/");
        assert_eq!(contents[1].key.as_deref().unwrap(), "a/c/");

        let common_prefixes = r.common_prefixes.unwrap();
        assert_eq!(common_prefixes.len(), 2);
        assert_eq!(common_prefixes[0].prefix.as_deref().unwrap(), "a/b/");
        assert_eq!(common_prefixes[1].prefix.as_deref().unwrap(), "a/c/b/");
    }
}
