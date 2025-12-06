#[cfg(test)]
mod tests {
    use crate::index::Index;
    use proptest::prelude::*;
    use tempfile::tempdir;

    proptest! {
        #[test]
        fn test_index_lookup_correctness(
            // Generate a vector of (offset_delta, position_delta) tuples
            // We use deltas to ensure strictly increasing values
            entries in proptest::collection::vec((1u32..100, 1u32..1000), 1..100)
        ) {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("test.index");
            // Index::new takes a path, not a File object
            let mut index = Index::new(&file_path, 0).unwrap();
            let mut expected_entries = Vec::new();

            let mut current_offset = 0;
            let mut current_position = 0;

            // 1. Build the index
            for (off_delta, pos_delta) in entries {
                current_offset += off_delta;
                current_position += pos_delta;

                index.append(current_offset, current_position).unwrap();
                expected_entries.push((current_offset, current_position));
            }

            // 2. Verify lookups for exact matches
            for (off, pos) in &expected_entries {
                let result = index.lookup(*off);
                assert_eq!(result, Some(*pos), "Lookup failed for offset {}", off);
            }

            // 3. Verify lookups for gaps (should invoke floor behavior)
            // If we lookup offset X where entry[i].offset < X < entry[i+1].offset,
            // we should get entry[i].position.
            for i in 0..expected_entries.len() - 1 {
                let (off1, pos1) = expected_entries[i];
                let (off2, _) = expected_entries[i+1];

                if off2 > off1 + 1 {
                    let search_off = off1 + 1;
                    let result = index.lookup(search_off);
                    assert_eq!(result, Some(pos1), "Gap lookup failed at search_off {}", search_off);
                }
            }
        }
    }
}
