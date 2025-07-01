use vector_store::Quantization;

pub fn assert_contains(indexes: &[vector_store::IndexInfo], keyspace: &str, index: &str) {
    assert!(indexes.contains(&vector_store::IndexInfo {
        keyspace: String::from(keyspace).into(),
        index: String::from(index).into(),
        quantization: Quantization::F32,
    }));
}
