#[test]
fn openapi_json_is_synced() {
    let expected_json = serde_json::to_value(vector_store::httproutes::api()).unwrap();

    let file_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string("api/openapi.json").unwrap()).unwrap();

    assert_eq!(
        file_json, expected_json,
        "api/openapi.json is not in sync with the server's OpenAPI spec. Run `cargo openapi` to update it."
    );
}
