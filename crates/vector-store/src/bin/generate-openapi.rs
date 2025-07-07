use std::env;
use std::fs;
use std::io::Write;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let default_path = String::from("openapi.json");
    let pathname = args.get(1).unwrap_or(&default_path);

    let json = serde_json::to_string_pretty(&vector_store::httproutes::api())?;

    fs::File::create(pathname)?.write(json.as_bytes())?;

    println!("OpenAPI specification written to {}", pathname);
    Ok(())
}
