fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = ["common", "cart", "checkout", "order", "payment"]
        .map(|name| format!("proto/commerce/v1/{name}.proto"));
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }
    let descriptors = protox::compile(&files, ["proto"])?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
