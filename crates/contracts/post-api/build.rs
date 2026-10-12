//! Compiles the post.v1 contract into server + client stubs plus a reflection
//! descriptor set, from the shared contracts/proto root (the single IDL source).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("post_descriptor.bin");

    tonic_prost_build::configure()
        // #907: attachments carry media.v1's DeliveredMedia — the media-api
        // crate's type, not a copy.
        .extern_path(".media.v1", "::media_api")
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(
            &[
                "../proto/post/v1/enums.proto",
                "../proto/post/v1/messages.proto",
                "../proto/post/v1/service.proto",
            ],
            &["../proto/"],
        )?;
    Ok(())
}
