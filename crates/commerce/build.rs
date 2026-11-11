fn main() {
    // SQLx embeds migrations; new migration files must invalidate the build too.
    println!("cargo:rerun-if-changed=../../migrations");
}
