//! Android JNI bridge for the embedded LinkSpool host.
//!
//! The Java class `dev.linkspool.app.NativeHost` declares the five static
//! native methods registered by [`rivium_jni::export!`]. Shared text reaches the
//! Rust core through the inbox directory, not through an additional JNI surface.

rivium_jni::export!(class = "dev/linkspool/app/NativeHost", app = linkspool::app::App);
