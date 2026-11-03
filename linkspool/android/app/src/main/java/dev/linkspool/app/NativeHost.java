package dev.linkspool.app;

/**
 * The fixed JNI ABI exported by the Rust {@code linkspool-jni} library.
 *
 * <p>The class name and method signatures must match
 * {@code rivium_jni::export!(class = "dev/linkspool/app/NativeHost", ...)}.
 */
public final class NativeHost {
    static {
        System.loadLibrary("linkspool_jni");
    }

    private NativeHost() {}

    /** Start the embedded Rivium host; returns a Rivium result code. */
    public static native int nativeStart(String[] args);

    /** Stop the host, waiting up to {@code timeoutMillis}. */
    public static native int nativeStop(long timeoutMillis);

    /** Current host status: idle=0, starting=1, running=2, stopping=3, restarting=4, failed=5. */
    public static native int nativeStatus();

    /** Safe summary of the last host error, or an empty string. */
    public static native String nativeLastError();

    /** Application name and version reported by the Rust crate. */
    public static native String nativeVersion();
}
