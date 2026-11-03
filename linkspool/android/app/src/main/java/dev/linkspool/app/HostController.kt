package dev.linkspool.app

import android.content.Context
import android.os.Handler
import android.os.Looper
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Process-wide owner of the embedded Rivium host.
 *
 * [NativeHost.nativeStart] blocks until every service has started, so all
 * lifecycle calls run on a single background thread. Status, version and error
 * reads are cheap and may be called from the UI thread.
 */
object HostController {
    const val STATUS_IDLE = 0
    const val STATUS_STARTING = 1
    const val STATUS_RUNNING = 2
    const val STATUS_STOPPING = 3
    const val STATUS_RESTARTING = 4
    const val STATUS_FAILED = 5

    private const val STOP_TIMEOUT_MS = 5_000L

    private val executor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "linkspool-host")
    }
    private val main = Handler(Looper.getMainLooper())
    private val starting = AtomicBoolean(false)

    /** Start the host if it is not already starting. */
    fun start(context: Context, onResult: ((Int) -> Unit)? = null) {
        if (!starting.compareAndSet(false, true)) {
            return
        }
        val app = context.applicationContext
        executor.execute {
            val code = runCatching { NativeHost.nativeStart(arguments(app)) }.getOrDefault(-1)
            starting.set(false)
            if (onResult != null) {
                main.post { onResult(code) }
            }
        }
    }

    /** Request shutdown without blocking the caller. */
    fun stop(onResult: ((Int) -> Unit)? = null) {
        executor.execute {
            val code = runCatching { NativeHost.nativeStop(STOP_TIMEOUT_MS) }.getOrDefault(-1)
            if (onResult != null) {
                main.post { onResult(code) }
            }
        }
    }

    fun status(): Int = runCatching { NativeHost.nativeStatus() }.getOrDefault(STATUS_FAILED)

    fun version(): String = runCatching { NativeHost.nativeVersion() }.getOrDefault("")

    fun lastError(): String = runCatching { NativeHost.nativeLastError() }.getOrDefault("")

    /**
     * Embedded arguments. The API stays disabled: Android submits through the
     * durable inbox, not through an unauthenticated loopback server.
     */
    private fun arguments(context: Context): Array<String> = arrayOf(
        "--root", context.filesDir.absolutePath,
        "--set", "api.enabled=false",
        "--set", "inbox.enabled=true",
        "--set", "inbox.path=inbox",
        "--set", "inbox.poll_interval_ms=250",
        "--set", "status.enabled=true",
        "--set", "status.path=status.json",
        "--set", "status.interval_ms=1000",
        "--set", "database.path=linkspool.db",
        "--set", "transport.sink_addr=${Settings.sink(context)}",
    )
}
