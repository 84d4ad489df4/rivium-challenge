package dev.linkspool.app

import android.content.Context

/** Small persisted settings surface for the receiver address. */
object Settings {
    const val DEFAULT_SINK = "10.0.2.2:9000"

    private const val PREFS = "linkspool"
    private const val SINK = "sink_addr"

    fun sink(context: Context): String =
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .getString(SINK, DEFAULT_SINK)
            .orEmpty()
            .ifBlank { DEFAULT_SINK }

    fun setSink(context: Context, value: String) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit()
            .putString(SINK, value.trim())
            .apply()
    }

    /** Accept `host:port` and `[ipv6]:port`; the Rust transport validates the rest. */
    fun isValidSink(value: String): Boolean {
        val separator = value.lastIndexOf(':')
        if (separator <= 0 || separator == value.lastIndex) {
            return false
        }
        val port = value.substring(separator + 1).toIntOrNull() ?: return false
        return port in 1..65535
    }
}
