package dev.linkspool.app

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.widget.Toast

/**
 * Share Sheet entry point for `text/plain` content.
 *
 * The shared text is written durably before the host is asked to start, so a
 * failed or delayed start cannot lose the user's link.
 */
class ShareActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val text = intent?.getStringExtra(Intent.EXTRA_TEXT)?.trim().orEmpty()
        if (text.isEmpty()) {
            Toast.makeText(this, R.string.share_empty, Toast.LENGTH_SHORT).show()
            finish()
            return
        }

        try {
            Inbox.enqueue(this, text)
            startForegroundService(Intent(this, HostService::class.java))
            Toast.makeText(this, R.string.share_queued, Toast.LENGTH_SHORT).show()
        } catch (error: Exception) {
            val message = error.message ?: getString(R.string.unknown_error)
            Toast.makeText(this, getString(R.string.share_failed, message), Toast.LENGTH_LONG)
                .show()
        }
        finish()
    }
}
