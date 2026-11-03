package dev.linkspool.app

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import org.json.JSONObject
import java.io.File

/** Control surface and live queue counters for the embedded host. */
class MainActivity : Activity() {
    private val handler = Handler(Looper.getMainLooper())

    private lateinit var statusView: TextView
    private lateinit var versionView: TextView
    private lateinit var errorView: TextView
    private lateinit var sinkView: EditText
    private lateinit var messageView: EditText
    private lateinit var statsView: TextView

    private val refreshTask = object : Runnable {
        override fun run() {
            refresh()
            handler.postDelayed(this, REFRESH_INTERVAL_MS)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        statusView = findViewById(R.id.status)
        versionView = findViewById(R.id.version)
        errorView = findViewById(R.id.last_error)
        sinkView = findViewById(R.id.sink)
        messageView = findViewById(R.id.message)
        statsView = findViewById(R.id.stats)

        sinkView.setText(Settings.sink(this))
        findViewById<Button>(R.id.start).setOnClickListener { startHost() }
        findViewById<Button>(R.id.stop).setOnClickListener { stopHost() }
        findViewById<Button>(R.id.queue).setOnClickListener { queueMessage() }

        requestNotificationPermission()
        if (savedInstanceState == null) {
            startHost()
        }
    }

    override fun onResume() {
        super.onResume()
        handler.post(refreshTask)
    }

    override fun onPause() {
        handler.removeCallbacks(refreshTask)
        super.onPause()
    }

    private fun startHost() {
        val sink = sinkView.text.toString().trim()
        if (!Settings.isValidSink(sink)) {
            toast(getString(R.string.invalid_sink))
            return
        }
        Settings.setSink(this, sink)
        startForegroundService(Intent(this, HostService::class.java))
        toast(getString(R.string.starting))
    }

    private fun stopHost() {
        stopService(Intent(this, HostService::class.java))
        toast(getString(R.string.stopping))
    }

    private fun queueMessage() {
        val text = messageView.text.toString().trim()
        if (text.isEmpty()) {
            toast(getString(R.string.empty_message))
            return
        }
        try {
            val file = Inbox.enqueue(this, text)
            startForegroundService(Intent(this, HostService::class.java))
            messageView.setText("")
            toast(getString(R.string.queued, file.nameWithoutExtension))
        } catch (error: Exception) {
            val message = error.message ?: getString(R.string.unknown_error)
            toast(getString(R.string.queue_failed, message))
        }
    }

    private fun refresh() {
        val status = HostController.status()
        statusView.text = getString(R.string.status_format, statusName(status))
        versionView.text = getString(R.string.version_format, HostController.version())

        val error = HostController.lastError()
        errorView.visibility = if (error.isBlank()) View.GONE else View.VISIBLE
        errorView.text = error

        statsView.text = readStats()
    }

    private fun readStats(): String {
        val file = File(filesDir, "status.json")
        if (!file.isFile) {
            return getString(R.string.no_messages)
        }
        return try {
            val snapshot = JSONObject(file.readText())
            val counts = snapshot.optJSONObject("counts") ?: JSONObject()
            val queued = counts.optInt("pending") +
                counts.optInt("delivering") +
                counts.optInt("retry_scheduled")
            getString(
                R.string.stats_format,
                queued,
                counts.optInt("delivered"),
                counts.optInt("dead_lettered"),
                snapshot.optInt("total"),
            )
        } catch (_: Exception) {
            getString(R.string.status_unreadable)
        }
    }

    private fun statusName(status: Int): String = when (status) {
        HostController.STATUS_IDLE -> getString(R.string.status_idle)
        HostController.STATUS_STARTING -> getString(R.string.status_starting)
        HostController.STATUS_RUNNING -> getString(R.string.status_running)
        HostController.STATUS_STOPPING -> getString(R.string.status_stopping)
        HostController.STATUS_RESTARTING -> getString(R.string.status_restarting)
        else -> getString(R.string.status_failed)
    }

    private fun requestNotificationPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) !=
            PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    private fun toast(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
    }

    private companion object {
        const val REFRESH_INTERVAL_MS = 1_000L
    }
}
