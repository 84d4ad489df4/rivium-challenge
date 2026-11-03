package dev.linkspool.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.IBinder

/**
 * Keeps the embedded host alive while the UI is closed.
 *
 * The service is sticky: Android recreates it after a process kill, and the
 * SQLite message store plus the maintenance sweep recover unfinished work.
 */
class HostService : Service() {
    override fun onCreate() {
        super.onCreate()
        createChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(NOTIFICATION_ID, notification(getString(R.string.notification_starting)))
        HostController.start(this) { code ->
            val text = if (code >= 0) {
                getString(R.string.notification_running)
            } else {
                getString(R.string.notification_failed)
            }
            notify(text)
        }
        return START_STICKY
    }

    override fun onDestroy() {
        HostController.stop()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun createChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID,
            getString(R.string.notification_channel),
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            setShowBadge(false)
        }
        getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    }

    private fun notification(text: String): Notification =
        Notification.Builder(this, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentTitle(getString(R.string.app_name))
            .setContentText(text)
            .setOngoing(true)
            .build()

    private fun notify(text: String) {
        getSystemService(NotificationManager::class.java)
            .notify(NOTIFICATION_ID, notification(text))
    }

    private companion object {
        const val CHANNEL_ID = "linkspool-host"
        const val NOTIFICATION_ID = 1
    }
}
