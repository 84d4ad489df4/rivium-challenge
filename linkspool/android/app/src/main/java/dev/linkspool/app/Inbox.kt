package dev.linkspool.app

import android.content.Context
import java.io.File
import java.io.IOException
import java.util.UUID

/**
 * Durable file-drop producer used by the Share Sheet and the compose field.
 *
 * A file is written under a temporary name and renamed into place, so the
 * inbox service never observes a partial message. The file stem becomes the
 * idempotency key, which makes a crash between ingestion and deletion safe.
 */
object Inbox {
    fun enqueue(context: Context, text: String): File {
        val directory = File(context.filesDir, "inbox")
        if (!directory.isDirectory && !directory.mkdirs()) {
            throw IOException("cannot create inbox directory")
        }

        val key = "share-${System.currentTimeMillis()}-${UUID.randomUUID()}"
        val temporary = File(directory, "$key.tmp")
        val target = File(directory, "$key.txt")

        temporary.writeText(text, Charsets.UTF_8)
        if (!temporary.renameTo(target)) {
            temporary.delete()
            throw IOException("cannot publish inbox file")
        }
        return target
    }
}
