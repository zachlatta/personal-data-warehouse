package expo.modules.pdwchatlauncher

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.util.Log
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition

/**
 * Opens a chat in the phone's messaging app from a tapped PDW alert or a
 * timeline row (src/lib/deep-link.ts). React Native's Linking can only open a
 * URL, and BlueBubbles opens a chat (and a group chat at all) only from an
 * intent extra. The payload comes from a push, so only two apps and two
 * shapes are accepted here, whatever the JavaScript side passed.
 *
 * Resolves false when the app is missing or refuses the intent; the caller
 * then tries the link's URL and finally stays in PDW.
 */
class PdwChatLauncherModule : Module() {
  override fun definition() = ModuleDefinition {
    Name("PdwChatLauncher")

    AsyncFunction("openChat") { app: String, chatGuid: String, recipients: String ->
      val context = appContext.currentActivity ?: appContext.reactContext
      if (context == null) false else open(context, app, chatGuid, recipients)
    }
  }

  private fun open(context: Context, app: String, chatGuid: String, recipients: String): Boolean {
    val intent = intentFor(app, chatGuid, recipients) ?: return false
    return try {
      context.startActivity(intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
      true
    } catch (error: ActivityNotFoundException) {
      Log.i(TAG, "$app is not installed", error)
      false
    } catch (error: SecurityException) {
      Log.w(TAG, "$app refused the chat intent", error)
      false
    }
  }

  companion object {
    private const val TAG = "PdwChatLauncher"
    const val BLUEBUBBLES = "com.bluebubbles.messaging"
    const val GOOGLE_MESSAGES = "com.google.android.apps.messaging"

    // chat.db's guid, "<service>;<-|+>;<identifier>": BlueBubbles' chat guid.
    private val CHAT_GUID = Regex("^[A-Za-z]{2,16};[-+];[^\\x00-\\x1f\\x7f;]{1,200}$")
    private val RECIPIENT = Regex("^(\\+?[0-9]{3,20}|[A-Za-z0-9._+-]+@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)+)$")
    private const val MAX_RECIPIENTS = 20

    fun intentFor(app: String, chatGuid: String, recipients: String): Intent? = when (app) {
      // The intent BlueBubbles' own message notification sends
      // (CreateIncomingMessageNotification.kt): MainActivity with the chat's
      // guid, which its IntentsService opens for 1:1 and group chats alike. A
      // launcher intent would not do: with BlueBubbles already running, Android
      // brings its task forward and drops the extra.
      "bluebubbles" -> if (!CHAT_GUID.matches(chatGuid)) null else
        Intent()
          .setClassName(BLUEBUBBLES, "$BLUEBUBBLES.MainActivity")
          .setType("OpenChat")
          .putExtra("chatGuid", chatGuid)
          .putExtra("bubble", false)
      // sms:<members> is Google Messages' own conversation URL; with no members
      // (an RCS group's key is opaque) it opens the conversation list.
      "google_messages" -> when {
        recipients.isEmpty() ->
          Intent(Intent.ACTION_MAIN).addCategory(Intent.CATEGORY_LAUNCHER).setPackage(GOOGLE_MESSAGES)
        recipients.split(',').let { members -> members.size <= MAX_RECIPIENTS && members.all { RECIPIENT.matches(it) } } ->
          Intent(Intent.ACTION_VIEW, Uri.parse("sms:$recipients")).setPackage(GOOGLE_MESSAGES)
        else -> null
      }
      else -> null
    }
  }
}
