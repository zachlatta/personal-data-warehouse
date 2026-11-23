package deeplink

// The Android messaging apps a chat link can name. The phone app
// (mobile/modules/pdw-chat-launcher) knows how to open each one, and opens
// nothing else: a push payload can pick one of these apps, never an arbitrary
// activity.
const (
	// AndroidAppBlueBubbles is BlueBubbles, where iMessage lives on the Pixel.
	// Its MainActivity opens the chat whose chat.db guid it is given.
	AndroidAppBlueBubbles = "bluebubbles"
	// AndroidAppGoogleMessages is Google Messages, where SMS, MMS and RCS
	// live. It opens a conversation by its members (sms:a,b); with no members
	// (an RCS group, whose key is opaque) it opens the conversation list.
	AndroidAppGoogleMessages = "google_messages"
)

// AndroidChat is a conversation the Android app opens in its messaging app
// directly, so a tap lands in the chat rather than in a URL handler's guess.
// React Native's Linking cannot attach an intent extra, which BlueBubbles needs
// to open a chat (and the only way it opens a group chat at all).
type AndroidChat struct {
	App string `json:"app"`
	// ChatGUID is the chat.db guid ("any;-;+15555550100", "iMessage;+;chat1"):
	// BlueBubbles' chat guid. Set for AndroidAppBlueBubbles.
	ChatGUID string `json:"chat_guid,omitempty"`
	// Recipients is the conversation's members, comma-separated, as sms:
	// takes them. Empty opens Google Messages' conversation list.
	Recipients string `json:"recipients,omitempty"`
}
