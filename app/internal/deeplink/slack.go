// Package deeplink builds the URLs that open a synced record in the app it
// came from.
//
// It exists so that two surfaces cannot disagree about one source's link
// shape. The timeline attaches a link to every row; the mutation review
// attaches one to every Slack message it asks Zach to mark read, because the
// answer to "should this be marked read?" is often "let me reply to it first".
// A Slack permalink written twice is a Slack permalink that drifts, and the
// part that drifts silently is the thread query string: without it Slack opens
// the channel at the parent and the reply is nowhere on screen.
package deeplink

import (
	"net/url"
	"strings"
)

// Link is the JSON an app client needs to open a record: url works anywhere a
// browser does, app_url is the native scheme a phone should try first.
type Link struct {
	URL    string `json:"url"`
	Label  string `json:"label"`
	AppURL string `json:"app_url,omitempty"`
}

// Slack returns the link to one message. threadTS is that message's thread
// when it is a reply, and is ignored when it equals the message itself.
// domain is the workspace's slack.com subdomain; without it the link falls
// back to the client URL, which routes by team id alone.
func Slack(teamID, conversationID, messageTS, threadTS, domain string) *Link {
	teamID = strings.TrimSpace(teamID)
	conversationID = strings.TrimSpace(conversationID)
	messageTS = strings.TrimSpace(messageTS)
	if teamID == "" || conversationID == "" || messageTS == "" {
		return nil
	}
	permalinkTS := "p" + strings.ReplaceAll(messageTS, ".", "")
	link := &Link{
		Label:  "Slack",
		AppURL: "slack://channel?team=" + url.QueryEscape(teamID) + "&id=" + url.QueryEscape(conversationID) + "&message=" + url.QueryEscape(messageTS),
	}
	if domain = strings.TrimSpace(domain); domain != "" {
		link.URL = "https://" + domain + ".slack.com/archives/" + url.PathEscape(conversationID) + "/" + permalinkTS
		if threadTS = strings.TrimSpace(threadTS); threadTS != "" && threadTS != messageTS {
			link.URL += "?thread_ts=" + url.QueryEscape(threadTS) + "&cid=" + url.QueryEscape(conversationID)
		}
		return link
	}
	link.URL = "https://app.slack.com/client/" + url.PathEscape(teamID) + "/" + url.PathEscape(conversationID) + "/" + permalinkTS
	return link
}
