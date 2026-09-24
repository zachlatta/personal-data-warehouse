package mutations

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/deeplink"
)

// A Slack message is sent AS ZACH, through the client session that
// `pdw slack publish-session` publishes (the same xoxc + d-cookie pair the
// mark-read mutation spends), and only after a human approves it in the review
// UI. The proposal names the recipient exactly — a conversation id, or a user
// id for a direct message — and the executor (slack_mutations.py) re-resolves
// the account, the workspace and the recipient against the warehouse and the
// live API before it posts. Nothing here sends anything.

// slackMessageTextMaxLength is Slack's own advice for chat.postMessage text
// ("keep it under 4,000 characters"). Slack truncates a longer message with a
// warning that the executor could only discover after a human had approved
// the untruncated words, so the proposal is refused instead.
const slackMessageTextMaxLength = 4000

// slackSendPreviewContextLimit bounds the conversation context a send preview
// carries: enough to judge the reply, not a transcript.
const slackSendPreviewContextLimit = 6

const (
	slackSendDeliveryConversation = "conversation"
	slackSendDeliveryDM           = "dm"
	slackSendDeliveryThreadReply  = "thread_reply"
)

func isSlackSendMessageMutation(mutation Mutation) bool {
	return mutation.Provider == SlackProvider && mutation.Operation == SlackSendMessageOperation
}

func isStoredSlackSendMessageMutation(mutation storedMutation) bool {
	return mutation.Provider == SlackProvider && mutation.Operation == SlackSendMessageOperation
}

// validateSlackSendMessage rejects at proposal time what the executor would
// otherwise refuse after a human had approved it: an ambiguous recipient, no
// words, or a thread that names no conversation.
func validateSlackSendMessage(mutation MutationInput) error {
	conversationID := strings.TrimSpace(mutation.ConversationID)
	userID := strings.TrimSpace(mutation.UserID)
	threadTS := strings.TrimSpace(mutation.ThreadTS)
	switch {
	case conversationID == "" && userID == "":
		return errors.New("must include conversation_id (a Slack C, D, or G conversation ID) or user_id (a Slack U or W user ID to message directly)")
	case conversationID != "" && userID != "":
		return errors.New("must include conversation_id or user_id, not both")
	case conversationID != "" && !slackConversationIDPattern.MatchString(conversationID):
		return errors.New("conversation_id must be a Slack C, D, or G conversation ID")
	case userID != "" && !slackUserIDPattern.MatchString(userID):
		return errors.New("user_id must be a Slack U or W user ID (read it from base_slack.users.user_id)")
	}
	if err := validateSlackMessageText(mutation.Text); err != nil {
		return err
	}
	if threadTS != "" {
		if !slackMessageTSPattern.MatchString(threadTS) {
			return errors.New("thread_ts must be an exact Slack timestamp such as 1593473566.000200")
		}
		if conversationID == "" {
			return errors.New("thread_ts needs conversation_id: a thread lives in one conversation, so name the conversation rather than the person")
		}
	} else if mutation.ReplyBroadcast {
		return errors.New("reply_broadcast is only valid with thread_ts")
	}
	return nil
}

func validateSlackMessageText(text string) error {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return errors.New("must include text")
	}
	if utf8.RuneCountInString(trimmed) > slackMessageTextMaxLength {
		return fmt.Errorf("text must be at most %d characters; Slack truncates longer messages", slackMessageTextMaxLength)
	}
	return nil
}

func slackSendMessageDelivery(conversationID string, userID string, threadTS string) string {
	switch {
	case strings.TrimSpace(threadTS) != "":
		return slackSendDeliveryThreadReply
	case strings.TrimSpace(userID) != "" && strings.TrimSpace(conversationID) == "":
		return slackSendDeliveryDM
	default:
		return slackSendDeliveryConversation
	}
}

func slackSendMessagePayload(mutation MutationInput) map[string]any {
	threadTS := strings.TrimSpace(mutation.ThreadTS)
	return map[string]any{
		"conversation_id": strings.TrimSpace(mutation.ConversationID),
		"user_id":         strings.TrimSpace(mutation.UserID),
		"text":            strings.TrimSpace(mutation.Text),
		"thread_ts":       threadTS,
		"reply_broadcast": mutation.ReplyBroadcast && threadTS != "",
	}
}

// slackSendMessagePreview is the proposal-time preview: what the payload says,
// plus the effect in words. The recipient's name, the workspace and the
// conversation context are added by enrichSlackSendMessagePreviews when the
// warehouse holds them.
func slackSendMessagePreview(payload map[string]any) map[string]any {
	conversationID := strings.TrimSpace(stringFromAny(payload["conversation_id"]))
	userID := strings.TrimSpace(stringFromAny(payload["user_id"]))
	threadTS := strings.TrimSpace(stringFromAny(payload["thread_ts"]))
	replyBroadcast := boolFromAny(payload["reply_broadcast"])
	delivery := slackSendMessageDelivery(conversationID, userID, threadTS)
	return map[string]any{
		"conversation_id": conversationID,
		"user_id":         userID,
		"text":            stringFromAny(payload["text"]),
		"thread_ts":       threadTS,
		"reply_broadcast": replyBroadcast,
		"delivery":        delivery,
		"effect":          slackSendMessageEffect(delivery, replyBroadcast),
	}
}

func slackSendMessageEffect(delivery string, replyBroadcast bool) string {
	switch delivery {
	case slackSendDeliveryDM:
		return "Sends this message as you in a direct message to this person. Once approved it is posted and cannot be unsent by the warehouse."
	case slackSendDeliveryThreadReply:
		if replyBroadcast {
			return "Posts this message as you as a reply in the thread and also broadcasts it to the whole conversation. Once approved it is posted and cannot be unsent by the warehouse."
		}
		return "Posts this message as you as a reply in this thread. Once approved it is posted and cannot be unsent by the warehouse."
	default:
		return "Posts this message as you in this conversation. Once approved it is posted and cannot be unsent by the warehouse."
	}
}

// slackSendMessageTitle is the default request-row title before the recipient
// is resolved; enrichment replaces it with the person's or channel's name when
// the proposer left the title to us.
func slackSendMessageTitle(payload map[string]any) string {
	conversationID := strings.TrimSpace(stringFromAny(payload["conversation_id"]))
	userID := strings.TrimSpace(stringFromAny(payload["user_id"]))
	threadTS := strings.TrimSpace(stringFromAny(payload["thread_ts"]))
	switch slackSendMessageDelivery(conversationID, userID, threadTS) {
	case slackSendDeliveryDM:
		return "Send Slack DM to " + userID
	case slackSendDeliveryThreadReply:
		return "Reply in Slack thread in " + conversationID
	default:
		return "Send Slack message to " + conversationID
	}
}

func slackSendMessageTitleFor(delivery string, recipientLabel string) string {
	switch delivery {
	case slackSendDeliveryDM:
		return "Send Slack DM to " + recipientLabel
	case slackSendDeliveryThreadReply:
		return "Reply in Slack thread in " + recipientLabel
	default:
		return "Send Slack message to " + recipientLabel
	}
}

// updatedSlackMessagePayload applies a reviewer's edit. Only the text changes:
// the recipient and the thread were validated against the warehouse at
// proposal time, and the review UI has no way to re-run that check.
func updatedSlackMessagePayload(mutation Mutation, input UpdateSlackMessageMutationInput) (map[string]any, map[string]any, string, error) {
	text := strings.TrimSpace(input.Text)
	if err := validateSlackMessageText(text); err != nil {
		return nil, nil, "", fmt.Errorf("Slack message %w", err)
	}
	payload := cloneMap(mutation.Payload)
	payload["text"] = text
	preview := cloneMap(mutation.Preview)
	slackMessage := cloneMap(mapFromAny(preview["slack_message"]))
	slackMessage["text"] = text
	slackMessage["edited"] = true
	preview["slack_message"] = slackMessage
	return payload, preview, mutation.Title, nil
}

// --- proposal-time enrichment -------------------------------------------------

type slackSendPreviewKey struct {
	Account        string
	ConversationID string
	UserID         string
	ThreadTS       string
}

// slackSendPreviewDetail is what the warehouse knows about the recipient at
// proposal time. Found flags are what make a wrong id visible in the review:
// the executor refuses the same things, but a refusal after approval is a
// worse experience than a warning before it.
type slackSendPreviewDetail struct {
	Account           string
	TeamID            string
	TeamDomain        string
	SelfUserID        string
	ConversationID    string
	ConversationFound bool
	ConversationType  string
	ConversationName  string
	IsArchived        bool
	IsMember          bool
	RecipientUserID   string
	RecipientFound    bool
	RecipientName     string
	RecipientAvatar   string
	RecipientDeleted  bool
	RecipientIsBot    bool
	ThreadTS          string
	ThreadFound       bool
}

type slackSendPreviewContext struct {
	Key    slackSendPreviewKey
	Detail slackSendPreviewDetail
	Rows   []slackMarkReadPreviewRow
}

func slackSendPreviewKeyFor(mutation storedMutation) slackSendPreviewKey {
	return slackSendPreviewKey{
		Account:        normalizeAccount(mutation.Account),
		ConversationID: strings.TrimSpace(stringFromAny(mutation.Payload["conversation_id"])),
		UserID:         strings.TrimSpace(stringFromAny(mutation.Payload["user_id"])),
		ThreadTS:       strings.TrimSpace(stringFromAny(mutation.Payload["thread_ts"])),
	}
}

func slackSendMessagePreviewTargets(mutations []storedMutation) []slackSendPreviewKey {
	targets := []slackSendPreviewKey{}
	seen := map[slackSendPreviewKey]bool{}
	for _, mutation := range mutations {
		if !isStoredSlackSendMessageMutation(mutation) {
			continue
		}
		key := slackSendPreviewKeyFor(mutation)
		if key.Account == "" || (key.ConversationID == "" && key.UserID == "") || seen[key] {
			continue
		}
		targets = append(targets, key)
		seen[key] = true
	}
	return targets
}

// slackSendPreviewWarnings is the list the review UI shows in red. Each one
// is something the executor will refuse or Slack will reject, stated before
// approval.
func slackSendPreviewWarnings(detail slackSendPreviewDetail, delivery string) []string {
	warnings := []string{}
	switch {
	case delivery == slackSendDeliveryDM && !detail.RecipientFound:
		warnings = append(warnings, "This user id is not in the warehouse for this Slack account; the executor will refuse it.")
	case delivery == slackSendDeliveryDM && detail.RecipientDeleted:
		warnings = append(warnings, "This user is deactivated in Slack; the message cannot be delivered.")
	case delivery == slackSendDeliveryDM && detail.RecipientIsBot:
		warnings = append(warnings, "This user id belongs to a bot; the executor will refuse it.")
	case delivery != slackSendDeliveryDM && !detail.ConversationFound:
		warnings = append(warnings, "This conversation is not in the warehouse for this Slack account; the executor will refuse it.")
	}
	if detail.ConversationFound && detail.IsArchived {
		warnings = append(warnings, "This conversation is archived; Slack will reject the post.")
	}
	if detail.ConversationFound && !detail.IsMember &&
		(detail.ConversationType == "public_channel" || detail.ConversationType == "private_channel") {
		warnings = append(warnings, "You are not a member of this channel; Slack will reject the post (not_in_channel).")
	}
	if delivery == slackSendDeliveryThreadReply && !detail.ThreadFound {
		warnings = append(warnings, "The thread's parent message is not in the warehouse; the executor will refuse the reply.")
	}
	return warnings
}

func slackSendPreviewRecipientLabel(detail slackSendPreviewDetail, delivery string) string {
	if delivery == slackSendDeliveryDM {
		if name := strings.TrimSpace(detail.RecipientName); name != "" {
			return name
		}
		return detail.RecipientUserID
	}
	if name := strings.TrimSpace(detail.ConversationName); name != "" {
		switch detail.ConversationType {
		case "public_channel", "private_channel":
			return "#" + strings.TrimPrefix(name, "#")
		}
		return name
	}
	switch detail.ConversationType {
	case "im":
		return "Direct message"
	case "mpim":
		return "Group DM"
	}
	return detail.ConversationID
}

// applySlackSendMessagePreviewDetails writes what the warehouse resolved into
// each send mutation's `slack_message` preview: the workspace, the recipient
// by name, the warnings, and the conversation or thread the message lands in.
func applySlackSendMessagePreviewDetails(mutations []storedMutation, contexts []slackSendPreviewContext) {
	byKey := map[slackSendPreviewKey]slackSendPreviewContext{}
	for _, item := range contexts {
		byKey[item.Key] = item
	}
	for index := range mutations {
		mutation := &mutations[index]
		if !isStoredSlackSendMessageMutation(*mutation) {
			continue
		}
		item, ok := byKey[slackSendPreviewKeyFor(*mutation)]
		if !ok {
			continue
		}
		detail := item.Detail
		preview := cloneMap(mutation.Preview)
		slackMessage := cloneMap(mapFromAny(preview["slack_message"]))
		delivery := strings.TrimSpace(stringFromAny(slackMessage["delivery"]))
		if delivery == "" {
			delivery = slackSendMessageDelivery(item.Key.ConversationID, item.Key.UserID, item.Key.ThreadTS)
		}
		recipientLabel := slackSendPreviewRecipientLabel(detail, delivery)

		ordered := append([]slackMarkReadPreviewRow{}, item.Rows...)
		sort.SliceStable(ordered, func(i, j int) bool {
			if !ordered[i].SentAt.Equal(ordered[j].SentAt) {
				return ordered[i].SentAt.Before(ordered[j].SentAt)
			}
			return compareSlackTimestamp(ordered[i].MessageTS, ordered[j].MessageTS) < 0
		})
		messages := make([]map[string]any, 0, len(ordered))
		for _, row := range ordered {
			message := map[string]any{
				"message_ts":       strings.TrimSpace(row.MessageTS),
				"sent_at":          formatPreviewTime(row.SentAt),
				"user_id":          strings.TrimSpace(row.UserID),
				"actor_name":       strings.TrimSpace(row.ActorName),
				"avatar_url":       strings.TrimSpace(row.AvatarURL),
				"text":             strings.TrimSpace(row.Text),
				"is_from_me":       row.IsFromMe,
				"is_thread_parent": detail.ThreadTS != "" && strings.TrimSpace(row.MessageTS) == detail.ThreadTS,
			}
			if link := deeplink.Slack(detail.TeamID, detail.ConversationID, row.MessageTS, row.ThreadTS, detail.TeamDomain); link != nil {
				message["open"] = link
			}
			messages = append(messages, message)
		}

		slackMessage["team_id"] = detail.TeamID
		slackMessage["team_domain"] = detail.TeamDomain
		slackMessage["conversation_type"] = detail.ConversationType
		slackMessage["conversation_name"] = detail.ConversationName
		slackMessage["resolved_conversation_id"] = detail.ConversationID
		slackMessage["conversation_found"] = detail.ConversationFound
		slackMessage["is_archived"] = detail.IsArchived
		slackMessage["is_member"] = detail.IsMember
		slackMessage["recipient_user_id"] = detail.RecipientUserID
		slackMessage["recipient_name"] = detail.RecipientName
		slackMessage["recipient_found"] = detail.RecipientFound
		slackMessage["recipient_label"] = recipientLabel
		slackMessage["thread_found"] = detail.ThreadFound
		slackMessage["warnings"] = slackSendPreviewWarnings(detail, delivery)
		slackMessage["messages"] = messages
		if avatar := strings.TrimSpace(detail.RecipientAvatar); avatar != "" {
			slackMessage["avatar_url"] = avatar
		}
		// The link opens the thread being replied to, or the conversation at
		// its newest message; a channel with no synced message gets none.
		anchorTS := detail.ThreadTS
		if anchorTS == "" && len(ordered) > 0 {
			anchorTS = ordered[len(ordered)-1].MessageTS
		}
		if link := deeplink.Slack(detail.TeamID, detail.ConversationID, anchorTS, detail.ThreadTS, detail.TeamDomain); link != nil {
			slackMessage["open"] = link
		}
		preview["slack_message"] = slackMessage
		mutation.Preview = preview
		if mutation.Title == slackSendMessageTitle(mutation.Payload) {
			mutation.Title = slackSendMessageTitleFor(delivery, recipientLabel)
		}
	}
}

// --- read-time hydration -------------------------------------------------------

// applySlackSendMessagePreviewLinks is the send twin of
// applySlackMarkReadPreviewLinks: the workspace domain and every face are
// resolved on READ, because a snapshot's avatar goes stale and a request
// proposed before a domain was synced would otherwise never link.
func applySlackSendMessagePreviewLinks(
	mutations []Mutation,
	domains map[slackTeamKey]string,
	avatars map[slackUserKey]string,
) []Mutation {
	out := make([]Mutation, len(mutations))
	copy(out, mutations)
	for index, mutation := range out {
		if !isSlackSendMessageMutation(mutation) {
			continue
		}
		slackMessage := cloneMap(mapFromAny(mutation.Preview["slack_message"]))
		if len(slackMessage) == 0 {
			continue
		}
		account := normalizeAccount(mutation.Account)
		teamID := strings.TrimSpace(stringFromAny(slackMessage["team_id"]))
		if teamID == "" {
			continue
		}
		conversationID := strings.TrimSpace(stringFromAny(slackMessage["resolved_conversation_id"]))
		if conversationID == "" {
			conversationID = strings.TrimSpace(stringFromAny(slackMessage["conversation_id"]))
		}
		threadTS := strings.TrimSpace(stringFromAny(slackMessage["thread_ts"]))
		domain := domains[slackTeamKey{Account: account, TeamID: teamID}]
		slackMessage["team_domain"] = domain

		messages := make([]map[string]any, 0, len(mapSliceFromAny(slackMessage["messages"])))
		newestTS := ""
		for _, stored := range mapSliceFromAny(slackMessage["messages"]) {
			message := cloneMap(stored)
			userID := strings.TrimSpace(stringFromAny(message["user_id"]))
			if avatar, ok := avatars[slackUserKey{Account: account, TeamID: teamID, UserID: userID}]; ok {
				message["avatar_url"] = avatar
			}
			messageTS := strings.TrimSpace(stringFromAny(message["message_ts"]))
			if link := deeplink.Slack(teamID, conversationID, messageTS, threadTS, domain); link != nil {
				message["open"] = link
			}
			if newestTS == "" || compareSlackTimestamp(messageTS, newestTS) > 0 {
				newestTS = messageTS
			}
			messages = append(messages, message)
		}
		if len(messages) > 0 {
			slackMessage["messages"] = messages
		}
		recipientUserID := strings.TrimSpace(stringFromAny(slackMessage["recipient_user_id"]))
		if avatar, ok := avatars[slackUserKey{Account: account, TeamID: teamID, UserID: recipientUserID}]; ok && recipientUserID != "" {
			slackMessage["avatar_url"] = avatar
		}
		anchorTS := threadTS
		if anchorTS == "" {
			anchorTS = newestTS
		}
		if link := deeplink.Slack(teamID, conversationID, anchorTS, threadTS, domain); link != nil {
			slackMessage["open"] = link
		}
		preview := cloneMap(mutation.Preview)
		preview["slack_message"] = slackMessage
		out[index].Preview = preview
	}
	return out
}

func slackSendMessagePreviewLinkTargets(mutations []Mutation) ([]slackTeamKey, []slackUserKey) {
	teams := []slackTeamKey{}
	users := []slackUserKey{}
	seenTeam := map[slackTeamKey]bool{}
	seenUser := map[slackUserKey]bool{}
	for _, mutation := range mutations {
		if !isSlackSendMessageMutation(mutation) {
			continue
		}
		slackMessage := mapFromAny(mutation.Preview["slack_message"])
		team := slackTeamKey{
			Account: normalizeAccount(mutation.Account),
			TeamID:  strings.TrimSpace(stringFromAny(slackMessage["team_id"])),
		}
		if team.Account == "" || team.TeamID == "" {
			continue
		}
		if !seenTeam[team] {
			teams = append(teams, team)
			seenTeam[team] = true
		}
		userIDs := []string{strings.TrimSpace(stringFromAny(slackMessage["recipient_user_id"]))}
		for _, message := range mapSliceFromAny(slackMessage["messages"]) {
			userIDs = append(userIDs, strings.TrimSpace(stringFromAny(message["user_id"])))
		}
		for _, userID := range userIDs {
			user := slackUserKey{Account: team.Account, TeamID: team.TeamID, UserID: userID}
			if user.UserID == "" || seenUser[user] {
				continue
			}
			users = append(users, user)
			seenUser[user] = true
		}
	}
	return teams, users
}

// slackPreviewLinkTargets unions the read-time lookups every Slack preview
// kind needs, so one request holding both a mark-read and a send pays one
// pair of queries.
func slackPreviewLinkTargets(mutations []Mutation) ([]slackTeamKey, []slackUserKey) {
	teams, users := slackMarkReadPreviewLinkTargets(mutations)
	sendTeams, sendUsers := slackSendMessagePreviewLinkTargets(mutations)
	seenTeam := map[slackTeamKey]bool{}
	for _, team := range teams {
		seenTeam[team] = true
	}
	for _, team := range sendTeams {
		if !seenTeam[team] {
			teams = append(teams, team)
			seenTeam[team] = true
		}
	}
	seenUser := map[slackUserKey]bool{}
	for _, user := range users {
		seenUser[user] = true
	}
	for _, user := range sendUsers {
		if !seenUser[user] {
			users = append(users, user)
			seenUser[user] = true
		}
	}
	return teams, users
}

// applySlackPreviewLinks hydrates every Slack preview kind from the one pair
// of read-time lookups.
func applySlackPreviewLinks(
	mutations []Mutation,
	domains map[slackTeamKey]string,
	avatars map[slackUserKey]string,
) []Mutation {
	return applySlackSendMessagePreviewLinks(applySlackMarkReadPreviewLinks(mutations, domains, avatars), domains, avatars)
}
