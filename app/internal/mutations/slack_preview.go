package mutations

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/deeplink"
)

type slackMarkReadPreviewKey struct {
	Account        string
	ConversationID string
	MessageTS      string
}

type slackMarkReadPreviewDetail struct {
	Account            string
	TeamID             string
	ConversationID     string
	MessageTS          string
	ConversationName   string
	ConversationType   string
	CurrentLastRead    string
	CurrentUnreadCount int
	ContextKind        string
	ThreadTS           string
	SelfUserID         string
	TeamDomain         string
}

type slackMarkReadPreviewRow struct {
	Account         string
	ConversationID  string
	TargetMessageTS string
	MessageTS       string
	SentAt          time.Time
	UserID          string
	ActorName       string
	Text            string
	ThreadTS        string
	AvatarURL       string
	IsTarget        bool
	IsFromMe        bool
}

func slackMarkReadPreviewTargets(mutations []storedMutation) []slackMarkReadPreviewKey {
	targets := []slackMarkReadPreviewKey{}
	seen := map[slackMarkReadPreviewKey]bool{}
	for _, mutation := range mutations {
		if mutation.Provider != SlackProvider || mutation.Operation != SlackMarkConversationReadOperation {
			continue
		}
		key := slackMarkReadPreviewKey{
			Account:        normalizeAccount(mutation.Account),
			ConversationID: strings.TrimSpace(stringFromAny(mutation.Payload["conversation_id"])),
			MessageTS:      strings.TrimSpace(stringFromAny(mutation.Payload["message_ts"])),
		}
		if key.Account == "" || key.ConversationID == "" || key.MessageTS == "" || seen[key] {
			continue
		}
		targets = append(targets, key)
		seen[key] = true
	}
	return targets
}

func applySlackMarkReadPreviewRows(
	mutations []storedMutation,
	details []slackMarkReadPreviewDetail,
	rows []slackMarkReadPreviewRow,
) {
	detailByKey := map[slackMarkReadPreviewKey]slackMarkReadPreviewDetail{}
	for _, detail := range details {
		key := slackMarkReadPreviewKey{
			Account:        normalizeAccount(detail.Account),
			ConversationID: strings.TrimSpace(detail.ConversationID),
			MessageTS:      strings.TrimSpace(detail.MessageTS),
		}
		if key.Account != "" && key.ConversationID != "" && key.MessageTS != "" {
			detailByKey[key] = detail
		}
	}
	rowsByConversation := map[slackMarkReadPreviewKey][]slackMarkReadPreviewRow{}
	for _, row := range rows {
		if targetMessageTS := strings.TrimSpace(row.TargetMessageTS); targetMessageTS != "" {
			key := slackMarkReadPreviewKey{
				Account:        normalizeAccount(row.Account),
				ConversationID: strings.TrimSpace(row.ConversationID),
				MessageTS:      targetMessageTS,
			}
			if _, ok := detailByKey[key]; ok {
				rowsByConversation[key] = append(rowsByConversation[key], row)
			}
			continue
		}
		for key := range detailByKey {
			if normalizeAccount(row.Account) == key.Account && strings.TrimSpace(row.ConversationID) == key.ConversationID {
				rowsByConversation[key] = append(rowsByConversation[key], row)
			}
		}
	}

	for index := range mutations {
		mutation := &mutations[index]
		if mutation.Provider != SlackProvider || mutation.Operation != SlackMarkConversationReadOperation {
			continue
		}
		key := slackMarkReadPreviewKey{
			Account:        normalizeAccount(mutation.Account),
			ConversationID: strings.TrimSpace(stringFromAny(mutation.Payload["conversation_id"])),
			MessageTS:      strings.TrimSpace(stringFromAny(mutation.Payload["message_ts"])),
		}
		detail, ok := detailByKey[key]
		if !ok {
			continue
		}
		ordered := append([]slackMarkReadPreviewRow{}, rowsByConversation[key]...)
		sort.SliceStable(ordered, func(i, j int) bool {
			if !ordered[i].SentAt.Equal(ordered[j].SentAt) {
				return ordered[i].SentAt.Before(ordered[j].SentAt)
			}
			return compareSlackTimestamp(ordered[i].MessageTS, ordered[j].MessageTS) < 0
		})
		messages := make([]map[string]any, 0, len(ordered))
		for _, row := range ordered {
			position := "before"
			comparison := compareSlackTimestamp(row.MessageTS, key.MessageTS)
			isTarget := row.IsTarget || comparison == 0
			if isTarget {
				position = "target"
			} else if comparison > 0 {
				position = "after"
			}
			message := map[string]any{
				"message_ts": strings.TrimSpace(row.MessageTS),
				"sent_at":    formatPreviewTime(row.SentAt),
				"user_id":    strings.TrimSpace(row.UserID),
				"actor_name": strings.TrimSpace(row.ActorName),
				"avatar_url": strings.TrimSpace(row.AvatarURL),
				"text":       strings.TrimSpace(row.Text),
				"is_target":  isTarget,
				"is_from_me": row.IsFromMe,
				"position":   position,
			}
			// Every message is openable in Slack, because the honest answer to
			// "should this be marked read?" is often "let me reply first", and
			// a review that cannot be acted on is a review that gets approved
			// unread. Same JSON as a timeline row's `open`, so the clients
			// open it with the helper they already have.
			if link := deeplink.Slack(detail.TeamID, detail.ConversationID, row.MessageTS, row.ThreadTS, detail.TeamDomain); link != nil {
				message["open"] = link
			}
			messages = append(messages, message)
		}

		preview := cloneMap(mutation.Preview)
		slackRead := cloneMap(mapFromAny(preview["slack_read"]))
		slackRead["team_id"] = strings.TrimSpace(detail.TeamID)
		slackRead["conversation_name"] = slackConversationPreviewName(detail, ordered)
		slackRead["conversation_type"] = strings.TrimSpace(detail.ConversationType)
		slackRead["current_last_read"] = strings.TrimSpace(detail.CurrentLastRead)
		slackRead["current_unread_count"] = detail.CurrentUnreadCount
		slackRead["context_kind"] = strings.TrimSpace(detail.ContextKind)
		slackRead["thread_ts"] = strings.TrimSpace(detail.ThreadTS)
		slackRead["team_domain"] = strings.TrimSpace(detail.TeamDomain)
		if link := deeplink.Slack(detail.TeamID, detail.ConversationID, detail.MessageTS, detail.ThreadTS, detail.TeamDomain); link != nil {
			slackRead["open"] = link
		}
		// The face on the compact row is whoever wrote the message the read
		// boundary lands on: in a DM that is the person, and in a channel it
		// is the message actually being reviewed.
		for _, row := range ordered {
			if row.IsTarget && strings.TrimSpace(row.AvatarURL) != "" {
				slackRead["avatar_url"] = strings.TrimSpace(row.AvatarURL)
			}
		}
		slackRead["messages"] = messages
		preview["slack_read"] = slackRead
		mutation.Preview = preview
	}
}

func slackConversationPreviewName(detail slackMarkReadPreviewDetail, rows []slackMarkReadPreviewRow) string {
	if name := strings.TrimSpace(detail.ConversationName); name != "" {
		return name
	}
	switch strings.TrimSpace(detail.ConversationType) {
	case "im":
		for _, row := range rows {
			if !row.IsFromMe {
				if name := strings.TrimSpace(row.ActorName); name != "" && name != "Unknown" {
					return name
				}
			}
		}
		return "Direct message"
	case "mpim":
		return "Group DM"
	default:
		return detail.ConversationID
	}
}

func compareSlackTimestamp(left string, right string) int {
	leftRat, leftOK := new(big.Rat).SetString(strings.TrimSpace(left))
	rightRat, rightOK := new(big.Rat).SetString(strings.TrimSpace(right))
	if leftOK && rightOK {
		return leftRat.Cmp(rightRat)
	}
	return strings.Compare(strings.TrimSpace(left), strings.TrimSpace(right))
}

func intFromPreviewAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		parsed, _ := strconv.Atoi(strings.TrimSpace(stringFromAny(value)))
		return parsed
	}
}

type slackTeamKey struct {
	Account string
	TeamID  string
}

type slackUserKey struct {
	Account string
	TeamID  string
	UserID  string
}

// applySlackMarkReadPreviewLinks puts the face and the permalink on an
// already-snapshotted Slack mark-read preview.
//
// The conversation context is captured when the request is proposed, and it
// stays captured: re-reading it for a 277-conversation batch would be ~1,100
// queries on one page load. But a permalink is pure derivation from ids the
// snapshot already holds, and an avatar URL goes stale the moment somebody
// changes their picture — so both are resolved at READ time, from two bounded
// lookups, and overwrite whatever the snapshot carried. Without this, a
// request proposed before either shipped would never show them at all.
func applySlackMarkReadPreviewLinks(
	mutations []Mutation,
	domains map[slackTeamKey]string,
	avatars map[slackUserKey]string,
) []Mutation {
	out := make([]Mutation, len(mutations))
	copy(out, mutations)
	for index, mutation := range out {
		if mutation.Provider != SlackProvider || mutation.Operation != SlackMarkConversationReadOperation {
			continue
		}
		slackRead := cloneMap(mapFromAny(mutation.Preview["slack_read"]))
		if len(slackRead) == 0 {
			continue
		}
		account := normalizeAccount(mutation.Account)
		teamID := strings.TrimSpace(stringFromAny(slackRead["team_id"]))
		conversationID := strings.TrimSpace(stringFromAny(slackRead["conversation_id"]))
		messageTS := strings.TrimSpace(stringFromAny(slackRead["message_ts"]))
		conversationThreadTS := strings.TrimSpace(stringFromAny(slackRead["thread_ts"]))
		domain := domains[slackTeamKey{Account: account, TeamID: teamID}]
		slackRead["team_domain"] = domain

		messages := make([]map[string]any, 0, len(mapSliceFromAny(slackRead["messages"])))
		targetAvatar := ""
		for _, stored := range mapSliceFromAny(slackRead["messages"]) {
			message := cloneMap(stored)
			userID := strings.TrimSpace(stringFromAny(message["user_id"]))
			if avatar, ok := avatars[slackUserKey{Account: account, TeamID: teamID, UserID: userID}]; ok {
				message["avatar_url"] = avatar
			}
			// A message in the surrounding window may be a reply of its own,
			// and the snapshot records that per message where it knows it.
			threadTS := strings.TrimSpace(stringFromAny(message["thread_ts"]))
			if threadTS == "" {
				threadTS = conversationThreadTS
			}
			if link := deeplink.Slack(teamID, conversationID, strings.TrimSpace(stringFromAny(message["message_ts"])), threadTS, domain); link != nil {
				message["open"] = link
			}
			if message["is_target"] == true {
				targetAvatar = strings.TrimSpace(stringFromAny(message["avatar_url"]))
			}
			messages = append(messages, message)
		}
		if len(messages) > 0 {
			slackRead["messages"] = messages
		}
		if targetAvatar != "" {
			slackRead["avatar_url"] = targetAvatar
		}
		if link := deeplink.Slack(teamID, conversationID, messageTS, conversationThreadTS, domain); link != nil {
			slackRead["open"] = link
		}
		preview := cloneMap(mutation.Preview)
		preview["slack_read"] = slackRead
		out[index].Preview = preview
	}
	return out
}

// slackMarkReadPreviewLinkTargets is what the two lookups need: one workspace
// per (account, team) and one row per person who spoke in any of the reviewed
// conversations.
func slackMarkReadPreviewLinkTargets(mutations []Mutation) ([]slackTeamKey, []slackUserKey) {
	teams := []slackTeamKey{}
	users := []slackUserKey{}
	seenTeam := map[slackTeamKey]bool{}
	seenUser := map[slackUserKey]bool{}
	for _, mutation := range mutations {
		if mutation.Provider != SlackProvider || mutation.Operation != SlackMarkConversationReadOperation {
			continue
		}
		slackRead := mapFromAny(mutation.Preview["slack_read"])
		if len(slackRead) == 0 {
			continue
		}
		team := slackTeamKey{
			Account: normalizeAccount(mutation.Account),
			TeamID:  strings.TrimSpace(stringFromAny(slackRead["team_id"])),
		}
		if team.Account == "" || team.TeamID == "" {
			continue
		}
		if !seenTeam[team] {
			teams = append(teams, team)
			seenTeam[team] = true
		}
		for _, message := range mapSliceFromAny(slackRead["messages"]) {
			user := slackUserKey{
				Account: team.Account,
				TeamID:  team.TeamID,
				UserID:  strings.TrimSpace(stringFromAny(message["user_id"])),
			}
			if user.UserID == "" || seenUser[user] {
				continue
			}
			users = append(users, user)
			seenUser[user] = true
		}
	}
	return teams, users
}
