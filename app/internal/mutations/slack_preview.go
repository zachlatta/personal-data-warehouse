package mutations

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
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
			messages = append(messages, map[string]any{
				"message_ts": strings.TrimSpace(row.MessageTS),
				"sent_at":    formatPreviewTime(row.SentAt),
				"user_id":    strings.TrimSpace(row.UserID),
				"actor_name": strings.TrimSpace(row.ActorName),
				"text":       strings.TrimSpace(row.Text),
				"is_target":  isTarget,
				"is_from_me": row.IsFromMe,
				"position":   position,
			})
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
