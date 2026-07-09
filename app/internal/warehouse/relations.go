package warehouse

import "strings"

type Relation struct {
	Schema string
	Name   string
}

var QueryableSchemas = []string{
	"gmail",
	"google_calendar",
	"google_contacts",
	"google_drive",
	"slack",
	"apple_notes",
	"apple_messages",
	"apple_voice_memos",
	"alice_voice_recordings",
	"whatsapp",
	"chatgpt",
	"claude_desktop",
	"claude_code",
	"codex",
	"openclaw",
	"pi",
	"marts",
	"timeline",
	"search",
	"enrichment",
	"ai_processing",
	"upstream_mutations",
	"util",
}

var Relations = map[string]Relation{
	"gmail_messages":                        {"gmail", "messages"},
	"gmail_attachments":                     {"gmail", "attachments"},
	"gmail_sync_state":                      {"gmail", "sync_state"},
	"calendar_events":                       {"google_calendar", "events"},
	"contact_cards":                         {"google_contacts", "cards"},
	"google_drive_files":                    {"google_drive", "files"},
	"google_drive_file_texts":               {"google_drive", "file_texts"},
	"slack_messages":                        {"slack", "messages"},
	"slack_files":                           {"slack", "files"},
	"slack_conversations":                   {"slack", "conversations"},
	"slack_users":                           {"slack", "users"},
	"slack_message_reactions":               {"slack", "message_reactions"},
	"apple_notes":                           {"apple_notes", "notes"},
	"apple_note_revisions":                  {"apple_notes", "revisions"},
	"apple_note_attachments":                {"apple_notes", "attachments"},
	"apple_messages":                        {"apple_messages", "messages"},
	"apple_message_attachments":             {"apple_messages", "attachments"},
	"apple_message_chat_messages":           {"apple_messages", "chat_messages"},
	"whatsapp_messages":                     {"whatsapp", "messages"},
	"whatsapp_media_items":                  {"whatsapp", "media_items"},
	"apple_voice_memos_files":               {"apple_voice_memos", "files"},
	"apple_voice_memos_transcription_runs":  {"apple_voice_memos", "transcription_runs"},
	"apple_voice_memos_transcript_segments": {"apple_voice_memos", "transcript_segments"},
	"apple_voice_memos_enrichments":         {"apple_voice_memos", "enrichments"},
	"chatgpt_events":                        {"chatgpt", "events"},
	"claude_desktop_events":                 {"claude_desktop", "events"},
	"claude_code_events":                    {"claude_code", "events"},
	"codex_events":                          {"codex", "events"},
	"openclaw_events":                       {"openclaw", "events"},
	"pi_events":                             {"pi", "events"},
	"agent_session_events":                  {"marts", "ai_conversation_events"},
	"ai_conversation_events":                {"marts", "ai_conversation_events"},
	"clean_agent_sessions":                  {"marts", "ai_conversation_sessions"},
	"agent_runs":                            {"ai_processing", "agent_runs"},
	"agent_run_events":                      {"ai_processing", "agent_run_events"},
	"agent_run_tool_calls":                  {"ai_processing", "agent_run_tool_calls"},
	"file_attachment_enrichments":           {"enrichment", "file_attachment_enrichments"},
	"timeline_events":                       {"timeline", "events"},
	"timeline_sync_state":                   {"timeline", "sync_state"},
	"upstream_mutation_requests":            {"upstream_mutations", "requests"},
	"upstream_mutations":                    {"upstream_mutations", "operations"},
	"upstream_mutation_events":              {"upstream_mutations", "operation_events"},
	"upstream_mutation_request_events":      {"upstream_mutations", "request_events"},
	"chatgpt_sessions":                      {"private", "chatgpt_sessions"},
	"claude_desktop_credentials":            {"private", "claude_desktop_credentials"},
	"whatsapp_client_sessions":              {"private", "whatsapp_client_sessions"},
	"search_text":                           {"search", "search_text"},
	"search_text_sources":                   {"search", "search_text_sources"},
	"search_schema_state":                   {"search", "schema_state"},
}

func SQLRelation(logical string) string {
	if rel, ok := Relations[logical]; ok {
		return QuoteIdent(rel.Schema) + "." + QuoteIdent(rel.Name)
	}
	return QuoteIdent(logical)
}

func DisplayRelation(logical string) string {
	if rel, ok := Relations[logical]; ok {
		return rel.Schema + "." + rel.Name
	}
	return logical
}

func QuoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func SQLString(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func QualifySQL(sql string) string {
	if isCreateSchemaStatement(sql) {
		return sql
	}
	var out strings.Builder
	out.Grow(len(sql) + 32)
	for i := 0; i < len(sql); {
		ch := sql[i]
		if ch == '\'' {
			start := i
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					i++
					if i < len(sql) && sql[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			out.WriteString(sql[start:i])
			continue
		}
		if ch == '"' {
			start := i
			i++
			var ident strings.Builder
			for i < len(sql) {
				if sql[i] == '"' {
					i++
					if i < len(sql) && sql[i] == '"' {
						ident.WriteByte('"')
						i++
						continue
					}
					break
				}
				ident.WriteByte(sql[i])
				i++
			}
			if rel, ok := Relations[ident.String()]; ok && !adjacentToDot(sql, start, i) {
				out.WriteString(QuoteIdent(rel.Schema) + "." + QuoteIdent(rel.Name))
			} else {
				out.WriteString(sql[start:i])
			}
			continue
		}
		if isIdentStart(ch) {
			start := i
			i++
			for i < len(sql) && isIdentPart(sql[i]) {
				i++
			}
			token := sql[start:i]
			if rel, ok := Relations[token]; ok && !adjacentToDot(sql, start, i) {
				out.WriteString(QuoteIdent(rel.Schema) + "." + QuoteIdent(rel.Name))
			} else {
				out.WriteString(token)
			}
			continue
		}
		out.WriteByte(ch)
		i++
	}
	return out.String()
}

func isCreateSchemaStatement(sql string) bool {
	fields := strings.Fields(sql)
	return len(fields) >= 2 && strings.EqualFold(fields[0], "CREATE") && strings.EqualFold(fields[1], "SCHEMA")
}

func isIdentStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}

func adjacentToDot(sql string, start, end int) bool {
	before := start - 1
	for before >= 0 && (sql[before] == ' ' || sql[before] == '\n' || sql[before] == '\t' || sql[before] == '\r') {
		before--
	}
	after := end
	for after < len(sql) && (sql[after] == ' ' || sql[after] == '\n' || sql[after] == '\t' || sql[after] == '\r') {
		after++
	}
	return (before >= 0 && sql[before] == '.') || (after < len(sql) && sql[after] == '.')
}
