package query

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The schema overview is the required first call, so its size is a hard
// constraint rather than a nicety: at 108 relations the old full-column dump was
// ~61KB, large enough that some clients spilled it to a file instead of showing
// it, and far too large to stay in an agent's context until the SQL actually got
// written. The observable result was that callers read it once, lost it, and
// then guessed — 70% of failed warehouse queries in 30 days of transcripts were
// SQLSTATE 42703 (undefined column), against a long tail of invented names that
// no hardcoded remap table could ever cover.
//
// So the overview now carries what you cannot look up per-table (which
// relations exist, how big they are, how to join them, the search and layer
// conventions, and the three type rules that no column name implies) and hands
// off the per-relation column catalog to describe_table.

// overviewTimeColumn is a relation's primary event-time column. These are
// curated, never inferred: the names diverge per source and picking the wrong
// timestamptz silently answers a different question than the caller asked.
var overviewTimeColumns = map[string]string{
	"timeline.events":                 "event_ts",
	"base_apple_photos.files":         "captured_at",
	"derived_photos.assets":           "capture_ts",
	"base_plaid.transactions":         "posted_at",
	"base_whatsapp.media_items":       "message_at",
	"base_apple_messages.attachments": "message_at",
}

func init() {
	// timeColumns is the list the undefined-column hint prints; the overview
	// needs the same answers, so derive rather than restate them.
	for _, tc := range timeColumns {
		overviewTimeColumns[tc.table] = tc.column
	}
}

// bookkeepingTimeColumns are sync/audit timestamps. They are real columns, but
// none of them is the event time a caller filtering "messages in March" wants,
// so they are only offered when a relation has nothing else.
var bookkeepingTimeColumns = map[string]bool{
	"ingested_at": true, "synced_at": true, "created_at": true, "updated_at": true,
	"last_synced_at": true, "first_seen_at": true, "last_seen_at": true,
	"completed_at": true, "last_success_at": true, "deleted_at": true,
	"sync_started_at": true, "ingest_ts": true,
}

// --- catalog guidance ----------------------------------------------------------
//
// Every schema, and 35 relations, carry a `comment` in warehouse_catalog.json
// saying which relation to reach for and what each one is actually for. Those
// comments are published as real Postgres comments, but neither schema_overview
// nor describe_table used to render them, so the guidance existed everywhere
// except where an agent would see it. Measured over 60 days, 43% of sessions
// opened with schema_overview and 18% opened by naming a relation that does not
// exist. This block is what closes that gap.
//
// The overview's size is the constraint (see the file header), so the rendering
// is selective in two ways:
//
//   - A schema headline prints only when the schema's comment names a specific
//     relation, which is exactly the guidance worth paying for. The 21 base_*
//     and 6 generic derived_* comments are one boilerplate sentence with the
//     source name substituted; the preamble's layer table already says it, and
//     repeating it 27 more times would spend ~3.2KB to say nothing.
//   - A relation's own comment prints as its FIRST SENTENCE, capped. The full
//     text is one describe_table call away, which is what makes truncating here
//     safe rather than lossy.
var (
	// schemaHeadlines holds the schema comments that name a relation.
	schemaHeadlines = map[string]string{}
	// relationHeadlines maps "schema.name" to that relation's own comment.
	relationHeadlines = map[string]string{}
	// entryPointRelations holds the "schema.name" of every relation the
	// catalog nominates as a starting point.
	entryPointRelations = map[string]bool{}
)

// relationReference matches a schema-qualified relation named inside catalog
// prose ("Start with marts_photos.photos, ..."). Bare words and the function
// and column references that share the surrounding text (search_text(),
// source_table/source_pk) deliberately do not match.
var relationReference = regexp.MustCompile(`\b([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\b`)

// overviewRelationCommentChars caps a per-relation line. At 110 it keeps the
// traps that cost real sessions intact ("Plaid's lookback is a hard 730 days"
// is 101) while holding the whole addition to ~3KB.
const overviewRelationCommentChars = 110

func init() {
	knownSchemas := map[string]bool{}
	for _, schema := range warehouse.Schemas {
		knownSchemas[schema.Name] = true
	}
	knownRelations := map[string]bool{}
	for _, obj := range warehouse.Objects {
		if !obj.IsRelation() {
			continue
		}
		display := obj.Schema + "." + obj.Name
		knownRelations[display] = true
		if obj.Comment == "" {
			continue
		}
		relationHeadlines[display] = obj.Comment
		// The catalog already has a convention for this: an entry point
		// introduces itself with "Start here" (timeline.events shouts it).
		// Reading the convention beats adding a second place to declare the
		// same fact and then keeping the two in agreement.
		if strings.HasPrefix(strings.ToLower(obj.Comment), "start here") {
			entryPointRelations[display] = true
		}
	}
	for _, schema := range warehouse.Schemas {
		if !schema.Discoverable || schema.Comment == "" {
			continue
		}
		named := false
		// Fallback nomination for a schema where no relation says "Start here"
		// of its own accord: the FIRST relation of this schema the headline
		// mentions. It has to be the first, because these comments go on to
		// name the drill-downs too — "Start with marts_photos.photos ...;
		// marts_photos.files lists every underlying rendition" nominates one
		// relation and describes another, and marking both would erase the
		// distinction this marker exists to draw.
		firstOwn := ""
		schemaDeclares := false
		for _, match := range relationReference.FindAllStringSubmatch(schema.Comment, -1) {
			if !knownSchemas[match[1]] {
				continue
			}
			named = true
			// Only a schema's own relations are candidates. A comment pointing
			// at another schema (derived_voice_memos naming
			// marts_voice_memos.recordings) is guidance, not a nomination.
			if match[1] != schema.Name || !knownRelations[match[0]] {
				continue
			}
			if firstOwn == "" {
				firstOwn = match[0]
			}
			if entryPointRelations[match[0]] {
				schemaDeclares = true
			}
		}
		if named {
			schemaHeadlines[schema.Name] = schema.Comment
		}
		if firstOwn != "" && !schemaDeclares {
			entryPointRelations[firstOwn] = true
		}
	}
}

// firstSentence trims catalog prose to its opening claim. Relation names carry
// their dot without a following space, so ". " is an unambiguous boundary here.
func firstSentence(comment string) string {
	if idx := strings.Index(comment, ". "); idx >= 0 {
		return comment[:idx+1]
	}
	return comment
}

// capRunes shortens text to a rune budget on a word boundary and marks the cut,
// so a reader knows describe_table holds the rest. It counts runes because the
// catalog prose contains em dashes, which a byte-slice would split.
func capRunes(text string, max int) string {
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	cut := string(runes[:max])
	if idx := strings.LastIndexByte(cut, ' '); idx > 0 {
		cut = cut[:idx]
	}
	return strings.TrimRight(cut, " ,;:.—-") + "…"
}

// startHereBlock renders the catalog's own start-here guidance. It lives in
// warehouse_catalog.json so the schema overview, the Postgres schema comments,
// and any other surface all say the same thing without restating it.
func startHereBlock() string {
	var out strings.Builder
	for _, line := range warehouse.StartHere.Lines {
		for i, wrapped := range wrapComment(line, 92) {
			if i == 0 {
				out.WriteString("-- " + wrapped + "\n")
			} else {
				out.WriteString("--   " + wrapped + "\n")
			}
		}
	}
	return out.String()
}

// wrapComment soft-wraps a guidance line so the overview stays readable in a
// terminal without the catalog having to carry pre-wrapped text.
func wrapComment(line string, width int) []string {
	words := strings.Fields(line)
	if len(words) == 0 {
		return []string{""}
	}
	lines := []string{}
	current := words[0]
	for _, word := range words[1:] {
		if len(current)+1+len(word) > width {
			lines = append(lines, current)
			current = word
			continue
		}
		current += " " + word
	}
	return append(lines, current)
}

// overviewPreamble is everything a caller needs before writing SQL that no
// per-relation lookup can tell them. Each paragraph earned its place from a
// recurring failure in real transcripts.
const overviewPreamble = `--
-- SCHEMA LAYERS (they sort in this order, which is also the order to reach for them):
--   base_<source>    faithful provider data — every field the source gave us
--   derived_<domain> modelled facts: identity resolution, enrichment, transcripts, ledger history
--   marts_<domain>   stable domain read interfaces (finance, contacts, messages, photos, inbox, ...)
--   timeline         the cross-source event stream + search_text()/search_text_exact()
--   (sync cursors, credentials and helper functions are deliberately not queryable)
--
-- HOW TO USE THIS: relation names + keys + row counts only. For any other column, call
--   describe_table('base_gmail.messages')  →  every column with its exact Postgres type.
--   Do NOT guess column names: 70%% of failed warehouse queries are 42703 undefined-column.
-- Reference relations schema-qualified (FROM base_gmail.messages).
--   Never prefix the database name ("%s.").
--
-- THREE TYPE RULES YOU CANNOT INFER FROM A COLUMN NAME:
--  1. Booleans are bigint 0/1, not boolean. Every is_*/has_* column (is_from_me, is_deleted,
--     is_read, is_archived, ...) is bigint. Write ` + "`is_from_me = 1`" + `, never ` + "`NOT is_from_me`" + `,
--     ` + "`= true`" + `, or ` + "`FILTER (WHERE is_read)`" + ` — those raise 42804.
--  2. JSON columns are text on the older sources and real jsonb on the newer ones, so there is
--     no single rule. text on: base_slack, base_gmail, base_google_calendar, base_apple_*,
--     base_whatsapp, and the agent-session event tables (base_claude_code, base_claude_desktop,
--     base_codex, base_chatgpt, base_openclaw, base_pi). jsonb on: base_plaid, base_whoop,
--     base_google_drive, base_google_contacts, base_apple_contacts, base_apple_photos,
--     base_manual_finance, base_alice_voice_recordings. On a text one, cast before
--     -> / ->> / subscripting, else 42883. describe_table is authoritative — check, don't assume.
--  3. Time columns are per-source names, all timestamptz, given as ` + "`time:`" + ` on each line below.
--     Compare them to timestamps, never epoch ints (>= '2026-01-01', not > 1700000000).
--     Neighbouring lookalikes are NOT timestamps: base_slack.messages.message_ts/edited_ts and
--     base_gmail.messages.date_header are text; base_apple_messages.messages.date_ns is bigint
--     NANOseconds.
--
-- SEARCH CONTRACT: base_* tables serve STRUCTURED predicates (keys, senders, time ranges, joins).
-- ALL text search goes through timeline.* over the timeline document. Raw body columns are
-- deliberately not text-indexed — ILIKE/regex/hand-rolled cross-source UNIONs force full table
-- scans and will hit the statement timeout on the big tables.
--   timeline.search_text('offer letter', 50)        RANKED keyword search (BM25; scores are
--                                                   negative, more negative = better; terms are
--                                                   OR'd + stemmed whole words, no phrases/typos)
--   timeline.search_text_exact('offer letter', 50)  LITERAL substring/phrase/id match, recency-
--                                                   ordered. Use this for 'every mention of X';
--                                                   never post-filter search_text() with ILIKE.
--   both take (query, max_results, sources => ARRAY['slack','gmail'], since => '2026-03-01',
--   priorities => ARRAY['self','direct']) and return (source, subsource, context, who,
--   occurred_at, account, ref, text, score, event_ts, title, source_table, source_pk,
--   priority). ` + "`priorities`" + ` scopes to the attention tiers on timeline.events — self
--   (Zach initiated it), direct (a real person reaching him directly), cc (real-people
--   activity he is peripheral to), noise (bulk/automated, 82%% of the corpus), background
--   (warehouse machinery) — and is pushed into every scan, so a narrow tier still gets a full
--   top-k. Omit it (or pass an empty array) for every tier; an unknown token raises listing
--   the valid set. The ` + "`text`" + ` preview is windowed around the
--   first matched term. event_ts = occurred_at (same value, both names accepted downstream).
--   A hit carries its own drill-down: source_table + source_pk point at the source relation
--   (source_table is the catalog id, e.g. gmail_messages, not a SQL name; this listing shows
--   which schema each one lives in). Read the conversation around a hit with
--   timeline.context(ref, 5, 5): a Gmail hit's thread, a Slack hit's thread or channel, a
--   message's chat, else neighbors in the same (source, context) stream.
--   valid ` + "`sources`" + ` tokens: SELECT * FROM timeline.search_text_sources(); familiar aliases
--   (apple_messages, apple_notes, voice_memos, drive, contacts, photos, ...) resolve to the
--   right token, and an unknown token raises an error listing the valid set. Detail text
--   (attachments, media enrichments, Drive extracts) is folded into the parent event's search
--   document; agent-session transcripts are indexed per TURN (kind agent_turn), so a hit lands
--   on the matching turn — use timeline.context(ref) to page the rest of that session.
--   search_text_exact additionally matches number-format variants of the needle (thousands
--   separators both ways, phone punctuation stripped), so '1441.52' finds '1,441.52'.
--   For meetings: sources => ARRAY['transcript'] covers transcript, action_items, participants,
--   and summary. Summaries are lossy — before calling a request unanswered, search transcript
--   text and Slack DMs dated AFTER it; decisions are often made on calls only.
--   The ` + "`search`" + ` tool wraps this contract: hybrid semantic+keyword retrieval through
--   timeline.search_hybrid when embeddings are configured, falling back to search_text keyword
--   search (reporting a fallback_reason) when they are not; modes keyword/exact force the
--   corresponding SQL function.
--
-- Row counts are planner estimates; use them for sizing instead of SELECT COUNT(*).
--
-- READING THE LISTING: a ` + "`#`" + ` line under a schema heading is that schema's own guidance,
-- and the indented line under a relation says what it is for. ` + "`→`" + ` in the left margin marks
-- the relation to START WITH in that schema — the others are drill-downs off it. A trailing
-- ` + "`…`" + ` means the description was cut; describe_table prints it in full.
`

// relationFacts is the per-relation catalog the overview renders.
type relationFacts struct {
	ColumnCount int64
	TimeColumns []string
	PrimaryKey  []string
	RowEstimate int64
	HasEstimate bool
}

// overviewCatalog collects every relation fact in three catalog-wide queries.
// The previous overview issued one describe query per relation (108 of them,
// fanned out 16-wide) purely to print columns it no longer prints.
func (s *Service) overviewCatalog(ctx context.Context, tables []tableRef) map[string]*relationFacts {
	facts := make(map[string]*relationFacts, len(tables))
	for _, table := range tables {
		facts[table.DisplayName()] = &relationFacts{}
	}

	columnSQL := "SELECT n.nspname AS schema, c.relname AS name, " +
		"count(*)::bigint AS column_count, " +
		"array_to_string(array_agg(a.attname ORDER BY a.attnum) " +
		"FILTER (WHERE format_type(a.atttypid, a.atttypmod) = 'timestamp with time zone'), ',') AS time_columns " +
		"FROM pg_attribute a " +
		"JOIN pg_class c ON c.oid = a.attrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") " +
		"AND c.relkind IN ('r', 'p', 'm', 'v') AND a.attnum > 0 AND NOT a.attisdropped " +
		"GROUP BY 1, 2"
	if result, err := s.runner.Query(ctx, columnSQL, 0); err != nil {
		s.logger.WarnContext(ctx, "schema overview column catalog failed", "error", err)
	} else {
		for _, row := range result.Rows {
			entry := facts[rowString(row, "schema")+"."+rowString(row, "name")]
			if entry == nil {
				continue
			}
			entry.ColumnCount, _ = int64Value(row["column_count"])
			entry.TimeColumns = splitList(rowString(row, "time_columns"))
		}
	}

	primaryKeySQL := "SELECT n.nspname AS schema, c.relname AS name, " +
		"array_to_string(array_agg(a.attname ORDER BY k.ord), ',') AS pk_columns " +
		"FROM pg_class c " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary " +
		"CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) " +
		"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " +
		"WHERE n.nspname = ANY(" + queryableSchemaArraySQL() + ") " +
		"GROUP BY 1, 2"
	if result, err := s.runner.Query(ctx, primaryKeySQL, 0); err != nil {
		s.logger.WarnContext(ctx, "schema overview primary key lookup failed", "error", err)
	} else {
		for _, row := range result.Rows {
			entry := facts[rowString(row, "schema")+"."+rowString(row, "name")]
			if entry == nil {
				continue
			}
			entry.PrimaryKey = splitList(rowString(row, "pk_columns"))
		}
	}

	for name, estimate := range s.tableRowEstimates(ctx) {
		if entry := facts[name]; entry != nil && estimate >= 0 {
			entry.RowEstimate, entry.HasEstimate = estimate, true
		}
	}
	return facts
}

// timeColumnFor answers with the curated event-time column, or with the sole
// plausible candidate. When several remain it lists them and says so: naming
// the wrong one produces a query that runs, returns rows, and answers the wrong
// question, which is worse than an error.
func timeColumnFor(display string, facts *relationFacts) string {
	if curated, ok := overviewTimeColumns[display]; ok {
		return curated
	}
	if facts == nil || len(facts.TimeColumns) == 0 {
		return ""
	}
	candidates := make([]string, 0, len(facts.TimeColumns))
	for _, column := range facts.TimeColumns {
		if !bookkeepingTimeColumns[column] {
			candidates = append(candidates, column)
		}
	}
	switch {
	case len(candidates) == 1:
		return candidates[0]
	case len(candidates) == 0:
		if len(facts.TimeColumns) == 1 {
			return facts.TimeColumns[0]
		}
		return ""
	}
	if len(candidates) > 3 {
		candidates = append(candidates[:3], "...")
	}
	return strings.Join(candidates, "|") + "  (ambiguous — confirm with describe_table)"
}

func (s *Service) renderOverview(database string, tables []tableRef, facts map[string]*relationFacts, timelineColumns string) string {
	var out strings.Builder
	out.WriteString(startHereBlock())
	out.WriteString(fmt.Sprintf(overviewPreamble, database))
	out.WriteString("\n")

	bySchema := map[string][]tableRef{}
	schemas := make([]string, 0, 8)
	for _, table := range tables {
		if _, seen := bySchema[table.Schema]; !seen {
			schemas = append(schemas, table.Schema)
		}
		bySchema[table.Schema] = append(bySchema[table.Schema], table)
	}
	sort.Strings(schemas)

	width := 0
	for _, table := range tables {
		if n := len(table.DisplayName()); n > width {
			width = n
		}
	}
	width += 2

	for _, schema := range schemas {
		relations := bySchema[schema]
		out.WriteString(fmt.Sprintf("# %s (%d relation%s)\n", schema, len(relations), plural(len(relations))))
		for _, line := range wrapComment(schemaHeadlines[schema], 88) {
			if line != "" {
				out.WriteString("#   " + line + "\n")
			}
		}
		for _, table := range relations {
			display := table.DisplayName()
			entry := facts[display]
			fields := []string{fmt.Sprintf("%-*s", width, display), fmt.Sprintf("%9s", overviewRowCount(entry))}
			if entry != nil && entry.ColumnCount > 0 {
				fields = append(fields, fmt.Sprintf("%3d cols", entry.ColumnCount))
			}
			if entry != nil && len(entry.PrimaryKey) > 0 {
				fields = append(fields, "pk("+strings.Join(entry.PrimaryKey, ",")+")")
			}
			if timeColumn := timeColumnFor(display, entry); timeColumn != "" {
				fields = append(fields, "time: "+timeColumn)
			}
			gutter := "  "
			if entryPointRelations[display] {
				gutter = "→ "
			}
			out.WriteString(gutter + strings.Join(fields, "  ") + "\n")
			if comment := relationHeadlines[display]; comment != "" {
				out.WriteString("      " + capRunes(firstSentence(comment), overviewRelationCommentChars) + "\n")
			}
		}
		out.WriteString("\n")
	}

	if timelineColumns != "" {
		out.WriteString("# timeline.events — full column catalog (the cross-source entry point every\n")
		out.WriteString("# search result drills through; call describe_table for any other relation)\n")
		out.WriteString(timelineColumns)
		out.WriteString("\n")
	}
	return out.String()
}

// overviewRowCount renders a relation's size compactly. Views have no planner
// estimate and are labelled as views rather than as zero rows.
func overviewRowCount(facts *relationFacts) string {
	if facts == nil || !facts.HasEstimate {
		return "view"
	}
	switch n := facts.RowEstimate; {
	case n >= 1_000_000:
		return fmt.Sprintf("~%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("~%.0fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("~%d", n)
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
