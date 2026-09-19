package plaid

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// AlreadyRemovedErrorCodes are the Plaid errors that mean "this Item is
// already gone on Plaid's side": the local rows are then the only thing left
// to clean up, so /item/remove failing with one of these must not block the
// delete.
var AlreadyRemovedErrorCodes = map[string]bool{
	"ITEM_NOT_FOUND":       true,
	"INVALID_ACCESS_TOKEN": true,
}

// ResolveItem finds one linked Item by exact id or unambiguous id prefix.
//
// Plaid item ids are long opaque strings that are usually read off a table
// that truncated them, so a prefix is what an operator actually has in hand.
// Ambiguity is an error, never a guess — this selects rows to delete.
func ResolveItem(items []LinkedItem, needle string) (LinkedItem, error) {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return LinkedItem{}, errors.New("an item id is required")
	}
	for _, item := range items {
		if item.ItemID == needle {
			return item, nil
		}
	}
	var matches []LinkedItem
	for _, item := range items {
		if strings.HasPrefix(item.ItemID, needle) {
			matches = append(matches, item)
		}
	}
	if len(matches) == 0 {
		return LinkedItem{}, fmt.Errorf("no linked Plaid item matches '%s'", needle)
	}
	if len(matches) > 1 {
		ids := make([]string, 0, len(matches))
		for _, item := range matches {
			ids = append(ids, item.ItemID)
		}
		sort.Strings(ids)
		return LinkedItem{}, fmt.Errorf("'%s' matches %d linked Plaid items: %s", needle, len(matches), strings.Join(ids, ", "))
	}
	return matches[0], nil
}

func describeItem(item LinkedItem) string {
	return item.ItemID + " (" + institutionLabel(item) + ")"
}

func institutionLabel(item LinkedItem) string {
	return common.FirstNonEmpty(item.InstitutionName, item.InstitutionID, "unknown institution")
}

func formatCounts(counts map[string]int64) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, " ")
}

// UnlinkItem retires one linked Plaid Item: revoke it at Plaid, then delete
// its rows. Plaid is revoked first: if that fails for any reason other than
// the Item already being gone, nothing is deleted, so a retry is safe.
func UnlinkItem(ctx context.Context, store Store, client API, item LinkedItem, confirm func(prompt string) bool, out io.Writer, dryRun, skipRemote bool) (int, error) {
	accounts, err := store.LoadItemAccounts(ctx, item.Account, item.ItemID)
	if err != nil {
		return 1, err
	}
	counts, err := store.CountItemRows(ctx, item.Account, item.ItemID)
	if err != nil {
		return 1, err
	}
	fmt.Fprintf(out, "Plaid item %s\n", describeItem(item))
	for _, account := range accounts {
		removed := ""
		if account.IsRemoved != 0 {
			removed = " [removed]"
		}
		mask := account.Mask
		if mask == "" {
			mask = "----"
		}
		fmt.Fprintf(out, "  account %s %s (%s/%s) balance %s%s\n",
			mask, account.Name, account.Type, account.Subtype, common.FloatRepr(account.CurrentBalance), removed)
	}
	fmt.Fprintf(out, "  rows to delete: %s\n", formatCounts(counts))
	if dryRun {
		fmt.Fprintln(out, "Dry run: nothing was revoked or deleted.")
		return 0, nil
	}
	if !confirm(fmt.Sprintf("Revoke %s at Plaid and delete its warehouse rows?", describeItem(item))) {
		fmt.Fprintln(out, "Aborted; nothing was revoked or deleted.")
		return 1, nil
	}
	if skipRemote {
		fmt.Fprintln(out, "Skipping Plaid /item/remove (--skip-remote).")
	} else {
		if _, err := client.ItemRemove(item.AccessToken); err != nil {
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				return 1, err
			}
			message := Redact(apiErr.Message, item.AccessToken)
			if !AlreadyRemovedErrorCodes[ErrorCode(message)] {
				fmt.Fprintf(out, "Plaid refused to remove the item: %s\n", message)
				fmt.Fprintln(out, "Nothing was deleted; fix the error and re-run.")
				return 1, nil
			}
			fmt.Fprintf(out, "Plaid has already forgotten this item (%s); deleting local rows.\n", message)
		} else {
			fmt.Fprintln(out, "Revoked at Plaid.")
		}
	}
	deleted, err := store.DeleteItem(ctx, item.Account, item.ItemID)
	if err != nil {
		return 1, err
	}
	fmt.Fprintf(out, "Deleted: %s\n", formatCounts(deleted))
	fmt.Fprintln(out, "The finance ledger reconciles on its next run: a re-linked account merges back into the "+
		"logical account it duplicated, and the duplicated transactions disappear.")
	return 0, nil
}

// confirmOnStdin asks on stdout and reads one line from stdin; EOF is "no".
func confirmOnStdin(stdin io.Reader, stdout io.Writer) func(string) bool {
	reader := bufio.NewReader(stdin)
	return func(prompt string) bool {
		fmt.Fprintf(stdout, "%s [y/N] ", prompt)
		line, err := reader.ReadString('\n')
		if err != nil && line == "" {
			return false
		}
		answer := strings.ToLower(strings.TrimSpace(line))
		return answer == "y" || answer == "yes"
	}
}

// OpenBrowser opens url in the user's browser; overridable for tests.
var OpenBrowser = func(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

// linkFlow is the shared link/update procedure (run_link in cli.py).
type linkFlow struct {
	cfg        Config
	store      Store
	client     API
	newServer  func(mode LinkMode, linkToken, clientName, host string, port int) (*LinkServer, error)
	open       func(url string) error
	stdout     io.Writer
	stderr     io.Writer
	host       string
	port       int
	noBrowser  bool
	update     bool   // `update` rather than `link`
	updateItem string // the item id / prefix given to `update`
	now        func() time.Time
}

func (f *linkFlow) run(ctx context.Context) int {
	if err := f.store.EnsurePlaidTables(ctx); err != nil {
		return f.fail()
	}
	var item *LinkedItem
	if f.update {
		items, err := f.store.LoadItemTokens(ctx)
		if err != nil {
			return f.fail()
		}
		resolved, err := ResolveItem(items, f.updateItem)
		if err != nil {
			fmt.Fprintf(f.stderr, "pdw ingest plaid update: %s\n", err)
			fmt.Fprintln(f.stderr, "Run `pdw ingest plaid items` to list linked items.")
			return 2
		}
		item = &resolved
	}
	var tokenResponse map[string]any
	var err error
	if item != nil {
		tokenResponse, err = f.client.CreateLinkToken(item.Account, item.AccessToken)
	} else {
		tokenResponse, err = f.client.CreateLinkToken(f.cfg.Account, "")
	}
	if err != nil {
		return f.fail()
	}
	linkToken := stringValue(tokenResponse["link_token"])
	if linkToken == "" {
		return f.fail()
	}
	mode := ModeLink
	if item != nil {
		mode = ModeUpdate
	}
	server, err := f.newServer(mode, linkToken, f.cfg.ClientName, f.host, f.port)
	if err != nil {
		return f.fail()
	}
	if err := server.Start(); err != nil {
		return f.fail()
	}
	defer server.Close()
	fmt.Fprintln(f.stdout, "Open this URL to authorize Plaid accounts:")
	fmt.Fprintln(f.stdout, server.URL())
	if !f.noBrowser {
		_ = f.open(server.URL())
	}
	result, err := server.WaitForResult()
	if err != nil {
		return f.fail()
	}
	if item != nil {
		fmt.Fprintf(f.stdout, "Existing Plaid Item %s updated; identity and credential unchanged.\n", item.ItemID)
		response, err := f.client.AccountsGet(item.AccessToken)
		var accounts []any
		if err == nil {
			accounts, _ = response["accounts"].([]any)
			if response["accounts"] == nil {
				err = errors.New("missing accounts")
			}
		}
		if err != nil {
			// Provider messages can echo credentials. Never print raw errors.
			fmt.Fprintln(f.stderr, "Account availability could not be verified; existing Item was kept.")
			return 1
		}
		fmt.Fprintf(f.stdout, "accounts available: %d\n", len(accounts))
		if len(accounts) == 0 {
			return 1
		}
		return 0
	}
	exchange, err := f.client.ExchangePublicToken(result.PublicToken)
	if err != nil {
		return f.fail()
	}
	accessToken := stringValue(exchange["access_token"])
	itemID := stringValue(exchange["item_id"])
	if accessToken == "" || itemID == "" {
		return f.fail()
	}
	err = f.store.UpsertItemToken(ctx, LinkedItem{
		Account:         f.cfg.Account,
		ItemID:          itemID,
		AccessToken:     accessToken,
		InstitutionID:   result.InstitutionID,
		InstitutionName: result.InstitutionName,
	}, f.now())
	if err != nil {
		return f.fail()
	}
	fmt.Fprintln(f.stdout, "Plaid institution linked successfully.")
	return 0
}

// fail is the one message every link/update failure prints: link tokens and
// credentials must not escape through error text.
func (f *linkFlow) fail() int {
	fmt.Fprintln(f.stderr, "Plaid Link failed or was canceled; no Item was replaced or deleted.")
	return 1
}

func runItems(ctx context.Context, store Store, stdout io.Writer) (int, error) {
	if err := store.EnsurePlaidTables(ctx); err != nil {
		return 1, err
	}
	items, err := store.LoadItemTokens(ctx)
	if err != nil {
		return 1, err
	}
	if len(items) == 0 {
		fmt.Fprintln(stdout, "No linked Plaid items. Run `pdw ingest plaid link` to add one.")
		return 0, nil
	}
	for _, item := range items {
		counts, err := store.CountItemRows(ctx, item.Account, item.ItemID)
		if err != nil {
			return 1, err
		}
		fmt.Fprintf(stdout, "%s  %s  accounts=%d transactions=%d\n",
			item.ItemID, institutionLabel(item), counts["plaid_accounts"], counts["plaid_transactions"])
	}
	return 0, nil
}
