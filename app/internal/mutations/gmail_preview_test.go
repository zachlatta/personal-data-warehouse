package mutations

import (
	"strings"
	"testing"
	"time"
)

func TestApplyGmailThreadPreviewRowsMergesWarehouseMessages(t *testing.T) {
	older := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 5, 20, 9, 30, 0, 0, time.UTC)
	mutations := []Mutation{{
		ID:        "mut-1",
		Provider:  "gmail",
		Operation: GmailArchiveOperation,
		Account:   "zach@example.test",
		Payload:   map[string]any{"thread_ids": []any{"thread-1"}},
		Preview: map[string]any{
			"thread_count": 1,
			"context":      map[string]any{"source": "test"},
			"threads":      []any{map[string]any{"thread_id": "thread-1"}},
		},
	}}

	got := applyGmailThreadPreviewRows(mutations, []gmailThreadPreviewRow{
		{
			Account:           "zach@example.test",
			ThreadID:          "thread-1",
			MessageID:         "message-older",
			Subject:           "Receipt received",
			FromAddress:       "HCB <receipts@hcb.example>",
			ToAddresses:       []string{"zach@example.test"},
			LabelIDs:          []string{"INBOX", "CATEGORY_UPDATES"},
			InternalDate:      older,
			Snippet:           "We received your receipt.",
			PreviewText:       "We received your receipt and attached it to the transaction.",
			BodyHTML:          "<main>Receipt body</main>",
			MessageCount:      2,
			InboxMessageCount: 2,
		},
		{
			Account:           "zach@example.test",
			ThreadID:          "thread-1",
			MessageID:         "message-latest",
			Subject:           "Receipt received",
			FromAddress:       "HCB <receipts@hcb.example>",
			ToAddresses:       []string{"zach@example.test"},
			LabelIDs:          []string{"INBOX", "UNREAD", "Label_29"},
			InternalDate:      latest,
			Snippet:           "Everything is synced. No action is required.",
			PreviewText:       "![](https://tracking.example/open) | | | --- | --- | noisy table",
			MessageCount:      2,
			InboxMessageCount: 2,
		},
	})

	threads := mapSliceFromAny(got[0].Preview["threads"])
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", got[0].Preview["threads"])
	}
	thread := threads[0]
	if thread["subject"] != "Receipt received" || thread["latest_from_address"] != "HCB <receipts@hcb.example>" {
		t.Fatalf("thread summary = %#v", thread)
	}
	if thread["latest_preview"] != "Everything is synced. No action is required." {
		t.Fatalf("latest preview = %#v", thread["latest_preview"])
	}
	if thread["message_count"] != 2 || thread["inbox_message_count"] != 2 {
		t.Fatalf("message counts = %#v / %#v", thread["message_count"], thread["inbox_message_count"])
	}
	messages := mapSliceFromAny(thread["messages"])
	if len(messages) != 2 {
		t.Fatalf("messages = %#v", thread["messages"])
	}
	if messages[0]["message_id"] != "message-older" || messages[1]["message_id"] != "message-latest" {
		t.Fatalf("message order = %#v", messages)
	}
	if messages[0]["body_html"] != "<main>Receipt body</main>" {
		t.Fatalf("body_html = %#v", messages[0]["body_html"])
	}
	labels := stringSliceFromAny(thread["labels"])
	if strings.Join(labels, ",") != "Updates,Unread" {
		t.Fatalf("labels = %#v", thread["labels"])
	}
	if mutations[0].Preview["threads"].([]any)[0].(map[string]any)["subject"] != nil {
		t.Fatalf("original mutation preview was mutated: %#v", mutations[0].Preview)
	}
}

func TestApplyGmailThreadPreviewRowsAddsReplyThreadToGmailEmailMutations(t *testing.T) {
	latest := time.Date(2026, 5, 22, 15, 45, 0, 0, time.UTC)
	mutations := []Mutation{{
		ID:        "mut-email",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Payload: map[string]any{
			"message": map[string]any{
				"to":                 []any{"one@example.test"},
				"subject":            "Re: Existing thread",
				"body_text":          "Reply body",
				"reply_to_thread_id": "thread-reply",
			},
		},
		Preview: map[string]any{
			"email": map[string]any{"mode": "reply", "subject": "Re: Existing thread"},
		},
	}}

	got := applyGmailThreadPreviewRows(mutations, []gmailThreadPreviewRow{{
		Account:           "zach@example.test",
		ThreadID:          "thread-reply",
		MessageID:         "message-parent",
		Subject:           "Existing thread",
		FromAddress:       "Customer <customer@example.test>",
		ToAddresses:       []string{"zach@example.test"},
		LabelIDs:          []string{"INBOX", "CATEGORY_PERSONAL"},
		InternalDate:      latest,
		Snippet:           "Can you take a look?",
		PreviewText:       "Can you take a look at this before Friday?",
		BodyHTML:          "<p>Can you take a look?</p>",
		MessageCount:      1,
		InboxMessageCount: 1,
	}})

	threads := mapSliceFromAny(got[0].Preview["reply_threads"])
	if len(threads) != 1 {
		t.Fatalf("reply_threads = %#v", got[0].Preview["reply_threads"])
	}
	thread := threads[0]
	if thread["thread_id"] != "thread-reply" || thread["subject"] != "Existing thread" {
		t.Fatalf("reply thread summary = %#v", thread)
	}
	messages := mapSliceFromAny(thread["messages"])
	if len(messages) != 1 || messages[0]["body_html"] != "<p>Can you take a look?</p>" {
		t.Fatalf("reply thread messages = %#v", thread["messages"])
	}
	if got[0].Preview["reply_thread_count"] != 1 {
		t.Fatalf("reply_thread_count = %#v", got[0].Preview["reply_thread_count"])
	}
}

func TestGmailThreadPreviewTargetsIncludeGmailEmailVariantReplyThreads(t *testing.T) {
	targets := gmailThreadPreviewTargets([]Mutation{{
		ID:        "mut-email",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Payload: map[string]any{
			"message": map[string]any{
				"reply_to_thread_id": "thread-direct",
			},
			"variants": []map[string]any{{
				"id":    "variant_1",
				"title": "Direct Reply",
				"message": map[string]any{
					"reply_to_thread_id": "thread-direct",
				},
			}, {
				"id":    "variant_2",
				"title": "Softer Ask",
				"message": map[string]any{
					"reply_to_thread_id": "thread-softer",
				},
			}},
		},
	}})

	if len(targets) != 2 {
		t.Fatalf("targets = %#v", targets)
	}
	if targets[0].ThreadID != "thread-direct" || targets[1].ThreadID != "thread-softer" {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestCleanGmailPreviewTextRemovesQuotedReply(t *testing.T) {
	got := cleanGmailPreviewText("Attached receipt -- Zach On Wed, May 13, 2026 at 8:25 AM HCB <hcb@hackclub.com> wrote: parent body")

	if got != "Attached receipt -- Zach" {
		t.Fatalf("cleaned preview = %q", got)
	}
}
