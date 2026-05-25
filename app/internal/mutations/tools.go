package mutations

import (
	"context"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const mutationToolDescription = "Create a pending upstream mutation request for human review in the Personal Data Warehouse. The tool does not execute the mutation; it returns a request_id and approval_url for the web review UI."

// Tools returns the propose_* MCP tools backed by the given service. Returns
// nil when service is nil so callers can pass the result straight into a
// tool.Registry without a nil guard.
func Tools(service *Service) []tool.Tool {
	if service == nil {
		return nil
	}
	return []tool.Tool{
		&tool.Typed[ProposeMutationRequestInput, ProposalResponse]{
			NameStr:        "propose_mutation_request",
			TitleStr:       "Propose Mutation Request",
			DescriptionStr: mutationToolDescription + " Use this when a task requires multiple Gmail or Google Contacts changes to be reviewed together.",
			Handle: func(ctx context.Context, in ProposeMutationRequestInput) (ProposalResponse, error) {
				return service.ProposeMutationRequest(ctx, in)
			},
		},
		&tool.Typed[ProposeGmailArchiveThreadsInput, ProposalResponse]{
			NameStr:        "propose_gmail_archive_threads",
			TitleStr:       "Propose Gmail Archive",
			DescriptionStr: mutationToolDescription + " Proposes removing INBOX from one or more Gmail threads.",
			Handle: func(ctx context.Context, in ProposeGmailArchiveThreadsInput) (ProposalResponse, error) {
				return service.ProposeGmailArchiveThreads(ctx, in)
			},
		},
		&tool.Typed[ProposeGmailUnarchiveThreadsInput, ProposalResponse]{
			NameStr:        "propose_gmail_unarchive_threads",
			TitleStr:       "Propose Gmail Unarchive",
			DescriptionStr: mutationToolDescription + " Proposes adding INBOX back to one or more Gmail threads.",
			Handle: func(ctx context.Context, in ProposeGmailUnarchiveThreadsInput) (ProposalResponse, error) {
				return service.ProposeGmailUnarchiveThreads(ctx, in)
			},
		},
		&tool.Typed[ProposeGmailSendEmailInput, ProposalResponse]{
			NameStr:        "propose_gmail_send_email",
			TitleStr:       "Propose Gmail Email",
			DescriptionStr: mutationToolDescription + " Proposes sending or drafting a Gmail email. May include one email or multiple variants; each variant needs a short two-word title for the review tabs.",
			Handle: func(ctx context.Context, in ProposeGmailSendEmailInput) (ProposalResponse, error) {
				return service.ProposeGmailSendEmail(ctx, in)
			},
		},
		&tool.Typed[ProposeContactMutationsInput, ProposalResponse]{
			NameStr:        "propose_contact_mutations",
			TitleStr:       "Propose Contact Mutations",
			DescriptionStr: mutationToolDescription + " Proposes reviewed Google Contacts create, update, or delete operations.",
			Handle: func(ctx context.Context, in ProposeContactMutationsInput) (ProposalResponse, error) {
				return service.ProposeContactMutations(ctx, in)
			},
		},
		&tool.Typed[ProposeCalendarCreateEventInput, ProposalResponse]{
			NameStr:        "propose_calendar_create_event",
			TitleStr:       "Propose Calendar Create Event",
			DescriptionStr: mutationToolDescription + " Proposes creating a new Google Calendar event. The event body is the standard Google Calendar event resource (summary, description, start, end, attendees, recurrence, etc.).",
			Handle: func(ctx context.Context, in ProposeCalendarCreateEventInput) (ProposalResponse, error) {
				return service.ProposeCalendarCreateEvent(ctx, in)
			},
		},
		&tool.Typed[ProposeCalendarUpdateEventInput, ProposalResponse]{
			NameStr:        "propose_calendar_update_event",
			TitleStr:       "Propose Calendar Update Event",
			DescriptionStr: mutationToolDescription + " Proposes patching an existing Google Calendar event. Only fields listed in `patch` are replaced; pass an instance event_id to override a single occurrence of a recurring event. expected_etag enables optimistic concurrency by rejecting the mutation if the event changes between proposal and execution.",
			Handle: func(ctx context.Context, in ProposeCalendarUpdateEventInput) (ProposalResponse, error) {
				return service.ProposeCalendarUpdateEvent(ctx, in)
			},
		},
		&tool.Typed[ProposeCalendarDeleteEventInput, ProposalResponse]{
			NameStr:        "propose_calendar_delete_event",
			TitleStr:       "Propose Calendar Delete Event",
			DescriptionStr: mutationToolDescription + " Proposes deleting a Google Calendar event. Pass an instance event_id to cancel a single occurrence of a recurring event; pass expected_etag to fail-fast if the event changed since proposal.",
			Handle: func(ctx context.Context, in ProposeCalendarDeleteEventInput) (ProposalResponse, error) {
				return service.ProposeCalendarDeleteEvent(ctx, in)
			},
		},
	}
}
