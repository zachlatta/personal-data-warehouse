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
	}
}
