package mutations

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Service struct {
	store Store
	cfg   Config
}

type ProposalResponse struct {
	RequestID   string   `json:"request_id"`
	MutationIDs []string `json:"mutation_ids"`
	ApprovalURL string   `json:"approval_url"`
	Status      string   `json:"status"`
}

type ProposeMutationRequestInput struct {
	Title     string           `json:"title" jsonschema:"short human-readable title for the reviewed request"`
	Reason    string           `json:"reason" jsonschema:"why these mutations should be reviewed and approved"`
	Mutations []map[string]any `json:"mutations" jsonschema:"one or more mutation objects"`
	Context   map[string]any   `json:"context,omitempty" jsonschema:"optional source context for human review"`
}

type ProposeGmailArchiveThreadsInput struct {
	Account   string         `json:"account" jsonschema:"configured Gmail account email address"`
	ThreadIDs []string       `json:"thread_ids" jsonschema:"Gmail thread IDs to archive"`
	Reason    string         `json:"reason" jsonschema:"why these threads should be archived"`
	Title     string         `json:"title,omitempty" jsonschema:"optional review request title"`
	Context   map[string]any `json:"context,omitempty" jsonschema:"optional source context for human review"`
}

type ProposeGmailUnarchiveThreadsInput struct {
	Account   string         `json:"account" jsonschema:"configured Gmail account email address"`
	ThreadIDs []string       `json:"thread_ids" jsonschema:"Gmail thread IDs to unarchive"`
	Reason    string         `json:"reason" jsonschema:"why these threads should be returned to the inbox"`
	Title     string         `json:"title,omitempty" jsonschema:"optional review request title"`
	Context   map[string]any `json:"context,omitempty" jsonschema:"optional source context for human review"`
}

type ProposeGmailSendEmailInput struct {
	Account         string         `json:"account" jsonschema:"configured Gmail account email address"`
	To              []string       `json:"to" jsonschema:"primary recipients"`
	CC              []string       `json:"cc,omitempty" jsonschema:"carbon-copy recipients"`
	BCC             []string       `json:"bcc,omitempty" jsonschema:"blind-copy recipients"`
	Subject         string         `json:"subject" jsonschema:"email subject"`
	BodyText        string         `json:"body_text" jsonschema:"plain-text email body"`
	BodyHTML        string         `json:"body_html,omitempty" jsonschema:"optional HTML email body"`
	ReplyToThreadID string         `json:"reply_to_thread_id,omitempty" jsonschema:"optional Gmail thread ID to reply in"`
	DeliveryMode    string         `json:"delivery_mode,omitempty" jsonschema:"send or draft; default send"`
	Reason          string         `json:"reason" jsonschema:"why this email should be sent or drafted"`
	Title           string         `json:"title,omitempty" jsonschema:"optional review request title"`
	Context         map[string]any `json:"context,omitempty" jsonschema:"optional source context for human review"`
}

type ProposeContactMutationsInput struct {
	Account    string           `json:"account" jsonschema:"configured Google Contacts account email address"`
	Operations []map[string]any `json:"operations" jsonschema:"contact create, update, or delete operation objects"`
	Reason     string           `json:"reason" jsonschema:"why these contact mutations should be applied"`
	Title      string           `json:"title,omitempty" jsonschema:"optional review request title"`
	Context    map[string]any   `json:"context,omitempty" jsonschema:"optional source context for human review"`
}

func NewService(store Store, cfg Config) *Service {
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	if strings.TrimSpace(cfg.SessionSecret) == "" {
		if secret, err := randomToken(); err == nil {
			cfg.SessionSecret = secret
		}
	}
	cfg.GmailAccounts = normalizeAccountList(cfg.GmailAccounts)
	cfg.ContactGoogleAccounts = normalizeAccountList(cfg.ContactGoogleAccounts)
	return &Service{store: store, cfg: cfg}
}

func (s *Service) ProposeMutationRequest(ctx context.Context, input ProposeMutationRequestInput) (ProposalResponse, error) {
	mutations := make([]MutationInput, 0, len(input.Mutations))
	for index, raw := range input.Mutations {
		mutation, err := mutationInputFromMap(raw, index)
		if err != nil {
			return ProposalResponse{}, err
		}
		mutations = append(mutations, mutation)
	}
	return s.createRequest(ctx, CreateRequestInput{
		Title:       strings.TrimSpace(input.Title),
		Reason:      strings.TrimSpace(input.Reason),
		Context:     cloneMap(input.Context),
		Mutations:   mutations,
		RequestedBy: defaultRequestedBy,
	})
}

func (s *Service) ProposeGmailArchiveThreads(ctx context.Context, input ProposeGmailArchiveThreadsInput) (ProposalResponse, error) {
	threadIDs := normalizeStringSlice(input.ThreadIDs)
	return s.createRequest(ctx, CreateRequestInput{
		Title:   optionalTitle(input.Title, fmt.Sprintf("Archive %d Gmail thread%s", len(threadIDs), plural(len(threadIDs)))),
		Reason:  strings.TrimSpace(input.Reason),
		Context: cloneMap(input.Context),
		Mutations: []MutationInput{{
			Type:      GmailArchiveOperation,
			Account:   normalizeAccount(input.Account),
			ThreadIDs: threadIDs,
		}},
		RequestedBy: defaultRequestedBy,
	})
}

func (s *Service) ProposeGmailUnarchiveThreads(ctx context.Context, input ProposeGmailUnarchiveThreadsInput) (ProposalResponse, error) {
	threadIDs := normalizeStringSlice(input.ThreadIDs)
	return s.createRequest(ctx, CreateRequestInput{
		Title:   optionalTitle(input.Title, fmt.Sprintf("Unarchive %d Gmail thread%s", len(threadIDs), plural(len(threadIDs)))),
		Reason:  strings.TrimSpace(input.Reason),
		Context: cloneMap(input.Context),
		Mutations: []MutationInput{{
			Type:      GmailUnarchiveOperation,
			Account:   normalizeAccount(input.Account),
			ThreadIDs: threadIDs,
		}},
		RequestedBy: defaultRequestedBy,
	})
}

func (s *Service) ProposeGmailSendEmail(ctx context.Context, input ProposeGmailSendEmailInput) (ProposalResponse, error) {
	deliveryMode := strings.TrimSpace(input.DeliveryMode)
	if deliveryMode == "" {
		deliveryMode = "send"
	}
	message := map[string]any{
		"to":                 normalizeStringSlice(input.To),
		"cc":                 normalizeStringSlice(input.CC),
		"bcc":                normalizeStringSlice(input.BCC),
		"subject":            strings.TrimSpace(input.Subject),
		"body_text":          input.BodyText,
		"body_html":          input.BodyHTML,
		"reply_to_thread_id": strings.TrimSpace(input.ReplyToThreadID),
	}
	return s.createRequest(ctx, CreateRequestInput{
		Title:   optionalTitle(input.Title, emailRequestTitle(message)),
		Reason:  strings.TrimSpace(input.Reason),
		Context: cloneMap(input.Context),
		Mutations: []MutationInput{{
			Type:         GmailSendEmailOperation,
			Account:      normalizeAccount(input.Account),
			DeliveryMode: deliveryMode,
			Message:      message,
		}},
		RequestedBy: defaultRequestedBy,
	})
}

func (s *Service) ProposeContactMutations(ctx context.Context, input ProposeContactMutationsInput) (ProposalResponse, error) {
	operations := cloneMapSlice(input.Operations)
	return s.createRequest(ctx, CreateRequestInput{
		Title:   optionalTitle(input.Title, fmt.Sprintf("Apply %d contact mutation%s", len(operations), plural(len(operations)))),
		Reason:  strings.TrimSpace(input.Reason),
		Context: cloneMap(input.Context),
		Mutations: []MutationInput{{
			Type:       GooglePeopleContactsOperation,
			Account:    normalizeAccount(input.Account),
			Operations: operations,
		}},
		RequestedBy: defaultRequestedBy,
	})
}

func (s *Service) createRequest(ctx context.Context, input CreateRequestInput) (ProposalResponse, error) {
	if s == nil || s.store == nil {
		return ProposalResponse{}, errors.New("mutation store is not configured")
	}
	input.Title = strings.TrimSpace(input.Title)
	input.Reason = strings.TrimSpace(input.Reason)
	if input.RequestedBy == "" {
		input.RequestedBy = defaultRequestedBy
	}
	if err := s.validateCreateInput(input); err != nil {
		return ProposalResponse{}, err
	}
	request, err := s.store.CreateRequest(ctx, input)
	if err != nil {
		return ProposalResponse{}, err
	}
	return s.responseForRequest(request), nil
}

func (s *Service) validateCreateInput(input CreateRequestInput) error {
	if input.Title == "" {
		return errors.New("title must not be blank")
	}
	if input.Reason == "" {
		return errors.New("reason must not be blank")
	}
	if len(input.Mutations) == 0 {
		return errors.New("mutations must include at least one mutation")
	}
	for index, mutation := range input.Mutations {
		if err := s.validateMutation(index, mutation); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) validateMutation(index int, mutation MutationInput) error {
	mutationType := strings.TrimSpace(mutation.Type)
	account := normalizeAccount(mutation.Account)
	if account == "" {
		return fmt.Errorf("mutation %d must include account", index)
	}
	switch mutationType {
	case GmailArchiveOperation, GmailUnarchiveOperation:
		if err := validateConfiguredAccount(account, s.cfg.GmailAccounts, "GMAIL_ACCOUNTS"); err != nil {
			return err
		}
		if len(normalizeStringSlice(mutation.ThreadIDs)) == 0 {
			return fmt.Errorf("mutation %d must include thread_ids", index)
		}
	case GmailSendEmailOperation:
		if err := validateConfiguredAccount(account, s.cfg.GmailAccounts, "GMAIL_ACCOUNTS"); err != nil {
			return err
		}
		deliveryMode, err := normalizeDeliveryMode(mutation.DeliveryMode)
		if err != nil {
			return err
		}
		_ = deliveryMode
		if !hasAnyRecipient(mutation.Message) {
			return fmt.Errorf("mutation %d Gmail email mutation must include at least one recipient", index)
		}
		if strings.TrimSpace(stringFromAny(mutation.Message["subject"])) == "" {
			return fmt.Errorf("mutation %d Gmail email mutation must include subject", index)
		}
		if strings.TrimSpace(stringFromAny(mutation.Message["body_text"])) == "" && strings.TrimSpace(stringFromAny(mutation.Message["body_html"])) == "" {
			return fmt.Errorf("mutation %d Gmail email mutation must include body_text or body_html", index)
		}
	case GooglePeopleContactsOperation, ContactsBatchMutationOperation:
		if err := validateConfiguredAccount(account, s.cfg.ContactGoogleAccounts, "CONTACT_GOOGLE_ACCOUNTS"); err != nil {
			return err
		}
		if len(mutation.Operations) == 0 {
			return fmt.Errorf("mutation %d must include operations", index)
		}
	default:
		return fmt.Errorf("mutation %d has unsupported type %q; expected gmail.archive_threads, gmail.unarchive_threads, gmail.send_email, or google_people.contacts", index, mutationType)
	}
	return nil
}

func (s *Service) responseForRequest(request Request) ProposalResponse {
	mutationIDs := make([]string, 0, len(request.Mutations))
	for _, mutation := range request.Mutations {
		mutationIDs = append(mutationIDs, mutation.ID)
	}
	return ProposalResponse{
		RequestID:   request.ID,
		MutationIDs: mutationIDs,
		ApprovalURL: s.requestURL(request.ID),
		Status:      request.Status,
	}
}

func (s *Service) requestURL(requestID string) string {
	if s == nil || s.cfg.BaseURL == "" {
		return ReviewPath + "/requests/" + url.PathEscape(requestID)
	}
	return s.cfg.BaseURL + ReviewPath + "/requests/" + url.PathEscape(requestID)
}

func mutationInputFromMap(raw map[string]any, index int) (MutationInput, error) {
	if raw == nil {
		return MutationInput{}, fmt.Errorf("mutation %d must be an object", index)
	}
	mutationType := stringFromAny(raw["type"])
	if strings.TrimSpace(mutationType) == "" {
		mutationType = stringFromAny(raw["operation"])
	}
	message := mapFromAny(raw["message"])
	deliveryMode := stringFromAny(raw["delivery_mode"])
	if deliveryMode == "" {
		deliveryMode = "send"
	}
	return MutationInput{
		Type:         strings.TrimSpace(mutationType),
		Account:      normalizeAccount(stringFromAny(raw["account"])),
		Title:        strings.TrimSpace(stringFromAny(raw["title"])),
		Reason:       strings.TrimSpace(stringFromAny(raw["reason"])),
		ThreadIDs:    stringSliceFromAny(raw["thread_ids"]),
		DeliveryMode: deliveryMode,
		Message:      message,
		Operations:   mapSliceFromAny(raw["operations"]),
		Raw:          cloneMap(raw),
	}, nil
}

func optionalTitle(value string, fallback string) string {
	if title := strings.TrimSpace(value); title != "" {
		return title
	}
	return fallback
}

func plural(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func emailRequestTitle(message map[string]any) string {
	subject := strings.TrimSpace(stringFromAny(message["subject"]))
	if subject == "" {
		return "Send email"
	}
	return "Send email: " + subject
}

func normalizeDeliveryMode(value string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "" {
		mode = "send"
	}
	if mode != "send" && mode != "draft" {
		return "", fmt.Errorf("delivery_mode must be send or draft")
	}
	return mode, nil
}

func hasAnyRecipient(message map[string]any) bool {
	for _, field := range []string{"to", "cc", "bcc"} {
		if len(stringSliceFromAny(message[field])) > 0 {
			return true
		}
	}
	return false
}

func validateConfiguredAccount(account string, configured []string, envName string) error {
	if len(configured) == 0 {
		return nil
	}
	for _, candidate := range configured {
		if candidate == account {
			return nil
		}
	}
	return fmt.Errorf("%s is not configured in %s (%s)", account, envName, strings.Join(configured, ", "))
}

func normalizeAccount(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeAccountList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		account := normalizeAccount(value)
		if account != "" && !seen[account] {
			out = append(out, account)
			seen[account] = true
		}
	}
	return out
}

func normalizeStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case nil:
		return nil
	case []string:
		return normalizeStringSlice(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(stringFromAny(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func mapFromAny(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	if typed, ok := value.(map[string]any); ok {
		return cloneMap(typed)
	}
	return map[string]any{}
}

func mapSliceFromAny(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return cloneMapSlice(typed)
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if entry, ok := item.(map[string]any); ok {
				out = append(out, cloneMap(entry))
			}
		}
		return out
	default:
		return nil
	}
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneMapSlice(input []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(input))
	for _, item := range input {
		out = append(out, cloneMap(item))
	}
	return out
}
