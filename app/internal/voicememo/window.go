// Package voicememo contains the voice memo workflow age policy.
package voicememo

// WorkflowWindowHours is the age window of the external voice memo workflow feed.
// Recorder recovery only accepts recordings strictly older than this window.
// TestRecoveryWindowMatchesExternalFeed checks the cutoff in the feed SQL literal.
const WorkflowWindowHours = 48
