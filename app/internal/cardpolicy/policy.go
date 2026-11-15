// Package cardpolicy reads the generated mirror of the single shared policy.
package cardpolicy

import "encoding/json"

type Issuer struct {
	Name    string
	Lengths []int
	Ranges  [][2]string
}
type Policy struct {
	AttachedLabels        []string `json:"attached_labels"`
	PaymentLabelPattern   string   `json:"payment_label_pattern"`
	EncodedTokenMinLength int      `json:"encoded_token_min_length"`
	Issuers               []Issuer
}

var Rules = func() Policy {
	var result Policy
	if err := json.Unmarshal([]byte(JSON), &result); err != nil {
		panic("invalid card policy")
	}
	return result
}()
