// Package applecontacts uploads the Mac's Address Book stores (local and
// iCloud/account) through the app's /ingest/apple-contacts/batch endpoint.
package applecontacts

import (
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultStorePath is the Address Book root.
const DefaultStorePath = "~/Library/Application Support/AddressBook"

// StoreFilename is the Core Data store inside each source directory.
const StoreFilename = "AddressBook-v22.abcddb"

// Contact is one card as the uploader ships it.
type Contact struct {
	SourceID      string
	ContactID     string
	SourceUID     string
	DisplayName   string
	GivenName     string
	MiddleName    string
	FamilyName    string
	Nickname      string
	Organization  string
	Department    string
	JobTitle      string
	PrimaryEmail  string
	PrimaryPhone  string
	Emails        []map[string]any
	Phones        []map[string]any
	Addresses     []map[string]any
	Organizations []map[string]any
	URLs          []map[string]any
	Nicknames     []map[string]any
	Groups        []map[string]any
	Dates         map[string]any
	Photos        []map[string]any
	Note          string
	CreatedAt     time.Time
	ModifiedAt    time.Time
	Raw           map[string]any
}

// Store is one discovered Address Book database.
type Store struct {
	SourceID string
	Path     string
}

// DiscoverStores lists every AddressBook-v22.abcddb under the root (or the
// root itself when it is a file): "local" for the top-level store, the
// account source directory's name for the rest.
func DiscoverStores(storePath string) ([]Store, error) {
	path := common.ExpandUser(storePath)
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("No Apple Contacts stores found under %s", path)
	}
	if !info.IsDir() {
		sourceID := filepath.Base(filepath.Dir(path))
		if sourceID == "" || sourceID == "." || sourceID == "/" {
			sourceID = "default"
		}
		return []Store{{SourceID: sourceID, Path: path}}, nil
	}
	var candidates []string
	_ = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if d.Name() == StoreFilename {
			if fi, err := d.Info(); err == nil && fi.Mode().IsRegular() {
				candidates = append(candidates, p)
			}
		}
		return nil
	})
	sort.Strings(candidates)
	var stores []Store
	for _, candidate := range candidates {
		sourceID := filepath.Base(filepath.Dir(candidate))
		if filepath.Dir(candidate) == path {
			sourceID = "local"
		}
		stores = append(stores, Store{SourceID: sourceID, Path: candidate})
	}
	if len(stores) == 0 {
		return nil, fmt.Errorf("No Apple Contacts stores found under %s", path)
	}
	return stores, nil
}

// Snapshot copies a live store to destination.
func Snapshot(storePath, destination string) error {
	return common.SnapshotSQLite(common.ExpandUser(storePath), destination)
}

// Scan reads every contact from a (snapshotted) store.
func Scan(storePath, sourceID string) ([]Contact, error) {
	db, err := common.OpenSQLite(storePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	if sourceID == "" {
		sourceID = filepath.Base(filepath.Dir(storePath))
		if sourceID == "" || sourceID == "." {
			sourceID = "default"
		}
	}
	if tables["contacts"] {
		return scanSynthetic(db, sourceID, tables)
	}
	if tables["ZABCDRECORD"] && tables["Z_PRIMARYKEY"] {
		return scanCoreData(db, sourceID, tables)
	}
	return nil, fmt.Errorf("Unsupported Apple Contacts schema: expected a synthetic contacts table or Apple's ZABCDRECORD Core Data tables")
}

func scanSynthetic(db *sql.DB, sourceID string, tables map[string]bool) ([]Contact, error) {
	phones, err := syntheticPoints(db, "phones", tables)
	if err != nil {
		return nil, err
	}
	emails, err := syntheticPoints(db, "emails", tables)
	if err != nil {
		return nil, err
	}
	rows, err := common.Query(db, "SELECT * FROM contacts ORDER BY contact_id")
	if err != nil {
		return nil, err
	}
	var contacts []Contact
	for _, row := range rows {
		contactID := row.String("contact_id")
		rowSource := row.String("source_id")
		if rowSource == "" {
			rowSource = sourceID
		}
		organization := row.String("organization")
		department := row.String("department")
		jobTitle := row.String("job_title")
		nickname := row.String("nickname")
		contactPhones := phones[contactID]
		contactEmails := emails[contactID]
		var nicknames []map[string]any
		if nickname != "" {
			nicknames = []map[string]any{{"value": nickname}}
		}
		contacts = append(contacts, Contact{
			SourceID:      rowSource,
			ContactID:     contactID,
			SourceUID:     contactID,
			DisplayName:   row.String("display_name"),
			GivenName:     row.String("given_name"),
			MiddleName:    row.String("middle_name"),
			FamilyName:    row.String("family_name"),
			Nickname:      nickname,
			Organization:  organization,
			Department:    department,
			JobTitle:      jobTitle,
			PrimaryEmail:  primaryValue(contactEmails),
			PrimaryPhone:  primaryValue(contactPhones),
			Emails:        contactEmails,
			Phones:        contactPhones,
			Addresses:     nil,
			Organizations: organizationValues(organization, department, jobTitle),
			URLs:          nil,
			Nicknames:     nicknames,
			Groups:        nil,
			Dates:         map[string]any{},
			Photos:        nil,
			Note:          row.String("note"),
			CreatedAt:     parseSyntheticDatetime(row.Get("created_at")),
			ModifiedAt:    parseSyntheticDatetime(row.Get("modified_at")),
			Raw:           row.PublicSkippingBlobs(),
		})
	}
	return contacts, nil
}

func syntheticPoints(db *sql.DB, table string, tables map[string]bool) (map[string][]map[string]any, error) {
	if !tables[table] {
		return map[string][]map[string]any{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table+" ORDER BY contact_id, is_primary DESC, rowid")
	if err != nil {
		return nil, err
	}
	values := map[string][]map[string]any{}
	for _, row := range rows {
		values[row.String("contact_id")] = append(values[row.String("contact_id")], map[string]any{
			"value":    row.String("value"),
			"label":    row.String("label"),
			"metadata": map[string]any{"primary": row.Int("is_primary") != 0},
		})
	}
	return values, nil
}

func scanCoreData(db *sql.DB, sourceID string, tables map[string]bool) ([]Contact, error) {
	entityRows, err := common.Query(db, "SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME IN ('ABCDContact', 'ABCDSubscribedContact')")
	if err != nil {
		return nil, err
	}
	var entityIDs []int64
	for _, row := range entityRows {
		entityIDs = append(entityIDs, row.Int("Z_ENT"))
	}
	if len(entityIDs) == 0 {
		return nil, fmt.Errorf("Unsupported Apple Contacts schema: ABCDContact entity is missing")
	}
	sort.Slice(entityIDs, func(i, j int) bool { return entityIDs[i] < entityIDs[j] })

	phones, err := coreDataValues(db, tables, "ZABCDPHONENUMBER", []string{"ZFULLNUMBER"})
	if err != nil {
		return nil, err
	}
	emails, err := coreDataValues(db, tables, "ZABCDEMAILADDRESS", []string{"ZADDRESS"})
	if err != nil {
		return nil, err
	}
	addresses, err := coreDataAddresses(db, tables)
	if err != nil {
		return nil, err
	}
	urls, err := coreDataValues(db, tables, "ZABCDURLADDRESS", []string{"ZURL"})
	if err != nil {
		return nil, err
	}
	relatedNames, err := coreDataValues(db, tables, "ZABCDRELATEDNAME", []string{"ZNAME"})
	if err != nil {
		return nil, err
	}
	socialProfiles, err := coreDataSocialProfiles(db, tables)
	if err != nil {
		return nil, err
	}
	contactDates, err := coreDataDates(db, tables)
	if err != nil {
		return nil, err
	}
	notes, err := coreDataNotes(db, tables)
	if err != nil {
		return nil, err
	}

	placeholders := make([]string, len(entityIDs))
	args := make([]any, len(entityIDs))
	for i, id := range entityIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := common.Query(db, "SELECT * FROM ZABCDRECORD WHERE Z_ENT IN ("+strings.Join(placeholders, ",")+") ORDER BY Z_PK", args...)
	if err != nil {
		return nil, err
	}
	var contacts []Contact
	for _, row := range rows {
		recordPK := row.Int("Z_PK")
		contactID := row.String("ZUNIQUEID")
		if contactID == "" {
			contactID = fmt.Sprint(recordPK)
		}
		givenName := row.String("ZFIRSTNAME")
		middleName := row.String("ZMIDDLENAME")
		familyName := row.String("ZLASTNAME")
		organization := row.String("ZORGANIZATION")
		department := row.String("ZDEPARTMENT")
		jobTitle := row.String("ZJOBTITLE")
		nickname := row.String("ZNICKNAME")
		displayName := row.String("ZNAME")
		if displayName == "" {
			displayName = buildDisplayName(givenName, middleName, familyName, organization)
		}
		contactPhones := phones[recordPK]
		contactEmails := emails[recordPK]
		birthdays := []any{}
		if birthday, ok := common.AppleSeconds(row.Get("ZBIRTHDAY")); ok {
			birthdays = append(birthdays, map[string]any{"date": common.ISOFormat(birthday)})
		}
		dates := map[string]any{
			"birthdays":       birthdays,
			"events":          listOrEmpty(contactDates[recordPK]),
			"related_names":   listOrEmpty(relatedNames[recordPK]),
			"social_profiles": listOrEmpty(socialProfiles[recordPK]),
		}
		var photos []map[string]any
		for _, column := range []string{"ZIMAGEHASH", "ZIMAGEDATA", "ZTHUMBNAILIMAGEDATA"} {
			if truthy(row.Get(column)) {
				photos = []map[string]any{{"has_image": true}}
				break
			}
		}
		var nicknames []map[string]any
		if nickname != "" {
			nicknames = []map[string]any{{"value": nickname}}
		}
		sourceUID := row.String("ZEXTERNALUUID")
		if sourceUID == "" {
			sourceUID = contactID
		}
		createdAt := common.AppleEpoch
		if t, ok := common.AppleSeconds(row.Get("ZCREATIONDATE")); ok {
			createdAt = t
		}
		modifiedAt := common.AppleEpoch
		if t, ok := common.AppleSeconds(row.Get("ZMODIFICATIONDATE")); ok {
			modifiedAt = t
		}
		contacts = append(contacts, Contact{
			SourceID:      sourceID,
			ContactID:     contactID,
			SourceUID:     sourceUID,
			DisplayName:   displayName,
			GivenName:     givenName,
			MiddleName:    middleName,
			FamilyName:    familyName,
			Nickname:      nickname,
			Organization:  organization,
			Department:    department,
			JobTitle:      jobTitle,
			PrimaryEmail:  primaryValue(contactEmails),
			PrimaryPhone:  primaryValue(contactPhones),
			Emails:        contactEmails,
			Phones:        contactPhones,
			Addresses:     addresses[recordPK],
			Organizations: organizationValues(organization, department, jobTitle),
			URLs:          urls[recordPK],
			Nicknames:     nicknames,
			Groups:        nil,
			Dates:         dates,
			Photos:        photos,
			Note:          notes[recordPK],
			CreatedAt:     createdAt,
			ModifiedAt:    modifiedAt,
			Raw:           row.PublicSkippingBlobs(),
		})
	}
	return contacts, nil
}

// truthy mirrors Python truthiness for the scanned scalar types.
func truthy(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return v != ""
	case []byte:
		return len(v) > 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	case bool:
		return v
	default:
		return true
	}
}

func listOrEmpty(items []map[string]any) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func ownerID(row common.Row) int64 {
	if owner := row.Int("ZOWNER"); owner != 0 {
		return owner
	}
	return row.Int("Z22_OWNER")
}

func coreDataValues(db *sql.DB, tables map[string]bool, table string, valueColumns []string) (map[int64][]map[string]any, error) {
	if !tables[table] {
		return map[int64][]map[string]any{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table+" ORDER BY ZORDERINGINDEX, Z_PK")
	if err != nil {
		return nil, err
	}
	values := map[int64][]map[string]any{}
	for _, row := range rows {
		owner := ownerID(row)
		value := ""
		for _, column := range valueColumns {
			if v := row.String(column); v != "" {
				value = v
				break
			}
		}
		if owner == 0 || value == "" {
			continue
		}
		item := map[string]any{
			"value":    value,
			"label":    cleanLabel(row.String("ZLABEL")),
			"metadata": map[string]any{"primary": row.Int("ZISPRIMARY") != 0},
		}
		if table == "ZABCDPHONENUMBER" {
			item["canonicalForm"] = value
			item["countryCode"] = row.String("ZCOUNTRYCODE")
		}
		values[owner] = append(values[owner], item)
	}
	return values, nil
}

func coreDataAddresses(db *sql.DB, tables map[string]bool) (map[int64][]map[string]any, error) {
	const table = "ZABCDPOSTALADDRESS"
	if !tables[table] {
		return map[int64][]map[string]any{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table+" ORDER BY ZORDERINGINDEX, Z_PK")
	if err != nil {
		return nil, err
	}
	values := map[int64][]map[string]any{}
	for _, row := range rows {
		owner := ownerID(row)
		if owner == 0 {
			continue
		}
		region := row.String("ZSTATE")
		if region == "" {
			region = row.String("ZREGION")
		}
		values[owner] = append(values[owner], map[string]any{
			"streetAddress": row.String("ZSTREET"),
			"city":          row.String("ZCITY"),
			"region":        region,
			"postalCode":    row.String("ZZIPCODE"),
			"country":       row.String("ZCOUNTRYNAME"),
			"countryCode":   row.String("ZCOUNTRYCODE"),
			"label":         cleanLabel(row.String("ZLABEL")),
			"metadata":      map[string]any{"primary": row.Int("ZISPRIMARY") != 0},
		})
	}
	return values, nil
}

func coreDataSocialProfiles(db *sql.DB, tables map[string]bool) (map[int64][]map[string]any, error) {
	const table = "ZABCDSOCIALPROFILE"
	if !tables[table] {
		return map[int64][]map[string]any{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table+" ORDER BY ZORDERINGINDEX, Z_PK")
	if err != nil {
		return nil, err
	}
	values := map[int64][]map[string]any{}
	for _, row := range rows {
		owner := ownerID(row)
		if owner == 0 {
			continue
		}
		values[owner] = append(values[owner], map[string]any{
			"service":        row.String("ZSERVICENAME"),
			"username":       row.String("ZUSERNAME"),
			"userIdentifier": row.String("ZUSERIDENTIFIER"),
			"url":            row.String("ZURLSTRING"),
			"label":          cleanLabel(row.String("ZLABEL")),
		})
	}
	return values, nil
}

func coreDataDates(db *sql.DB, tables map[string]bool) (map[int64][]map[string]any, error) {
	const table = "ZABCDCONTACTDATE"
	if !tables[table] {
		return map[int64][]map[string]any{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table+" ORDER BY ZORDERINGINDEX, Z_PK")
	if err != nil {
		return nil, err
	}
	values := map[int64][]map[string]any{}
	for _, row := range rows {
		owner := ownerID(row)
		value, ok := common.AppleSeconds(row.Get("ZDATE"))
		if owner != 0 && ok {
			values[owner] = append(values[owner], map[string]any{
				"date":  common.ISOFormat(value),
				"label": cleanLabel(row.String("ZLABEL")),
			})
		}
	}
	return values, nil
}

func coreDataNotes(db *sql.DB, tables map[string]bool) (map[int64]string, error) {
	const table = "ZABCDNOTE"
	if !tables[table] {
		return map[int64]string{}, nil
	}
	rows, err := common.Query(db, "SELECT * FROM "+table)
	if err != nil {
		return nil, err
	}
	notes := map[int64]string{}
	for _, row := range rows {
		owner := row.Int("ZCONTACT")
		if owner == 0 {
			owner = row.Int("Z22_CONTACT")
		}
		if owner != 0 {
			notes[owner] = row.String("ZTEXT")
		}
	}
	return notes, nil
}

func organizationValues(organization, department, jobTitle string) []map[string]any {
	if organization == "" && department == "" && jobTitle == "" {
		return nil
	}
	return []map[string]any{{
		"name":       organization,
		"department": department,
		"title":      jobTitle,
		"metadata":   map[string]any{"primary": true},
	}}
}

func primaryValue(values []map[string]any) string {
	for _, item := range values {
		if metadata, ok := item["metadata"].(map[string]any); ok {
			if primary, ok := metadata["primary"].(bool); ok && primary {
				return common.PyStr(item["value"])
			}
		}
	}
	if len(values) > 0 {
		return common.PyStr(values[0]["value"])
	}
	return ""
}

func buildDisplayName(given, middle, family, organization string) string {
	var parts []string
	for _, part := range []string{given, middle, family} {
		if part != "" {
			parts = append(parts, part)
		}
	}
	name := strings.TrimSpace(strings.Join(parts, " "))
	if name == "" {
		return organization
	}
	return name
}

func cleanLabel(value string) string {
	const prefix, suffix = "_$!<", ">!$_"
	if strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix) && len(value) >= len(prefix)+len(suffix) {
		return strings.ToLower(value[len(prefix) : len(value)-len(suffix)])
	}
	return value
}

func parseSyntheticDatetime(value any) time.Time {
	switch v := value.(type) {
	case nil:
		return common.AppleEpoch
	case string:
		if v == "" {
			return common.AppleEpoch
		}
		if t, ok := common.TryParseISO(v); ok {
			return t
		}
		return common.AppleEpoch
	default:
		text := common.PyStr(v)
		if t, ok := common.TryParseISO(text); ok {
			return t
		}
		return common.AppleEpoch
	}
}
