package account

import "context"

// Upsert is a create-or-replace account command.
type Upsert struct {
	Token string
	Pool  string
	Tags  []string
	Ext   map[string]any
}

// Patch mutates an existing account (only set fields are applied).
type Patch struct {
	Token            string
	Pool             *string
	Status           *Status
	Tags             []string
	AddTags          []string
	RemoveTags       []string
	QuotaAuto        *map[string]any
	QuotaFast        *map[string]any
	QuotaExpert      *map[string]any
	QuotaHeavy        *map[string]any
	QuotaGrok43      *map[string]any
	QuotaConsole     *map[string]any
	UsageUseDelta    *int
	UsageFailDelta   *int
	UsageSyncDelta   *int
	LastUseAt        *int64
	LastFailAt       *int64
	LastFailReason   *string
	LastSyncAt       *int64
	LastClearAt      *int64
	StateReason      *string
	ExtMerge         map[string]any
	ClearFailures    bool
}

// ListQuery filters and paginates the account list.
type ListQuery struct {
	Page           int
	PageSize       int
	Pool           string
	Status         *Status
	Tags           []string
	IncludeDeleted bool
	SortBy         string
	SortDesc       bool
}

// Page is a paginated slice of records.
type Page struct {
	Items      []*Record
	Total      int
	Page       int
	PageSize   int
	TotalPages int
	Revision   int
}

// MutationResult summarizes a repository mutation.
type MutationResult struct {
	Upserted int
	Patched  int
	Deleted  int
	Revision int
}

// ChangeSet is an incremental scan result.
type ChangeSet struct {
	Revision     int
	BatchMaxRev  int
	Items        []*Record
	DeletedTokens []string
	HasMore      bool
}

// Snapshot is the full runtime view.
type Snapshot struct {
	Revision int
	Items    []*Record
}

// Repository is the storage contract for accounts.
type Repository interface {
	Initialize(ctx context.Context) error
	GetRevision(ctx context.Context) (int, error)
	RuntimeSnapshot(ctx context.Context) (*Snapshot, error)
	ScanChanges(ctx context.Context, since int, limit int) (*ChangeSet, error)
	UpsertAccounts(ctx context.Context, items []Upsert) (*MutationResult, error)
	PatchAccounts(ctx context.Context, patches []Patch) (*MutationResult, error)
	DeleteAccounts(ctx context.Context, tokens []string) (*MutationResult, error)
	GetAccounts(ctx context.Context, tokens []string) ([]*Record, error)
	ListAccounts(ctx context.Context, query ListQuery) (*Page, error)
	ReplacePool(ctx context.Context, pool string, upserts []Upsert) (*MutationResult, error)
	ResetExpiredConsoleWindows(ctx context.Context) (int, error)
	RecoverConsoleExpiredAccounts(ctx context.Context) (int, error)
	Close(ctx context.Context) error
}
