package seed

type PreFetchInfo struct {
	URL 	string
	Header  map[string][]string
	Length  int64

	// if FilePath is valid, means the seed file already exists.
	FilePath    string
}

type PreFetchResult struct {
	Success		bool

	Err			error
	// if canceled, caller need not to do other
	Canceled    bool
}