package seed

type PreFetchInfo struct {
	TaskID  string
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

type DownloadStatus struct {
	Finished    bool
	Canceled	bool
	Start		int64
	Length      int64
}

type prefetchSt struct {
	sd   *seed
	ch    chan PreFetchResult
}
