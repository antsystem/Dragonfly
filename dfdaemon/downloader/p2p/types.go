package p2p

import "github.com/dragonflyoss/Dragonfly/pkg/rate"

type downloadNodeInfo struct {
	ip 		string
	port    int
	path    string
	peerID  string
}

type Config struct {
	Cid         string   `json:"cid"`
	IP          string   `json:"ip"`
	HostName    string   `json:"hostName"`
	Port        int      `json:"port"`
	Version     string   `json:"version,omitempty"`
	Md5         string   `json:"md5,omitempty"`
	Identifier  string   `json:"identifier,omitempty"`
	CallSystem  string   `json:"callSystem,omitempty"`
	Dfdaemon    bool     `json:"dfdaemon,omitempty"`
	Insecure    bool     `json:"insecure,omitempty"`
	RootCAs     [][]byte `json:"rootCAs,omitempty"`

	MetaDir string

	// seed pattern config
	// high level of water level which to control the weed out seed file
	HighLevel 		int

	// low level of water level which to control the weed out seed file
	LowLevel 		int

	// DefaultBlockOrder represents the default block order of seed file. it should be in range [10-31].
	DefaultBlockOrder int

	//
	PerDownloadBlocks int

	DownRate 		 rate.Rate
	UploadRate       rate.Rate

	TotalLimit       int
	ConcurrentLimit  int
}
