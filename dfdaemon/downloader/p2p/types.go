package p2p

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

	metaDir    string
}
