package seed

import (
	"crypto/md5"
	"fmt"
)

func GenerateKeyByUrl(url string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(url)))
}
