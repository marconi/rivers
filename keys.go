package rivers

import (
	"fmt"
)

func AllQueueKeys(qname string) string {
	return fmt.Sprintf("%s:*", qname)
}
