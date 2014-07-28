package rivers

import (
	"fmt"
)

type statsLog struct {
	name  string
	value int64
}

func PushStatsKey(qname string) string {
	return fmt.Sprintf("%s:push", qname)
}

func PopStatsKey(qname string) string {
	return fmt.Sprintf("%s:pop", qname)
}

func AckStatsKey(qname string) string {
	return fmt.Sprintf("%s:ack", qname)
}

func SecStatsKey(sname string, sec int64) string {
	return fmt.Sprintf("%s:%d", sname, sec)
}

func SecQueueSizeKey(qname string, sec int64) string {
	return fmt.Sprintf("%s:size:%d", qname, sec)
}

func AllQueueKeys(qname string) string {
	return fmt.Sprintf("%s:*", qname)
}
