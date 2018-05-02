package table

import (
	loglib "log"
	"os"
)

var log *loglib.Logger

func init() {
	log = loglib.New(os.Stderr, "[routing-table] ", loglib.Lshortfile|loglib.Ltime)
}
