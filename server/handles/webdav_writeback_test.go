package handles

import (
	"strings"
	"sync"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm/schema"
)

func TestWebDAVWritebackMonitorUsesGormETagColumnName(t *testing.T) {
	parsed, err := schema.Parse(&model.WebDAVWritebackObject{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatalf("parse writeback schema: %v", err)
	}
	field := parsed.LookUpField("ETag")
	if field == nil {
		t.Fatal("ETag field missing from GORM schema")
	}
	if field.DBName != "e_tag" {
		t.Fatalf("unexpected ETag DB column: got %q want %q", field.DBName, "e_tag")
	}
	if !strings.Contains(webDAVWritebackMonitorColumns, field.DBName) {
		t.Fatalf("monitor SELECT %q does not contain GORM ETag column %q", webDAVWritebackMonitorColumns, field.DBName)
	}
	if strings.Contains(webDAVWritebackMonitorColumns, " etag") {
		t.Fatalf("monitor SELECT must not use non-existent raw column etag: %q", webDAVWritebackMonitorColumns)
	}
}
