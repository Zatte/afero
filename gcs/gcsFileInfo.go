package gcs

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

type fileInfo struct {
	objAtt *storage.ObjectAttrs
	fs     *GcsFs
}

func (fi *fileInfo) name() string {
	if fi.objAtt.Name != "" {
		return fi.objAtt.Name
	}
	//In case of GCS virtual folders; they will only have a prefix
	return fi.objAtt.Prefix
}

func (fi *fileInfo) Name() string {
	return filepath.Base(fi.name())
}

func (fi *fileInfo) Size() int64 {
	return fi.objAtt.Size
}
func (fi *fileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return 0755
	}
	return 0664
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.objAtt.Updated
}

func (fi *fileInfo) IsDir() bool {
	return fi.objAtt.Metadata["virtual_folder"] == "y" || strings.HasSuffix(fi.Name(), fi.fs.separator)
}

func (fi *fileInfo) Sys() interface{} {
	return nil
}

type ByName []*fileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i].objAtt, a[j].objAtt = a[j].objAtt, a[i].objAtt }
func (a ByName) Less(i, j int) bool { return strings.Compare(a[i].Name(), a[j].Name()) == -1 }
