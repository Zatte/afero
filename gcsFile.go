// Copyright © 2016 Mikael Rapp <micke.rapp@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afero

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GcsFs is a Fs implementation that uses functions provided by google cloud storage
type GcsBaseFile struct {
	ctx    context.Context
	fs     *GcsFs
	bucket *storage.BucketHandle

	obj  *storage.ObjectHandle
	name string

	actualOffset int64
	reader       io.ReadCloser
	writer       io.WriteCloser

	maybeWriterPadReader      io.Reader
	maybeWriterPadSize        int64
	maybeWriterPadStartOffset int64

	openFlags int
	fileMode  os.FileMode
	offset    int64
	closed    bool
}

type GcsFile struct {
	openFlags int
	fileMode  os.FileMode
	offset    int64
	closed    bool
	ReadDirIt *storage.ObjectIterator
	resource  *GcsBaseFile
}

func NewGcsFile(
	ctx context.Context,
	fs *GcsFs,
	obj *storage.ObjectHandle,
	openFlags int,
	fileMode os.FileMode,
	name string,
) *GcsFile {
	return &GcsFile{
		openFlags: openFlags,
		fileMode:  fileMode,
		offset:    0,
		closed:    false,
		ReadDirIt: nil,
		resource: &GcsBaseFile{
			ctx:    ctx,
			fs:     fs,
			bucket: fs.bucket,

			obj:  obj,
			name: name,

			actualOffset: 0,
			reader:       nil,
			writer:       nil,

			maybeWriterPadReader:      nil,
			maybeWriterPadSize:        0,
			maybeWriterPadStartOffset: 0,
		},
	}
}

func NewGcsFileFromOldFH(
	openFlags int,
	fileMode os.FileMode,
	oldFile *GcsBaseFile,
) *GcsFile {
	return &GcsFile{
		openFlags: openFlags,
		fileMode:  fileMode,
		offset:    0,
		closed:    false,
		ReadDirIt: nil,

		resource: oldFile,
	}
}

func (o *GcsFile) Close() error {
	// Threre shouldn't be a case where both are open at the same time
	// but the check is omitted at this time.
	o.closed = true
	return o.maybeCloseIo()
}

func (o *GcsFile) maybeCloseIo() error {
	// Threre shouldn't be a case where both are open at the same time
	// but the check is omitted at this time.
	o.maybeCloseReader()
	return o.maybeCloseWriter()
}

func (o *GcsFile) maybeCloseReader() {
	if o.resource.reader != nil {
		o.resource.reader.Close()
		o.resource.reader = nil
	}
}

func (o *GcsFile) maybeCloseWriter() error {

	// There might be a need to write some more data before closing. This is due to how WriteAt forces us to keep
	// the tail of a file if we write to the middle of it.
	if o.resource.maybeWriterPadReader != nil {
		bytesToSkipFromSrc := o.offset - o.resource.maybeWriterPadStartOffset
		_, err := io.CopyN(ioutil.Discard, o.resource.maybeWriterPadReader, bytesToSkipFromSrc)
		if err == nil {
			if _, err2 := io.Copy(o.resource.writer, o.resource.maybeWriterPadReader); err2 != nil {
				panic(err2)
			}
		}
		o.resource.maybeWriterPadReader = nil
		o.resource.maybeWriterPadStartOffset = 0
	}

	if o.resource.writer != nil {
		o.resource.writer.Close()
		o.resource.writer = nil
	}
	return nil
}

func (o *GcsFile) Seek(offset int64, whence int) (int64, error) {
	if o.closed {
		return 0, ErrFileClosed
	}

	//Since this is an expensive operation; let's make sure we need it
	if (whence == 0 && offset == o.offset) || (whence == 1 && offset == 0) {
		return o.offset, nil
	}
	fmt.Printf("offset before seek is at %d\n", o.offset)

	//Fore the reader/writers to be reopened (at correct offset)
	o.maybeCloseIo()
	stat, err := o.Stat()
	if err != nil {
		return 0, nil
	}

	switch whence {
	case 0:
		o.offset = offset
	case 1:
		o.offset += offset
	case 2:
		o.offset = stat.Size() + offset
	}
	fmt.Printf("offset after seek is at %d\n", o.offset)
	return o.offset, nil
}

func (o *GcsFile) Read(p []byte) (n int, err error) {

	//GCS is strongly consistant; just make sure the object is closed before we try to read it again.
	o.maybeCloseWriter()

	if len(p) <= 0 {
		return 0, nil
	}

	// Assume that if the reader is open; it is at the correct offset
	// a good performance assumption that we must ensure holds
	if o.resource.reader != nil {
		n, err = o.resource.reader.Read(p)
		o.offset += int64(n)
		return n, err
	}
	if o.offset != 0 {
		return o.ReadAt(p, o.offset)
	}
	if r, err := o.resource.obj.NewReader(o.resource.ctx); err != nil {
		return 0, err
	} else {
		o.resource.reader = r
		return o.resource.reader.Read(p)
	}

}

func (o *GcsFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off == o.resource.offset && o.resource.reader != nil {
		return o.Read(p)
	}
	o.maybeCloseIo()
	stat, err := o.Stat()
	if err != nil {
		return 0, err
	}

	r, err := o.resource.obj.NewRangeReader(o.resource.ctx, off, stat.Size())
	if err != nil {
		return 0, err
	}
	o.resource.reader = r
	o.resource.offset = off
	return o.Read(p)
}

func (o *GcsFile) Write(p []byte) (n int, err error) {

	if o.openFlags == os.O_RDONLY {
		return 0, fmt.Errorf("File is opend as read only")
	}

	o.maybeCloseReader() //will force Read to reopen the file with the new content

	// Assume that if the writer is open; it is at the correct offset
	// a good performance assumption that we must ensure holds
	if o.resource.writer != nil {
		n, err = o.resource.writer.Write(p)
		o.offset += int64(n)
		return n, err
	}

	if o.offset != 0 {
		return o.WriteAt(p, o.offset)
	}

	w := o.resource.obj.NewWriter(o.resource.ctx)
	o.resource.writer = w
	written, err := o.resource.writer.Write(p)
	if err == nil {
		o.offset += int64(written)
	}
	return written, err
}

func (o *GcsFile) WriteAt(b []byte, off int64) (n int, err error) {

	if o.openFlags == os.O_RDONLY {
		return 0, fmt.Errorf("File is opend as read only")
	}

	if off == o.offset && o.resource.writer != nil {
		return o.Write(b)
	}
	o.maybeCloseIo() //Ensure readers must be re-opened and that any writers are flushed/synced

	// TRIGGER WARNING: This can seem like a hack but it works thanks
	// to GCS strong consistency. We will open and write to the same file; First when the
	// writer is closed will the content get commented to GCS. This allows us to do this funkyness
	// Objectv1[:offset] -> Objectv2
	// newData1 -> Objectv2
	// .. multiple consecuctive writes without seek ..
	// Objectv1[offset+len(new data):] -> Objectv2
	// Objectv2.Close
	// It will however require a download and upload of the original file but it can't be avoided
	stat, err := o.Stat()
	if err != nil {
		return 0, nil
	}
	orignalSize := stat.Size()
	if off > orignalSize {
		return 0, ErrOutOfRange
	}

	r, err := o.resource.obj.NewReader(o.resource.ctx)
	if err != nil {
		return 0, err
	}
	w := o.resource.obj.NewWriter(o.resource.ctx)
	fmt.Printf("Copying file to offset %d\n", off)
	if _, err := io.CopyN(w, r, off); err != nil {
		return 0, err
	}

	o.resource.writer = w
	o.resource.offset = off

	o.resource.maybeWriterPadReader = r
	o.resource.maybeWriterPadStartOffset = off

	return o.Write(b)
}

func (o *GcsFile) Name() string {
	return o.resource.name
}

func (o *GcsFile) readdir(count int) ([]*fileInfo, error) {
	//fmt.Println("readdir called with count : %d", count)
	o.Sync()
	path := o.resource.fs.ensureTrailingSeparator(normSeparators(o.Name(), o.resource.fs.separator))
	if o.ReadDirIt == nil {
		//fmt.Printf("Querying path : %s\n", path)
		o.ReadDirIt = o.resource.bucket.Objects(
			o.resource.ctx, &storage.Query{o.resource.fs.separator, path, false})
	}
	var res []*fileInfo
	for {
		//fmt.Println("Start of the loop")
		object, err := o.ReadDirIt.Next()
		//fmt.Println(object, err)
		if err == iterator.Done {
			if len(res) > 0 || count == 0 {
				return res, nil
			}
			return res, io.EOF
		}
		if err != nil {
			return res, err
		}

		// GCS also returns the synthetic parent folder when querying like this
		// (since it matches the prefix condition)
		tmp := fileInfo{object, o.resource.fs}

		// Since we create "virtual folders are empty objects these can sometimes be returned twice
		// when we do a query (As the query will also return GCS version of "virtual folders")
		if object.Name == "" {
			continue
		}
		//We can also sometime get the queries folder back...
		if o.resource.fs.ensureTrailingSeparator(tmp.name()) == o.resource.fs.ensureTrailingSeparator(path) {
			continue
		}

		res = append(res, &tmp)
		//fmt.Printf("%d && lend(res)==%d; %v \n", count, len(res), count > 0 && len(res) >= count)
		if count > 0 && len(res) >= count {
			//fmt.Println("Ending the loop")
			break
		}
	}
	return res, nil
}

func (o *GcsFile) Readdir(count int) ([]os.FileInfo, error) {
	fi, err := o.readdir(count)
	if len(fi) > 0 {
		sort.Sort(ByName(fi))
	}

	var res []os.FileInfo
	for _, f := range fi {
		res = append(res, f)
	}
	return res, err
}

func (o *GcsFile) Readdirnames(n int) ([]string, error) {
	fi, err := o.Readdir(n)
	if err != nil && err != io.EOF {
		panic(err)
	}
	names := make([]string, len(fi))

	for i, f := range fi {
		names[i] = filepath.Base(f.Name())
	}
	return names, err
}

func (o *GcsFile) Stat() (os.FileInfo, error) {
	o.Sync()
	objAttrs, err := o.resource.obj.Attrs(o.resource.ctx)
	if err != nil {
		if err.Error() == "storage: object doesn't exist" {
			return nil, os.ErrNotExist //works with os.IsNotExist check
		}
		return nil, err
	}
	return &fileInfo{objAttrs, o.resource.fs}, nil
}
func (o *GcsFile) Sync() error {
	return o.maybeCloseIo()
}

func (o *GcsFile) Truncate(wantedSize int64) error {
	if o.openFlags == os.O_RDONLY {
		return fmt.Errorf("File is opend as read only")
	}

	if o.closed == true {
		return ErrFileClosed
	}
	/*if  o.openFlags== os.O_RDONLY {
		return &os.PathError{Op: "truncate", Path: o.Name(), Err: errors.New("file handle is read only")}
	}*/
	if wantedSize < 0 {
		return ErrOutOfRange
	}

	o.maybeCloseIo()

	//This is bonkers but works. Read and write from the same object
	r, err := o.resource.obj.NewRangeReader(o.resource.ctx, 0, wantedSize)
	if err != nil {
		return err
	}
	w := o.resource.obj.NewWriter(o.resource.ctx)
	written, err := io.Copy(w, r)
	if err != nil {
		return err
	}

	//Maybe extend the filesize
	for written < wantedSize {
		if w, err := w.Write([]byte(" ")); err != nil {
			break //maybe panic
		} else {
			written += int64(w)
		}
	}
	r.Close() //this error doesn't matter as much
	return w.Close()
}

func (o *GcsFile) WriteString(s string) (ret int, err error) {
	return o.Write([]byte(s))
}

type fileInfo struct {
	objAtt *storage.ObjectAttrs
	fs     *GcsFs
}

func (fi *fileInfo) name() string {
	if fi.objAtt.Name != "" {
		return fi.objAtt.Name
	}
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
	return fi.objAtt.Metadata["virtual_folder"] == "y"
}

func (fi *fileInfo) Sys() interface{} {
	return nil
}

type ByName []*fileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i].objAtt, a[j].objAtt = a[j].objAtt, a[i].objAtt }
func (a ByName) Less(i, j int) bool { return strings.Compare(a[i].Name(), a[j].Name()) == -1 }
