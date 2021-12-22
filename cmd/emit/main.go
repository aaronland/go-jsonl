package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aaronland/go-jsonl/walk"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	"io"
	"log"
	"os"
	"strings"
)

func main() {

	bucket_uri := flag.String("bucket-uri", "file:///", "A valid GoCloud blob URI.")

	flag.Parse()

	uris := flag.Args()
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, *bucket_uri)

	if err != nil {
		log.Fatalf("Failed to open bucket, %v", err)
	}

	defer bucket.Close()

	writers := []io.Writer{
		os.Stdout,
	}

	mw := io.MultiWriter(writers...)

	for _, uri := range uris {

		uri = strings.TrimLeft(uri, "/")

		fh, err := bucket.NewReader(ctx, uri, nil)

		if err != nil {
			log.Fatalf("Failed to open %s, %v", uri, err)
		}

		defer fh.Close()

		err = walkReader(ctx, fh, mw)

		if err != nil {
			log.Fatalf("Failed to walk %s, %v", uri, err)
		}
	}

}

func walkReader(ctx context.Context, r io.Reader, wr io.Writer) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var walk_err error

	record_ch := make(chan *walk.WalkRecord)
	error_ch := make(chan *walk.WalkError)
	done_ch := make(chan bool)

	go func() {

		for {
			select {
			case <-ctx.Done():
				done_ch <- true
				return
			case err := <-error_ch:
				walk_err = err
				done_ch <- true
			case r := <-record_ch:

				_, err := wr.Write(r.Body)

				if err != nil {
					error_ch <- &walk.WalkError{
						Path:       r.Path,
						LineNumber: r.LineNumber,
						Err:        fmt.Errorf("Failed to index feature, %w", err),
					}
				}
			}
		}
	}()

	walk_opts := &walk.WalkOptions{
		RecordChannel: record_ch,
		ErrorChannel:  error_ch,
		Workers:       10,
		FormatJSON:    true,
	}

	walk.WalkReader(ctx, walk_opts, r)

	<-done_ch

	if walk_err != nil && !walk.IsEOFError(walk_err) {
		return fmt.Errorf("Failed to walk document, %v", walk_err)
	}

	return nil
}
