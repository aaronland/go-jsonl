package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aaronland/go-jsonl/walk"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
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

		r, err := bucket.NewReader(ctx, uri, nil)

		if err != nil {
			log.Fatalf("Failed to open %s, %v", uri, err)
		}

		defer r.Close()

		iter_opts := &walk.IterateOptions{}

		for rec, err := range walk.IterateReader(ctx, iter_opts, r) {

			if err != nil {
				log.Fatalf("Failed to walk %s, %v", uri, err)
			}

			_, err := mw.Write(rec.Body)

			if err != nil {
				log.Fatalf("Failed to write body at line %d, %v", rec.LineNumber, err)
			}
		}
	}

}
