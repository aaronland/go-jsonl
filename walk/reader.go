package walk

import (
	"bufio"
	"compress/bzip2"
	"context"
	"encoding/json"
	"io"
	"iter"

	"github.com/aaronland/go-json-query"
	"github.com/tidwall/pretty"
)

// DEPRECATED - Use IterateReader instead

func WalkReader(ctx context.Context, opts *WalkOptions, fh io.Reader) {

	record_ch := opts.RecordChannel
	error_ch := opts.ErrorChannel
	done_ch := opts.DoneChannel

	reader := bufio.NewReader(fh)

	if opts.IsBzip {
		br := bufio.NewReader(fh)
		cr := bzip2.NewReader(br)
		reader = bufio.NewReader(cr)
	}

	path := ""
	lineno := 0

	v := ctx.Value(CONTEXT_PATH)

	if v != nil {
		path = v.(string)
	}

	for {

		select {
		case <-ctx.Done():
			break
		default:
			// pass
		}

		lineno += 1

		body, err := reader.ReadBytes('\n')

		if err != nil {

			if err == io.EOF {
				break
			}

			if err == io.ErrUnexpectedEOF {
				break
			}

			e := &WalkError{
				Path:       path,
				LineNumber: lineno,
				Err:        err,
			}

			error_ch <- e
			continue
		}

		if opts.ValidateJSON {

			var stub interface{}
			err = json.Unmarshal(body, &stub)

			if err != nil {

				e := &WalkError{
					Path:       path,
					LineNumber: lineno,
					Err:        err,
				}

				error_ch <- e
				continue
			}

			body, err = json.Marshal(stub)

			if err != nil {

				e := &WalkError{
					Path:       path,
					LineNumber: lineno,
					Err:        err,
				}

				error_ch <- e
				continue
			}
		}

		if opts.QuerySet != nil {

			matches, err := query.Matches(ctx, opts.QuerySet, body)

			if err != nil {

				e := &WalkError{
					Path:       path,
					LineNumber: lineno,
					Err:        err,
				}

				error_ch <- e
				continue
			}

			if !matches {
				continue
			}
		}

		if opts.FormatJSON {
			body = pretty.Pretty(body)
		}

		rec := &WalkRecord{
			Path:       path,
			LineNumber: lineno,
			Body:       body,
		}

		var completed_ch chan bool

		if opts.SendCompletedChannel {
			completed_ch = make(chan bool)
			rec.CompletedChannel = completed_ch
		}

		record_ch <- rec

		if opts.SendCompletedChannel {
			<-completed_ch
		}
	}

	done_ch <- true
}

func IterateReader(ctx context.Context, opts *IterateOptions, r io.Reader) iter.Seq2[*WalkRecord, error] {

	return func(yield func(*WalkRecord, error) bool) {

		reader := bufio.NewReader(r)

		if opts.IsBzip {
			br := bufio.NewReader(r)
			cr := bzip2.NewReader(br)
			reader = bufio.NewReader(cr)
		}

		path := ""
		lineno := 0

		v := ctx.Value(CONTEXT_PATH)

		if v != nil {
			path = v.(string)
		}

		for {

			select {
			case <-ctx.Done():
				break
			default:
				// pass
			}

			lineno += 1

			body, err := reader.ReadBytes('\n')

			if err != nil {

				if err == io.EOF {
					break
				}

				if err == io.ErrUnexpectedEOF {
					break
				}

				e := &WalkError{
					Path:       path,
					LineNumber: lineno,
					Err:        err,
				}

				if !yield(nil, e) {
					return
				}
			}

			if opts.ValidateJSON {

				var stub interface{}
				err = json.Unmarshal(body, &stub)

				if err != nil {

					e := &WalkError{
						Path:       path,
						LineNumber: lineno,
						Err:        err,
					}

					if !yield(nil, e) {
						return
					}
				}

				body, err = json.Marshal(stub)

				if err != nil {

					e := &WalkError{
						Path:       path,
						LineNumber: lineno,
						Err:        err,
					}

					if !yield(nil, e) {
						return
					}
				}
			}

			if opts.QuerySet != nil {

				matches, err := query.Matches(ctx, opts.QuerySet, body)

				if err != nil {

					e := &WalkError{
						Path:       path,
						LineNumber: lineno,
						Err:        err,
					}

					if !yield(nil, e) {
						return
					}
				}

				if !matches {
					continue
				}
			}

			if opts.FormatJSON {
				body = pretty.Pretty(body)
			}

			rec := &WalkRecord{
				Path:       path,
				LineNumber: lineno,
				Body:       body,
			}

			yield(rec, nil)
		}

	}
}
