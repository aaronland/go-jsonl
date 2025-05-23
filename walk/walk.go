// package walk provides methods for walking all in the records in a line-delimited JSON document.
package walk

import (
	"context"
	"fmt"
	"io"

	"github.com/aaronland/go-json-query"
)

const CONTEXT_PATH string = "github.com/aaronland/go-jsonl#path"

type WalkFilterFunc func(context.Context, string) bool

type IterateOptions struct {
	ValidateJSON bool
	FormatJSON   bool
	QuerySet     *query.QuerySet
	IsBzip       bool
	Filter       WalkFilterFunc
}

type WalkRecord struct {
	Path             string
	LineNumber       int
	Body             []byte
	CompletedChannel chan bool
}

type WalkError struct {
	Path       string
	LineNumber int
	Err        error
}

func (e *WalkError) Error() string {
	return e.String()
}

func (e *WalkError) String() string {
	return fmt.Sprintf("[%s] line %d, %v", e.Path, e.LineNumber, e.Err)
}

func IsEOFError(err error) bool {

	switch err.(type) {
	case *WalkError:

		if err.(*WalkError).Err == io.EOF {
			return true
		}

		return false
	default:
		return false
	}
}
