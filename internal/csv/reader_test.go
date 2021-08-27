package csv

import (
	"context"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	in := `id,type,actor_id,repo_id
11185376329,PushEvent,8422699,224252202
11185376333,CreateEvent,53201765,231161852
11185376335,PushEvent,2631623,155254893
11185376336,PushEvent,52553915,231065965
11185376338,PushEvent,31390726,225080339
`
	r := strings.NewReader(in)

	reader, err := NewReader(r)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	chOut, chErr := reader.ReadConcurrently(ctx, 4)
	records := make(map[string][]string)

OUT:
	for {
		select {
		case rec, ok := <-chOut:
			if !ok {
				break OUT
			}
			records[rec[0]] = rec

		case err := <-chErr:
			if err != nil {
				t.Error(err)
			}
		}
	}
	if _, ok := records["11185376336"]; !ok {
		t.Error("failed to read the record")
	}
}
