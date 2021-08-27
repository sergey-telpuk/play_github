package csv

import (
	"context"
	"encoding/csv"
	"io"
	"sync"
)

type Reader struct {
	reader *csv.Reader
	mtx    *sync.Mutex
}

func NewReader(r io.Reader) (Reader, error) {
	reader := csv.NewReader(r)
	_, err := reader.Read() //skip header
	if err != nil {
		return Reader{}, err
	}
	return Reader{reader: reader, mtx: &sync.Mutex{}}, nil
}

func (r Reader) ReadConcurrently(ctx context.Context, pool int) (<-chan []string, <-chan error) {
	chOut := make(chan []string, pool)
	chErr := make(chan error)
	wg := &sync.WaitGroup{}

	for i := 0; i < pool; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				r.mtx.Lock()
				record, err := r.reader.Read()
				r.mtx.Unlock()

				// Stop at EOF.
				if err == io.EOF {
					return
				}

				if err != nil {
					chErr <- err
					return
				}
				select {
				case chOut <- record:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(chOut)
		close(chErr)
	}()

	return chOut, chErr
}
