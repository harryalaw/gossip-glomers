package snowflake

import (
	"fmt"
	"sync"
	"testing"
)

func TestSnowflake_NextId(t *testing.T) {
	t.Run("Generates positive Ids", func(t *testing.T) {
		w, _ := NewWorker(1)

		id, err := w.NextId()

		if err != nil {
			t.Fatalf("Error getting next Id: %v", err)
		}

		if id < 0 {
			t.Fatalf("Id expected to be positive, was %d", id)
		}

	})

	t.Run("Generate Ids", func(t *testing.T) {
		w, _ := NewWorker(1)

		length := 100
		ids := make([]uint64, 0)
		idSet := make(map[uint64]struct{})

		for i := 0; i < length; i++ {
			nextId, err := w.NextId()

			if err != nil {
				t.Fatalf("Error getting next Id: %v", err)
			}

			ids = append(ids, nextId)
			idSet[nextId] = struct{}{}
		}

		if len(idSet) != length {
			t.Fatalf("Did not generate unique Ids. Expected %d, got %d", length, len(idSet))
		}
	})

	t.Run("Generates unique Ids for two nodes", func(t *testing.T) {
		w1, _ := NewWorker(1)
		w2, _ := NewWorker(2)

		length := 100
		w1Ids := make([]uint64, length)
		w2Ids := make([]uint64, length)
		idSet := make(map[uint64]struct{})

		for i := 0; i < length; i++ {
			nextId1, err := w1.NextId()
			if err != nil {
				t.Fatalf("Error getting next Id: %v", err)
			}

			w1Ids[i] = nextId1

			nextId2, err := w2.NextId()
			if err != nil {
				t.Fatalf("Error getting next Id: %v", err)
			}

			w2Ids[i] = nextId2

			idSet[nextId1] = struct{}{}
			idSet[nextId2] = struct{}{}
		}

		if len(idSet) != 2*length {
			t.Fatalf("Did not generate unique Ids. Expected %d, got %d", 2*length, len(idSet))
		}
	})

	t.Run("More concurrent Id generation?", func(t *testing.T) {
		w1, _ := NewWorker(1)
		w2, _ := NewWorker(2)
		w3, _ := NewWorker(3)

		length := 100
		w1Ids := make([]uint64, length)
		w2Ids := make([]uint64, length)
		w3Ids := make([]uint64, length)

		var wg sync.WaitGroup
		incrementer := func(gen *Worker, idList *[]uint64, id int) {
			defer wg.Done()
			for i := 0; i < length; i++ {
				nextId, err := gen.NextId()

				if err != nil {
					fmt.Printf("Error getting next Id: %v\n", err)
					continue
				}
				(*idList)[i] = nextId
			}
		}

		wg.Add(3)
		go incrementer(w1, &w1Ids, 1)
		go incrementer(w2, &w2Ids, 2)
		go incrementer(w3, &w3Ids, 3)
		wg.Wait()

		if length != len(w1Ids) {
			t.Fatalf("Didn't generate enough Ids for list 1, expected %d, got %d", length, len(w1Ids))
		}
		if length != len(w2Ids) {
			t.Fatalf("Didn't generate enough Ids for list 2, expected %d, got %d", length, len(w2Ids))
		}
		if length != len(w3Ids) {
			t.Fatalf("Didn't generate enough Ids for list 3, expected %d, got %d", length, len(w3Ids))
		}

		idSet := make(map[uint64]int)

		for i := 0; i < length; i++ {
			prev, _ := idSet[w1Ids[i]]
			idSet[w1Ids[i]] = prev + 1

			prev, _ = idSet[w2Ids[i]]
			idSet[w2Ids[i]] = prev + 100

			prev, _ = idSet[w3Ids[i]]
			idSet[w3Ids[i]] = prev + 100_000
		}

		if len(idSet) != 3*length {
			for key, value := range idSet {
				if value != 1 && value != 100 && value != 100_000 {
					fmt.Printf("Duplicated id: %d, %d\n", key, value)
				}
			}
			t.Fatalf("Generated duplicate IDs. Expected %d Ids, got %d", 3*length, len(idSet))
		}
	})

	t.Run("One generator, lots of ids", func(t *testing.T) {
		w, _ := NewWorker(5)

		var wg sync.WaitGroup
		count := 10_000
		ch := make(chan uint64, count)
		wg.Add(count)
		defer close(ch)

		for i := 0; i < count; i++ {
			go func() {
				defer wg.Done()
				id, _ := w.NextId()
				ch <- id
			}()
		}
		wg.Wait()

		ids := make(map[uint64]int)

		for i := 0; i < count; i++ {
			id := <-ch
			prev, _ := ids[id]

			ids[id] = prev + 1
		}

		if len(ids) != count {
			for key, val := range ids {
				if val > 1 {
					fmt.Printf("Duplicate key: %d\n", key)
				}

			}
			t.Fatalf("Repeated IDs, expected: %d, got: %d", count, len(ids))
		}

	})
}
