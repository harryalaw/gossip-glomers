package snowflake

import (
	"fmt"
	"sync"
	"testing"
)

func TestSnowflake_NextId(t *testing.T) {
	t.Run("Generates positive Ids", func(t *testing.T) {
		gen1, _ := NewGenerator(1)

		id := gen1.NextId()

		if id < 0 {
			t.Fatalf("Id expected to be positive, was %d", id)
		}

	})

	t.Run("Generate Ids", func(t *testing.T) {
		gen1, _ := NewGenerator(1)

		length := 100
		gen1Ids := make([]int64, 0)
		idSet := make(map[int64]struct{})

		for i := 0; i < length; i++ {
			nextId := gen1.NextId()

			gen1Ids = append(gen1Ids, nextId)
			idSet[nextId] = struct{}{}
		}

		if len(idSet) != length {
			t.Fatalf("Did not generate unique Ids. Expected %d, got %d", length, len(idSet))
		}
	})

	t.Run("Generates unique Ids for two nodes", func(t *testing.T) {
		gen1, _ := NewGenerator(1)
		gen2, _ := NewGenerator(2)

		length := 100
		gen1Ids := make([]int64, length)
		gen2Ids := make([]int64, length)
		idSet := make(map[int64]struct{})

		for i := 0; i < length; i++ {
			nextId1 := gen1.NextId()
			gen1Ids[i] = nextId1

			nextId2 := gen2.NextId()
			gen2Ids[i] = nextId2

			idSet[nextId1] = struct{}{}
			idSet[nextId2] = struct{}{}
		}

		if len(idSet) != 2*length {
			t.Fatalf("Did not generate unique Ids. Expected %d, got %d", 2*length, len(idSet))
		}
	})

	t.Run("More concurrent Id generation?", func(t *testing.T) {
		gen1, _ := NewGenerator(1)
		gen2, _ := NewGenerator(2)
		gen3, _ := NewGenerator(3)

		length := 100
		gen1Ids := make([]int64, length)
		gen2Ids := make([]int64, length)
		gen3Ids := make([]int64, length)

		var wg sync.WaitGroup
		incrementer := func(gen *Generator, idList *[]int64, id int) {
			defer wg.Done()
			for i := 0; i < length; i++ {
				nextId := gen.NextId()
				(*idList)[i] = nextId
			}
		}

		wg.Add(3)
		go incrementer(gen1, &gen1Ids, 1)
		go incrementer(gen2, &gen2Ids, 2)
		go incrementer(gen3, &gen3Ids, 3)
		wg.Wait()

		if length != len(gen1Ids) {
			t.Fatalf("Didn't generate enough Ids for list 1, expected %d, got %d", length, len(gen1Ids))
		}
		if length != len(gen2Ids) {
			t.Fatalf("Didn't generate enough Ids for list 2, expected %d, got %d", length, len(gen2Ids))
		}
		if length != len(gen3Ids) {
			t.Fatalf("Didn't generate enough Ids for list 3, expected %d, got %d", length, len(gen3Ids))
		}

		idSet := make(map[int64]int)

		for i := 0; i < length; i++ {
			prev, _ := idSet[gen1Ids[i]]
			idSet[gen1Ids[i]] = prev + 1

			prev, _ = idSet[gen2Ids[i]]
			idSet[gen2Ids[i]] = prev + 100

			prev, _ = idSet[gen3Ids[i]]
			idSet[gen3Ids[i]] = prev + 100_000
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
}
