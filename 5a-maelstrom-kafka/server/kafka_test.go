package server

import "testing"

func TestKafka(t *testing.T) {
	kafka := NewKafka()

	kafka.Append("key1", 1)
	kafka.Append("key2", 2)
	kafka.Append("key1", 10)

	offsets := map[string]int{
		"key1": 0,
		"key2": 0,
	}
	expectedLogs := map[string][][2]int{
		"key1": {{0, 1}, {1, 10}},
		"key2": {{0, 2}},
	}
	logs := kafka.Poll(offsets)

	if len(logs) != 2 {
		t.Fatalf("expected to get a map with two keys, got %d keys", len(logs))
	}

	log1, ok := logs["key1"]
	if !ok {
		t.Fatalf("no logs found for key: %s", "key1")
	}
	if len(log1) != len(expectedLogs["key1"]) {
		t.Fatalf("expected %d logs, got %d", len(expectedLogs["key1"]), len(log1))
	}
	for i, log := range log1 {
		if log != expectedLogs["key1"][i] {
			t.Fatalf("log is different at index %d, expected %v, got %v", i, expectedLogs["key1"][i], log)
		}
	}

	log2, ok := logs["key2"]
	if !ok {
		t.Fatalf("no logs found for key: %s", "key2")
	}
	if len(log2) != len(expectedLogs["key2"]) {
		t.Fatalf("expected %d logs, got %d", len(expectedLogs["key2"]), len(log2))
	}
	for i, log := range log2 {
		if log != expectedLogs["key2"][i] {
			t.Fatalf("log is different at index %d, expected %v, got %v", i, expectedLogs["key2"][i], log)
		}
	}
}

func TestTopicPoll(t *testing.T) {
	logs := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	topic := Topic{Logs: logs}

	tests := []struct {
		offset   int
		expected [][2]int
	}{
		{
			offset: 0,
			expected: [][2]int{
				{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4},
				{5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9},
			},
		},
		{
			offset: 9,
			expected: [][2]int{
				{9, 9}, {10, 10}, {11, 11}, {12, 12},
				{13, 13}, {14, 14}, {15, 15},
			},
		},
		{
			offset:   20,
			expected: [][2]int{},
		},
	}

	for _, tt := range tests {
		actual := topic.Poll(tt.offset)

		if len(tt.expected) != len(actual) {
			t.Fatalf("len of actual not equal to expected. expected: %d, got %d",
				len(tt.expected), len(actual))
		}

		for i, val := range tt.expected {
			if actual[i] != val {
				t.Fatalf("actual[i] has incorrect value. expected %d, got %d",
					val, actual[i])
			}
		}
	}
}
