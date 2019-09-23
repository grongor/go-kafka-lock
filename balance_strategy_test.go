package kafkalock

import (
	"reflect"
	"sort"
	"testing"

	"github.com/Shopify/sarama"
)

func Test_Plan(t *testing.T) {
	tests := []struct {
		name        string
		maxHolders  uint8
		members     map[string]sarama.ConsumerGroupMemberMetadata
		lockHolders []string
	}{
		{
			"single member, 1 partition, no preference",
			1,
			map[string]sarama.ConsumerGroupMemberMetadata{"m1": {}},
			[]string{"m1"},
		},
		{
			"two members, 1 partition, no preference",
			1,
			map[string]sarama.ConsumerGroupMemberMetadata{"m1": {}, "m2": {}},
			[]string{"m1"},
		},
		{
			"two members, 1 partition, with preference",
			1,
			map[string]sarama.ConsumerGroupMemberMetadata{"m1": {}, "m2": {UserData: []byte{1}}},
			[]string{"m2"},
		},
		{
			"five members, 3 partitions, no preference",
			3,
			map[string]sarama.ConsumerGroupMemberMetadata{"m1": {}, "m2": {}, "m3": {}, "m4": {}, "m5": {}},
			[]string{"m1", "m2", "m3"},
		},
		{
			"five members, 3 partitions, with preference",
			3,
			map[string]sarama.ConsumerGroupMemberMetadata{
				"m1": {}, "m2": {UserData: []byte{1}}, "m3": {}, "m4": {}, "m5": {UserData: []byte{1}},
			},
			[]string{"m1", "m2", "m5"},
		},
	}
	logger := NewNopLogger()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			strategy := kafkaLockBalanceStrategy{logger}
			topics := map[string][]int32{"loremLock": generatePartitions(test.maxHolders)}
			plan, err := strategy.Plan(test.members, topics)
			if err != nil {
				t.Fatal("expected no error")
			}

			members := make([]string, 0, len(test.lockHolders))
			for memberID, topics := range plan {
				partitions := topics["loremLock"]
				if len(partitions) == 1 {
					members = append(members, memberID)
				}
			}

			sort.Strings(members)

			if !reflect.DeepEqual(test.lockHolders, members) {
				t.Fatalf("expected different lock holders\nexpected:\t%v\ngot:\t%v", test.lockHolders, members)
			}
		})
	}
}

func generatePartitions(maxHolders uint8) []int32 {
	partitions := make([]int32, maxHolders)
	for i := range partitions {
		partitions[i] = int32(i)
	}

	return partitions
}
