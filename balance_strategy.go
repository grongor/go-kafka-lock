package kafkalock

import (
	"sort"

	"github.com/Shopify/sarama"
)

type memberInfo struct {
	MemberID string
	HasLock  bool
}

// Very simple custom sarama.BalanceStrategy implementation. It ensures that if a KafkaLocker instance holds the lock,
// then it will be assigned a partition during rebalance (so it won't lose the lock). It may not be the same partition,
// but that doesn't matter. This preference is indicated by single byte in UserData (consumer group member metadata).
type kafkaLockBalanceStrategy struct {
	logger *Logger
}

func (kafkaLockBalanceStrategy) Name() string {
	return "kafkalock"
}

func (s kafkaLockBalanceStrategy) Plan(
	members map[string]sarama.ConsumerGroupMemberMetadata,
	topics map[string][]int32,
) (sarama.BalanceStrategyPlan, error) {
	var membersArr []memberInfo
	for memberID, meta := range members {
		membersArr = append(membersArr, memberInfo{memberID, len(meta.UserData) == 1})
	}

	sort.Slice(membersArr, func(i, j int) bool {
		if membersArr[i].HasLock {
			return true
		}

		if membersArr[j].HasLock {
			return false
		}

		return membersArr[i].MemberID < membersArr[j].MemberID
	})

	var topic string
	for topic = range topics {
		break
	}
	maxHolders := len(topics[topic])

	plan := make(sarama.BalanceStrategyPlan, len(members))
	for i, member := range membersArr {
		if maxHolders > i {
			plan.Add(member.MemberID, topic, int32(i))
		}
	}

	s.logger.Debugw("rebalancing", "members", membersArr, "maxHolders", maxHolders)

	return plan, nil
}

var _ sarama.BalanceStrategy = (*kafkaLockBalanceStrategy)(nil)
