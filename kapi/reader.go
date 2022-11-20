package kapi

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
	"github.com/wvanbergen/kafka/consumergroup"
)

type Reader struct {
	*consumergroup.ConsumerGroup
	topic  string
	cgroup string
}

func NewReader(zookeeperConn, topic, cgroup string) (*Reader, error) {
	// consumer config
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	reader := &Reader{
		topic:  topic,
		cgroup: cgroup,
	}

	// join to consumer group
	if cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{topic}, []string{zookeeperConn}, config); err != nil {
		return nil, err
	} else {
		reader.ConsumerGroup = cg
	}

	return reader, nil
}

func (reader *Reader) Consume() {
	log.Info().Msg("start consuming")
	for {
		select {
		case msg := <-reader.Messages():
			// messages coming through chanel
			// only take messages from subscribed topic
			if msg.Topic != reader.topic {
				continue
			}

			// log.Info().Msgf("Topic %s, Value %s", msg.Topic, string(msg.Value))
			log.Info().Msgf("topic %s, value %v", msg.Topic, msg.Value)

			car := CarInfo{}
			if err := json.Unmarshal([]byte(msg.Value), &car); err != nil {
				log.Fatal().Err(err)
			}

			fmt.Println(car)

			// commit to zookeeper that message is read
			// this prevent read message multiple times after restart
			err := reader.CommitUpto(msg)
			if err != nil {
				log.Fatal().Err(err).Msg("error commit zookeeper")
			}
		}
	}
}
