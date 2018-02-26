package iotservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gopkg.in/satori/go.uuid.v1"
	"pack.ag/amqp"
)

// TODO: this can be separated into eventhub lib.
func EHSubscribe(ctx context.Context, s *amqp.Session, name, group string, f func(*amqp.Message)) error {
	ids, err := getPartitionIDs(ctx, s, name)
	if err != nil {
		return err
	}

	// stop all goroutines at return.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgc := make(chan *amqp.Message, len(ids))
	errc := make(chan error, len(ids))
	for _, id := range ids {
		recv, err := s.NewReceiver(
			amqp.LinkSourceAddress(fmt.Sprintf("/%s/ConsumerGroups/%s/Partitions/%s", name, group, id)),

			// TODO: make it configurable
			amqp.LinkSelectorFilter(fmt.Sprintf("amqp.annotation.x-opt-enqueuedtimeutc > '%d'",
				time.Now().UnixNano()/int64(time.Millisecond)),
			),
		)
		if err != nil {
			return err
		}

		go func(rc *amqp.Receiver) {
			defer recv.Close()
			for {
				msg, err := rc.Receive(ctx)
				if err != nil {
					errc <- err
					return
				}
				msg.Accept()
				msgc <- msg
			}
		}(recv)
	}

	for {
		select {
		case msg := <-msgc:
			go f(msg)
		case err := <-errc:
			return err
		}
	}
}

// getPartitionIDs returns partition ids for the named eventhub.
func getPartitionIDs(ctx context.Context, sess *amqp.Session, name string) ([]string, error) {
	replyTo := uuid.NewV4().String()
	recv, err := sess.NewReceiver(
		amqp.LinkSourceAddress("$management"),
		amqp.LinkTargetAddress(replyTo),
	)
	if err != nil {
		return nil, err
	}
	defer recv.Close()

	send, err := sess.NewSender(
		amqp.LinkTargetAddress("$management"),
		amqp.LinkSourceAddress(replyTo),
	)
	if err != nil {
		return nil, err
	}
	defer send.Close()

	msgID := uuid.NewV4().String()
	if err := send.Send(ctx, &amqp.Message{
		Properties: &amqp.MessageProperties{
			MessageID: msgID,
			ReplyTo:   replyTo,
		},
		ApplicationProperties: map[string]interface{}{
			"operation": "READ",
			"name":      name,
			"type":      "com.microsoft:eventhub",
		},
	}); err != nil {
		return nil, err
	}

	msg, err := recv.Receive(ctx)
	if err != nil {
		return nil, err
	}
	if err = checkResponseMessage(msg); err != nil {
		return nil, err
	}
	if msg.Properties.CorrelationID != msgID {
		return nil, errors.New("message-id mismatch")
	}
	msg.Accept()

	val, ok := msg.Value.(map[string]interface{})
	if !ok {
		return nil, errors.New("unable to typecast value")
	}
	ids, ok := val["partition_ids"].([]string)
	if !ok {
		return nil, errors.New("unable to parse partition_ids")
	}
	return ids, nil
}

// checkResponseMessage checks for 200 response code otherwise returns an error.
func checkResponseMessage(msg *amqp.Message) error {
	rc, ok := msg.ApplicationProperties["status-code"].(int32)
	if !ok {
		return errors.New("unable to parse status-code")
	}
	if rc == 200 {
		return nil
	}
	rd, _ := msg.ApplicationProperties["status-description"]
	return fmt.Errorf("code = %d, description = %q", rc, rd)
}
