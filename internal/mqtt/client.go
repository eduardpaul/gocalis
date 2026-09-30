// Package mqtt provides a lightweight MQTT transport adapter for the central brain.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"gocalis/internal/protocol"
	"gocalis/internal/taskgroup"

	paho "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultTopicPrefix = "gocalis"
	defaultQoS         = 1
)

// Config holds MQTT connection and topic settings.
type Config struct {
	Broker        string
	ClientID      string
	Username      string
	Password      string
	TopicPrefix   string
	QoS           byte
	AutoReconnect bool
}

// Client wraps a Paho MQTT connection and routes commands through the shared Executor.
type Client struct {
	tasks     *taskgroup.Group
	closeOnce sync.Once
	cfg       Config
	client    paho.Client
	executor  *protocol.Executor
}

// NewClient creates an MQTT client bound to the given executor.
func NewClient(ctx context.Context, cfg Config, executor *protocol.Executor) (*Client, error) {
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = defaultTopicPrefix
	}
	if cfg.QoS > 2 {
		cfg.QoS = defaultQoS
	}
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("gocalis-%d", time.Now().Unix())
	}

	c := &Client{cfg: cfg, executor: executor, tasks: taskgroup.New(ctx, 64)}
	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetAutoReconnect(cfg.AutoReconnect).
		SetConnectRetry(false).
		SetConnectTimeout(10 * time.Second).
		SetConnectRetryInterval(5 * time.Second).
		SetOnConnectHandler(func(client paho.Client) {
			_ = c.tasks.Go(func(ctx context.Context) {
				topics := map[string]byte{}
				for _, action := range []string{"tts", "asr", "speaker_id", "ask", "play"} {
					topics[c.cmdTopic(action)] = c.cfg.QoS
				}
				token := client.SubscribeMultiple(topics, c.onMessage)
				select {
				case <-ctx.Done():
					return
				case <-token.Done():
				}
				if err := token.Error(); err != nil {
					log.Printf("[MQTT] subscribe failed: %v", err)
				}
			})
		}).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Printf("[MQTT] Connection lost: %v\n", err)
		})

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	c.client = paho.NewClient(opts)
	return c, nil
}

// Connect establishes the MQTT connection and subscribes to command topics.
func (c *Client) Connect(ctx context.Context) error {
	token := c.client.Connect()
	select {
	case <-ctx.Done():
		c.Close()
		return ctx.Err()
	case <-token.Done():
		return token.Error()
	}
}

// Publish sends an event to the MQTT broker.
func (c *Client) Publish(event protocol.Response) {
	if !c.client.IsConnected() {
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("[MQTT] Failed to marshal event: %v\n", err)
		return
	}

	topic := c.eventTopic(event.Event)
	if err := c.tasks.Go(func(parent context.Context) {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		token := c.client.Publish(topic, c.cfg.QoS, false, payload)
		select {
		case <-ctx.Done():
			return
		case <-token.Done():
		}
		if err := token.Error(); err != nil {
			log.Printf("[MQTT] publish %s: %v", topic, err)
		}
	}); err != nil {
		log.Printf("[MQTT] publish skipped: %v", err)
	}
}

// Close disconnects from the broker.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.tasks.Close()
		c.client.Disconnect(250)
	})
}

func (c *Client) onMessage(_ paho.Client, msg paho.Message) {
	if len(msg.Payload()) > 8<<20 {
		log.Printf("[MQTT] oversized command rejected")
		return
	}
	var req protocol.Request
	if err := json.Unmarshal(msg.Payload(), &req); err != nil {
		log.Printf("[MQTT] Invalid JSON payload on %s: %v\n", msg.Topic(), err)
		c.executor.Publisher.Publish(protocol.Response{
			Event:   "error",
			Status:  "error",
			Message: "invalid JSON payload",
		})
		return
	}

	// Infer action from topic if not present in payload.
	if req.Action == "" {
		req.Action = actionFromTopic(msg.Topic(), c.cfg.TopicPrefix)
	}

	if err := c.executor.Submit(req); err != nil {
		c.executor.Publisher.Publish(protocol.Response{Event: "error", NodeID: req.NodeID, Status: "error", Message: err.Error()})
	}
}

func (c *Client) cmdTopic(action string) string {
	return fmt.Sprintf("%s/cmd/%s", c.cfg.TopicPrefix, action)
}

func (c *Client) eventTopic(event string) string {
	return fmt.Sprintf("%s/event/%s", c.cfg.TopicPrefix, event)
}

func actionFromTopic(topic string, prefix string) string {
	expected := fmt.Sprintf("%s/cmd/", prefix)
	if len(topic) > len(expected) && topic[:len(expected)] == expected {
		return topic[len(expected):]
	}
	return ""
}
