// Package mqtt provides a lightweight MQTT transport adapter for the central brain.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"gocalis/internal/protocol"

	paho "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultTopicPrefix = "gocalis"
	defaultQoS         = 1
)

// Config holds MQTT connection and topic settings.
type Config struct {
	Broker       string
	ClientID     string
	Username     string
	Password     string
	TopicPrefix  string
	QoS          byte
	AutoReconnect bool
}

// Client wraps a Paho MQTT connection and routes commands through the shared Executor.
type Client struct {
	cfg      Config
	client   paho.Client
	executor *protocol.Executor
}

// NewClient creates an MQTT client bound to the given executor.
func NewClient(cfg Config, executor *protocol.Executor) (*Client, error) {
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = defaultTopicPrefix
	}
	if cfg.QoS > 2 {
		cfg.QoS = defaultQoS
	}
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("gocalis-%d", time.Now().Unix())
	}

	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetAutoReconnect(cfg.AutoReconnect).
		SetConnectRetry(cfg.AutoReconnect).
		SetConnectRetryInterval(5 * time.Second).
		SetOnConnectHandler(func(_ paho.Client) {
			log.Println("[MQTT] Connected to broker")
		}).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Printf("[MQTT] Connection lost: %v\n", err)
		})

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	client := paho.NewClient(opts)
	return &Client{
		cfg:      cfg,
		client:   client,
		executor: executor,
	}, nil
}

// Connect establishes the MQTT connection and subscribes to command topics.
func (c *Client) Connect(ctx context.Context) error {
	if token := c.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	topics := map[string]byte{
		c.cmdTopic("tts"):        c.cfg.QoS,
		c.cmdTopic("asr"):        c.cfg.QoS,
		c.cmdTopic("speaker_id"): c.cfg.QoS,
		c.cmdTopic("ask"):        c.cfg.QoS,
	}

	if token := c.client.SubscribeMultiple(topics, c.onMessage); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to command topics: %w", token.Error())
	}

	log.Printf("[MQTT] Subscribed to command topics under %s/cmd/#\n", c.cfg.TopicPrefix)
	return nil
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
	token := c.client.Publish(topic, c.cfg.QoS, false, payload)
	go func() {
		_ = token.Wait()
		if token.Error() != nil {
			log.Printf("[MQTT] Failed to publish event to %s: %v\n", topic, token.Error())
		}
	}()
}

// Close disconnects from the broker.
func (c *Client) Close() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
	}
}

func (c *Client) onMessage(_ paho.Client, msg paho.Message) {
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

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		c.executor.Execute(ctx, req)
	}()
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
