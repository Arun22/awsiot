package awsiot

import (
	"crypto/tls"
	"fmt"
	"log"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type AwsIotConfig struct {
	DeviceCert string //Device Cert Path
	DeviceKey  string //Device Key Path
	DeviceId   string
	AwsUrl     string
}

type MqttType int

const (
	PUBLISH MqttType = iota + 1
	SUBSCRIBE
)

const MAX_TOPIC_REGISTER = 500

type AwsIotClient struct {
	mqttClient MQTT.Client
	deviceId   string
	topicsReg  [MAX_TOPIC_REGISTER]struct {
		topicName string
		topicType MqttType
	}
	topicsCount int
}

type AwsIotMsgHandler func(string)

func NewAwsIotClient(cfg AwsIotConfig) *AwsIotClient {

	iotClient := &AwsIotClient{}
	cer, err := tls.LoadX509KeyPair(cfg.DeviceCert, cfg.DeviceKey)
	if err != nil {
		panic(err)
	}

	cid := cfg.DeviceId

	// AutoReconnect option is true by default
	// CleanSession option is true by default
	// KeepAlive option is 30 seconds by default
	connOpts := MQTT.NewClientOptions() // This line is different, we use the constructor function instead of creating the instance ourselves.
	connOpts.SetClientID(cid)
	connOpts.SetMaxReconnectInterval(1 * time.Second)
	connOpts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}})

	host := cfg.AwsUrl //"a3ai5mxalabqsy-ats.iot.us-west-2.amazonaws.com"
	port := 8883
	path := "/mqtt"

	brokerURL := fmt.Sprintf("tcps://%s:%d%s", host, port, path)
	connOpts.AddBroker(brokerURL)

	mqttClient := MQTT.NewClient(connOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("[MQTT] Connected")
	iotClient.mqttClient = mqttClient
	iotClient.deviceId = cid
	return iotClient
}

func (c *AwsIotClient) RegisterTopic(tn string, tt MqttType, cb AwsIotMsgHandler) {
	c.mqttClient.Subscribe(tn, 0, func(client MQTT.Client, msg MQTT.Message) {
		log.Println(msg.Topic, ":", string(msg.Payload()))
		//		cb(string([]byte(msg.Payload)))
	})

}

func (c *AwsIotClient) PublishTopic(tn string, payload string) {
	c.mqttClient.Publish(tn, 0, false, []byte(payload))
}

func (c *AwsIotClient) DisconnectAwsIot() {
	c.mqttClient.Disconnect(250)
	fmt.Println("[MQTT] Disconnected")
}
