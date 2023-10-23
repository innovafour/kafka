package kafka

type KafkaBody struct {
	Action  string                 `json:"action,omitempty"`
	Country string                 `json:"country,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}

type KafkaTopicDTO struct {
	Topic string    `json:"topic"`
	Body  KafkaBody `json:"body"`
}

func (b *KafkaTopicDTO) GetData() map[string]interface{} {
	if b.Body.Body != nil {
		return b.Body.Body
	} else {
		return b.Body.Data
	}
}

func (b *KafkaTopicDTO) Uuid() string {
	uuid, ok := b.Body.Data["uuid"].(string)
	if !ok {
		uuid, ok = b.Body.Body["uuid"].(string)

		if !ok {
			return ""
		}
	}

	return uuid
}
