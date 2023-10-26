package kafka

type TopicDTO struct {
	Topic string `json:"topic"`
	Body  Body   `json:"body"`
}

func (b *TopicDTO) GetData() map[string]interface{} {
	if b.Body.Body != nil {
		return map[string]interface{}{
			"body": b.Body.Body,
		}
	} else {
		return map[string]interface{}{
			"data": b.Body.Data,
		}
	}
}

func (b *TopicDTO) Uuid() string {
	uuid, ok := b.Body.Data["uuid"].(string)
	if !ok {
		uuid, ok = b.Body.Body["uuid"].(string)

		if !ok {
			return ""
		}
	}

	return uuid
}

type Body struct {
	Action  string                 `json:"action,omitempty"`
	Country string                 `json:"country,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}
