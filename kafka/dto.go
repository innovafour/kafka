package kafka

import (
	"encoding/json"
)

type TopicDTO struct {
	Topic string `json:"topic"`
	Body  interface{}
}

func (b *TopicDTO) BodyAsMap() map[string]interface{} {
	bodyBytes, err := json.Marshal(b.Body)
	if err != nil {
		return nil
	}

	bodyAsMap := make(map[string]interface{})
	err = json.Unmarshal(bodyBytes, &bodyAsMap)

	if err != nil {
		return nil
	}

	return bodyAsMap
}

func (b *TopicDTO) tulBody() *TulBody {
	bodyBytes, err := json.Marshal(b.Body)
	if err != nil {
		return nil
	}

	tulBody := &TulBody{}
	err = json.Unmarshal(bodyBytes, tulBody)
	if err != nil {
		return nil
	}

	return tulBody
}

func (b *TopicDTO) GetData() map[string]interface{} {
	tulBody := b.tulBody()

	if tulBody.Body != nil {
		return map[string]interface{}{
			"body": tulBody.Body,
		}
	} else {
		return map[string]interface{}{
			"data": tulBody.Data,
		}
	}
}

func (b *TopicDTO) GetDataValue() map[string]interface{} {
	tulBody := b.tulBody()

	if tulBody.Body != nil {
		return tulBody.Body
	} else {
		return tulBody.Data
	}
}

func (b *TopicDTO) Uuid() string {
	tulBody := b.tulBody()

	uuid, ok := tulBody.Data["uuid"].(string)
	if !ok {
		uuid, ok = tulBody.Body["uuid"].(string)

		if !ok {
			return ""
		}
	}

	return uuid
}

type TulBody struct {
	Action  string                 `json:"action,omitempty"`
	Country string                 `json:"country,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}
