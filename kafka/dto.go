package kafka

import (
	"encoding/json"
)

type TopicDTO struct {
	Topic   string    `json:"topic"`
	Headers []Headers `json:"headers"`
	Body    []byte    `json:"body"`
}

func (b *TopicDTO) HeadersAsMapStringString() map[string]string {
	headersAsMap := make(map[string]string)
	for _, header := range b.Headers {
		headersAsMap[string(header.Key)] = string(header.Value)
	}

	return headersAsMap
}

func (b *TopicDTO) BodyAsMap() map[string]interface{} {
	bodyAsMap := make(map[string]interface{})
	err := json.Unmarshal(b.Body, &bodyAsMap)

	if err != nil {
		return nil
	}

	return bodyAsMap
}

func (b *TopicDTO) tulBody() *TulBody {
	tulBody := &TulBody{}
	err := json.Unmarshal(b.Body, tulBody)
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

func (b *TopicDTO) Country() string {
	tulBody := b.tulBody()

	country, ok := tulBody.Data["country"].(string)
	if !ok {
		country, ok = tulBody.Body["country"].(string)

		if !ok {
			return ""
		}
	}

	if country != "" {
		return country
	}

	country = GetKeyFromHeaders(b.HeadersAsMapStringString(), "x-country")

	return country
}

type TulBody struct {
	Action  string                 `json:"action,omitempty"`
	Country string                 `json:"country,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}
