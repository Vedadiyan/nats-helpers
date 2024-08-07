package headers

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"

	"github.com/nats-io/nats.go"
)

type (
	Header map[string][]string
)

func init() {
	gob.Register(Header{})
}

func (header Header) SetReflector(str string) {
	header["Reflector"] = []string{str}
}

func (header Header) DeleteReflector() {
	delete(header, "Reflector")
}

func (header Header) GetReflector() string {
	val := header["Reflector"]
	if len(val) == 0 {
		return ""
	}
	return val[0]
}

func (header Header) SetStatus(statusCode int) {
	header["Status"] = []string{fmt.Sprintf("%d", statusCode)}
}

func (header Header) DeleteStatus() {
	delete(header, "Status")
}

func (header Header) GetStatus() string {
	val := header["Status"]
	if len(val) == 0 {
		return ""
	}
	return val[0]
}

func (header Header) SetReply(str string) {
	header["Reply"] = []string{str}
}

func (header Header) DeleteReply() {
	delete(header, "Reply")
}

func (header Header) GetReply() string {
	val := header["Reply"]
	if len(val) == 0 {
		return ""
	}
	return val[0]
}

func (header Header) Set(key string, value string) {
	header[key] = []string{value}
}

func (header Header) Add(key string, value string) {
	val, ok := header[key]
	if !ok {
		val = make([]string, 0)
	}
	val = append(val, value)
	header[key] = val
}

func (header Header) Delete(key string) {
	delete(header, key)
}

func (header Header) Get(key string) []string {
	val, ok := header[key]
	if !ok || len(val) == 0 {
		return nil
	}
	return val
}

func (header Header) Export(natsHeader nats.Header) error {
	var writer bytes.Buffer
	encoder := gob.NewEncoder(&writer)
	err := encoder.Encode(header)
	if err != nil {
		return err
	}
	natsHeader.Set("ENC_HEADERS", base64.URLEncoding.EncodeToString(writer.Bytes()))
	return nil
}

func Import(natsHeader nats.Header) (Header, error) {
	headers := natsHeader.Get("ENC_HEADERS")
	if len(headers) == 0 {
		return Header{}, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(headers)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewBuffer(decoded)
	decoder := gob.NewDecoder(reader)
	header := new(Header)
	err = decoder.Decode(header)
	if err != nil {
		return nil, err
	}
	return *header, nil
}
