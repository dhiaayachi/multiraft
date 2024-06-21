package encoding

import (
	"bytes"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

// EncodeMsgPack writes an encoded object to a new bytes buffer.
func EncodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{
		BasicHandle: codec.BasicHandle{
			TimeNotBuiltin: true,
		},
	}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}
