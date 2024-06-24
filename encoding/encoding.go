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

// DecodeMsgPack reverses the encode operation on a byte slice input.
func DecodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}
