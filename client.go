package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		panic(fmt.Sprintf("Failed to connect: %v", err))
	}
	defer conn.Close()

	// 1. Prepare Payload
	key := "my_key"
	val := []byte("hello_production")
	
	keyLen := uint16(len(key))
	payload := make([]byte, 2+len(key)+len(val))
	binary.BigEndian.PutUint16(payload[0:2], keyLen)
	copy(payload[2:], key)
	copy(payload[2+keyLen:], val)

	// 2. Prepare Header
	header := make([]byte, 11)
	binary.BigEndian.PutUint16(header[0:2], 0xDEAD) // MagicBytes
	header[2] = 0                                   // OpPut
	binary.BigEndian.PutUint32(header[3:7], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[7:11], crc32.ChecksumIEEE(payload))

	// 3. Send Frame
	conn.Write(append(header, payload...))

	// 4. Read Response
	respBuf := make([]byte, 1024)
	n, err := conn.Read(respBuf)
	if err != nil {
		panic(fmt.Sprintf("Read failed: %v", err))
	}
	
	fmt.Printf("Server Response: %s", string(respBuf[:n]))
}