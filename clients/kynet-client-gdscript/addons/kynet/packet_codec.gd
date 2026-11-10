extends RefCounted
## Kynet gateway wire v1. No TCP prefix here. null and empty bytes stay distinct.

enum Type { REQUEST, RESPONSE, PUSH, PING, PONG }

const MAGIC := 0x4b594e54
const VERSION := 1
const MIN_PACKET_LENGTH := 60

var max_frame_length: int = 1024 * 1024
var max_payload_length: int = 512 * 1024
var max_string_length: int = 8 * 1024
var last_error: String = ""


func valid_length(length: int) -> bool:
	return length >= MIN_PACKET_LENGTH and length <= max_frame_length - 4


func encode(packet: Dictionary) -> PackedByteArray:
	last_error = ""
	var kind: int = packet.get("type", Type.REQUEST)
	if kind < Type.REQUEST or kind > Type.PONG:
		return _encode_error("Unknown packet type.")
	var fields: Array = []
	for key in ["error_code", "error_message", "payload"]:
		var value: Variant = packet.get(key)
		if value != null:
			if key != "payload":
				if not value is String:
					return _encode_error("Error fields must be strings or null.")
				value = value.to_utf8_buffer()
			elif not value is PackedByteArray:
				return _encode_error("Payload must be PackedByteArray or null.")
			var limit := max_payload_length if key == "payload" else max_string_length
			if value.size() > limit:
				return _encode_error("Variable-length field exceeds its limit.")
		fields.append(value)
	var length := MIN_PACKET_LENGTH
	for value in fields:
		if value != null:
			length += value.size()
	if not valid_length(length):
		return _encode_error("Invalid gateway frame length.")
	var stream := StreamPeerBuffer.new()
	stream.big_endian = true
	stream.put_32(MAGIC)
	stream.put_16(VERSION)
	stream.put_u8(kind)
	stream.put_u8(1 if packet.get("one_way", false) else 0)
	stream.put_64(packet.get("request_id", 0))
	stream.put_32(packet.get("cmd", 0))
	stream.put_32(packet.get("act", 0))
	stream.put_64(packet.get("timeout_ms", 0))
	stream.put_64(packet.get("session_id", 0))
	stream.put_64(packet.get("user_id", 0))
	for value in fields:
		stream.put_32(-1 if value == null else value.size())
		if value != null:
			stream.put_data(value)
	return stream.data_array


func decode(data: PackedByteArray) -> Dictionary:
	last_error = ""
	if not valid_length(data.size()):
		return _decode_error("Invalid gateway frame length.")
	var stream := StreamPeerBuffer.new()
	stream.big_endian = true
	stream.data_array = data
	if stream.get_32() != MAGIC or stream.get_16() != VERSION:
		return _decode_error("Invalid magic or unsupported gateway version.")
	var kind := stream.get_u8()
	if kind > Type.PONG:
		return _decode_error("Unknown packet type.")
	var packet := {"type": kind, "one_way": stream.get_u8() != 0,
		"request_id": stream.get_64(), "cmd": stream.get_32(), "act": stream.get_32(),
		"timeout_ms": stream.get_64(), "session_id": stream.get_64(), "user_id": stream.get_64()}
	for key in ["error_code", "error_message", "payload"]:
		if stream.get_available_bytes() < 4:
			return _decode_error("Truncated variable-length field.")
		var length := stream.get_32()
		var limit := max_payload_length if key == "payload" else max_string_length
		if length == -1:
			packet[key] = null
			continue
		if length < 0 or length > limit or length > stream.get_available_bytes():
			return _decode_error("Invalid variable-length field size.")
		var value: PackedByteArray = stream.get_data(length)[1]
		if key == "payload":
			packet[key] = value
		else:
			# Round trip validation rejects invalid UTF-8 without logging engine errors.
			if not _valid_utf8(value):
				return _decode_error("Invalid UTF-8 error field.")
			packet[key] = value.get_string_from_utf8()
	if stream.get_available_bytes() != 0:
		return _decode_error("Unexpected trailing packet data.")
	packet["ok"] = packet.error_code == null
	packet["transport_error"] = false
	return packet


static func action_key(cmd: int, act: int) -> int:
	if cmd < 0 or cmd > 65535 or act < 0 or act > 65535:
		return -1
	return (cmd << 16) | act


static func _valid_utf8(bytes: PackedByteArray) -> bool:
	var offset := 0
	while offset < bytes.size():
		var first := bytes[offset]
		offset += 1
		if first < 0x80:
			continue
		var count: int
		var low := 0x80
		var high := 0xbf
		if first >= 0xc2 and first <= 0xdf:
			count = 1
		elif first >= 0xe0 and first <= 0xef:
			count = 2
			if first == 0xe0: low = 0xa0
			if first == 0xed: high = 0x9f
		elif first >= 0xf0 and first <= 0xf4:
			count = 3
			if first == 0xf0: low = 0x90
			if first == 0xf4: high = 0x8f
		else:
			return false
		if offset + count > bytes.size() or bytes[offset] < low or bytes[offset] > high:
			return false
		for index in range(offset + 1, offset + count):
			if bytes[index] < 0x80 or bytes[index] > 0xbf:
				return false
		offset += count
	return true


func _encode_error(message: String) -> PackedByteArray:
	last_error = message
	return PackedByteArray()


func _decode_error(message: String) -> Dictionary:
	last_error = message
	return {}
