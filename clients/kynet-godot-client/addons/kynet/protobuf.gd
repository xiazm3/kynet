extends RefCounted
## Small wire-field helper, not a schema generator. Raw payloads can also be
## supplied by any external GDScript Protobuf implementation.

const PacketCodec = preload("packet_codec.gd")


static func varint_field(number: int, value: int) -> PackedByteArray:
	var output := _varint(number << 3)
	output.append_array(_varint(value))
	return output


static func bytes_field(number: int, value: PackedByteArray) -> PackedByteArray:
	var output := _varint((number << 3) | 2)
	output.append_array(_varint(value.size()))
	output.append_array(value)
	return output


static func string_field(number: int, value: String) -> PackedByteArray:
	return bytes_field(number, value.to_utf8_buffer())


static func float_field(number: int, value: float, double_precision: bool = true) -> PackedByteArray:
	var stream := StreamPeerBuffer.new()
	if double_precision:
		stream.put_double(value)
	else:
		stream.put_float(value)
	var output := _varint((number << 3) | (1 if double_precision else 5))
	output.append_array(stream.data_array)
	return output


static func decode_fields(data: PackedByteArray) -> Dictionary:
	var reader := {"offset": 0, "ok": true}
	var fields: Dictionary = {}
	while reader.offset < data.size():
		var tag := _read_varint(data, reader)
		var number := tag >> 3
		var wire := tag & 7
		if not reader.ok or number < 1 or number > 536870911:
			return {"ok": false, "fields": {}}
		var value: Variant
		match wire:
			0:
				value = _read_varint(data, reader)
			1, 5:
				var size := 8 if wire == 1 else 4
				if data.size() - reader.offset < size:
					return {"ok": false, "fields": {}}
				var stream := StreamPeerBuffer.new()
				stream.data_array = data.slice(reader.offset, reader.offset + size)
				value = stream.get_double() if wire == 1 else stream.get_float()
				reader.offset += size
			2:
				var size := _read_varint(data, reader)
				if not reader.ok or size < 0 or size > data.size() - reader.offset:
					return {"ok": false, "fields": {}}
				value = data.slice(reader.offset, reader.offset + size)
				reader.offset += size
			_:
				return {"ok": false, "fields": {}}
		if not reader.ok:
			return {"ok": false, "fields": {}}
		if not fields.has(number):
			fields[number] = []
		fields[number].append({"wire": wire, "value": value})
	return {"ok": true, "fields": fields}


static func string_value(data: PackedByteArray, field: int = 1) -> Dictionary:
	var result := decode_fields(data)
	if not result.ok:
		return {"ok": false, "value": ""}
	var entries: Array = result.fields.get(field, [])
	if entries.is_empty():
		return {"ok": true, "value": ""}
	var entry: Dictionary = entries.back()
	if entry.wire != 2 or not PacketCodec._valid_utf8(entry.value):
		return {"ok": false, "value": ""}
	return {"ok": true, "value": entry.value.get_string_from_utf8()}


static func login_message(username: String, password: String, token: String = "") -> PackedByteArray:
	var output := string_field(1, username)
	output.append_array(string_field(2, password))
	if not token.is_empty():
		output.append_array(string_field(3, token))
	return output


# Java PrimitiveValue kinds: INT=0, LONG=1, BOOL=2, DOUBLE=3, FLOAT=4.
static func encode_primitive(value: Variant, kind: int) -> PackedByteArray:
	if kind < 0 or kind > 4:
		return PackedByteArray()
	var output := varint_field(1, kind)
	if kind <= 2:
		output.append_array(varint_field(kind + 2, int(value)))
	else:
		output.append_array(float_field(kind + 2, float(value), kind == 3))
	return output


static func decode_primitive(data: PackedByteArray, kind: int) -> Dictionary:
	var decoded := decode_fields(data)
	if not decoded.ok or kind < 0 or kind > 4:
		return {"ok": false, "value": null}
	var kinds: Array = decoded.fields.get(1, [])
	if kinds.is_empty() or kinds.back().wire != 0 or kinds.back().value != kind:
		return {"ok": false, "value": null}
	var entries: Array = decoded.fields.get(kind + 2, [])
	var expected_wire := 0 if kind <= 2 else (1 if kind == 3 else 5)
	var value: Variant = 0 if kind <= 2 else 0.0
	if not entries.is_empty():
		if entries.back().wire != expected_wire:
			return {"ok": false, "value": null}
		value = entries.back().value
	if kind == 0:
		value = (int(value) & 0xffffffff)
		if value >= 0x80000000:
			value -= 0x100000000
	elif kind == 2:
		value = value != 0
	return {"ok": true, "value": value}


static func _varint(value: int) -> PackedByteArray:
	var output := PackedByteArray()
	while value < 0 or value > 127:
		output.append((value & 127) | 128)
		# Logical right shift of the signed GDScript int, including negative int64.
		value = (value >> 7) & 0x01ffffffffffffff
	output.append(value)
	return output


static func _read_varint(data: PackedByteArray, reader: Dictionary) -> int:
	var value: int = 0
	for index in range(10):
		if reader.offset >= data.size():
			break
		var byte := data[reader.offset]
		reader.offset += 1
		if index == 9 and byte > 1:
			break
		value |= (byte & 127) << (index * 7)
		if byte < 128:
			return value
	reader.ok = false
	return 0
