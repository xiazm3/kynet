extends RefCounted
## Nonblocking transport; all methods run on the Godot main thread.

var last_error: String = ""
var ready: bool = false
var _options
var _codec
var _endpoint: Dictionary
var _tcp: StreamPeerTCP
var _tls: StreamPeerTLS
var _stream: StreamPeer
var _udp: PacketPeerUDP
var _websocket: WebSocketPeer
var _writes: Array[Dictionary] = []
var _receive := PackedByteArray()
var _expected: int = 4
var _reading_prefix: bool = true


func start(options, codec) -> void:
	_options = options
	_codec = codec
	_endpoint = options.parse_endpoint(options.endpoint)
	var error := OK
	match _endpoint.scheme:
		"ws", "wss":
			_websocket = WebSocketPeer.new()
			_websocket.inbound_buffer_size = options.max_frame_length
			_websocket.outbound_buffer_size = options.max_frame_length
			_websocket.max_queued_packets = options.max_packets_per_poll
			error = _websocket.connect_to_url(options.endpoint, options.tls_options)
		"udp":
			_udp = PacketPeerUDP.new()
			error = _udp.connect_to_host(_endpoint.host, _endpoint.port)
			ready = error == OK
		_:
			_tcp = StreamPeerTCP.new()
			error = _tcp.connect_to_host(_endpoint.host, _endpoint.port)
	if error != OK:
		last_error = "Could not open gateway transport: %s" % error_string(error)


func poll() -> Array[PackedByteArray]:
	var packets: Array[PackedByteArray] = []
	if not last_error.is_empty():
		return packets
	if _tcp != null:
		_tcp.poll()
		var status := _tcp.get_status()
		if status == StreamPeerTCP.STATUS_ERROR or status == StreamPeerTCP.STATUS_NONE:
			last_error = "Gateway TCP connection closed."
			return packets
		if status != StreamPeerTCP.STATUS_CONNECTED:
			return packets
		if _stream == null:
			_tcp.set_no_delay(true)
			if _endpoint.scheme == "tcps":
				_tls = StreamPeerTLS.new()
				var error := _tls.connect_to_stream(_tcp, _endpoint.host, _options.tls_options)
				if error != OK:
					last_error = "Could not start TLS: %s" % error_string(error)
					return packets
				_stream = _tls
			else:
				_stream = _tcp
		if _tls != null:
			_tls.poll()
			var tls_status := _tls.get_status()
			if tls_status == StreamPeerTLS.STATUS_HANDSHAKING:
				return packets
			if tls_status != StreamPeerTLS.STATUS_CONNECTED:
				last_error = "TLS handshake failed or gateway closed TLS."
				return packets
		ready = true
	if _websocket != null:
		_websocket.poll()
		var status := _websocket.get_ready_state()
		if status == WebSocketPeer.STATE_CLOSED or status == WebSocketPeer.STATE_CLOSING:
			last_error = "Gateway WebSocket closed."
			return packets
		ready = status == WebSocketPeer.STATE_OPEN
	if not ready:
		return packets
	_flush_writes()
	if not last_error.is_empty():
		return packets
	for index in range(_options.max_packets_per_poll):
		if _udp != null or _websocket != null:
			var peer: PacketPeer = _udp if _udp != null else _websocket
			if peer.get_available_packet_count() == 0:
				break
			var data := peer.get_packet()
			if peer.get_packet_error() != OK:
				last_error = "Gateway packet read failed."
				break
			if _websocket != null and _websocket.was_string_packet():
				last_error = "Expected a binary WebSocket packet."
				break
			packets.append(data)
		else:
			# Read exactly the current prefix/body, never an unbounded stream buffer.
			while _receive.size() < _expected and _stream.get_available_bytes() > 0:
				var count: int = mini(_expected - _receive.size(), _stream.get_available_bytes())
				var part := _stream.get_partial_data(count)
				if part[0] != OK:
					last_error = "Gateway stream read failed."
					return packets
				if part[1].is_empty():
					break
				_receive.append_array(part[1])
			if _receive.size() < _expected:
				break
			if _reading_prefix:
				var prefix := StreamPeerBuffer.new()
				prefix.big_endian = true
				prefix.data_array = _receive
				_expected = prefix.get_32()
				if not _codec.valid_length(_expected):
					last_error = "Invalid TCP gateway frame length."
					break
				_receive = PackedByteArray()
				_reading_prefix = false
			else:
				packets.append(_receive)
				_receive = PackedByteArray()
				_expected = 4
				_reading_prefix = true
	return packets


func enqueue(data: PackedByteArray, request_id: int, deadline: int) -> Error:
	if not ready or not last_error.is_empty():
		return ERR_CONNECTION_ERROR
	if _writes.size() >= _options.max_pending_writes:
		return ERR_BUSY
	if _udp != null and data.size() > 65507:
		return ERR_INVALID_DATA
	if _tcp != null:
		var prefix := StreamPeerBuffer.new()
		prefix.big_endian = true
		prefix.put_32(data.size())
		prefix.put_data(data)
		data = prefix.data_array
	_writes.append({"data": data, "offset": 0, "request_id": request_id, "deadline": deadline})
	return OK


func cancel_write(request_id: int) -> void:
	for index in range(_writes.size() - 1, -1, -1):
		if _writes[index].request_id == request_id:
			if _writes[index].offset > 0:
				last_error = "A partially sent frame was cancelled."
			else:
				_writes.remove_at(index)


func _flush_writes() -> void:
	var budget: int = _options.max_frame_length
	for index in range(mini(_writes.size(), _options.max_packets_per_poll)):
		var entry: Dictionary = _writes[0]
		if Time.get_ticks_msec() >= entry.deadline:
			last_error = "Gateway write timed out."
			return
		var error: int
		if _udp != null:
			error = _udp.put_packet(entry.data)
		elif _websocket != null:
			error = _websocket.put_packet(entry.data)
		else:
			var remaining: PackedByteArray = entry.data.slice(entry.offset, entry.offset + budget)
			var part := _stream.put_partial_data(remaining)
			error = part[0]
			entry.offset += part[1]
			budget -= part[1]
			if error == OK and entry.offset < entry.data.size():
				return
		if error == ERR_BUSY or error == ERR_OUT_OF_MEMORY:
			return
		if error != OK:
			last_error = "Gateway write failed: %s" % error_string(error)
			return
		_writes.pop_front()
		if budget <= 0:
			return


func close() -> void:
	ready = false
	_writes.clear()
	if _websocket != null:
		_websocket.close()
	if _udp != null:
		_udp.close()
	if _tls != null:
		_tls.disconnect_from_stream()
	if _tcp != null:
		_tcp.disconnect_from_host()
	_websocket = null
	_udp = null
	_tls = null
	_tcp = null
	_stream = null
