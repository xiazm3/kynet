extends SceneTree

const Client = preload("../addons/kynet/client.gd")
const Codec = preload("../addons/kynet/packet_codec.gd")
const Protobuf = preload("../addons/kynet/protobuf.gd")
const Options = preload("../addons/kynet/options.gd")
const TlsGateway = preload("tls_gateway.gd")

var checks: int = 0
var failures: int = 0


class FakeGateway extends Node:
	const PacketCodec = preload("../addons/kynet/packet_codec.gd")
	var server := TCPServer.new()
	var codec = PacketCodec.new()
	var peers: Array[Dictionary] = []
	var session_counter: int = 40
	var pong_enabled: bool = true
	var fragment: bool = false
	var mismatch: bool = false
	var replacement: bool = false

	func _process(_delta: float) -> void:
		if server.is_connection_available():
			session_counter += 1
			peers.append({"peer": server.take_connection(), "buffer": PackedByteArray(),
				"out": PackedByteArray(), "session": session_counter})
		for entry in peers:
			var peer: StreamPeerTCP = entry.peer
			peer.poll()
			if peer.get_status() != StreamPeerTCP.STATUS_CONNECTED:
				continue
			if peer.get_available_bytes() > 0:
				entry.buffer.append_array(peer.get_data(peer.get_available_bytes())[1])
			while entry.buffer.size() >= 4:
				var prefix := StreamPeerBuffer.new()
				prefix.big_endian = true
				prefix.data_array = entry.buffer
				var size := prefix.get_32()
				if entry.buffer.size() < size + 4:
					break
				var packet: Dictionary = codec.decode(entry.buffer.slice(4, size + 4))
				entry.buffer = entry.buffer.slice(size + 4)
				if packet.one_way:
					continue
				if packet.type == PacketCodec.Type.PING and not pong_enabled:
					continue
				if packet.type == PacketCodec.Type.REQUEST and packet.act == 99:
					continue
				var response := {"type": PacketCodec.Type.PONG if packet.type == PacketCodec.Type.PING else PacketCodec.Type.RESPONSE,
					"request_id": packet.request_id, "cmd": packet.cmd, "act": packet.act,
					"session_id": entry.session + (1 if replacement else 0), "user_id": 0,
					"payload": packet.payload}
				if mismatch: response.act += 1
				queue_packet(entry, response)
			if not entry.out.is_empty():
				var count: int = 1 if fragment else entry.out.size()
				peer.put_data(entry.out.slice(0, count))
				entry.out = entry.out.slice(count)

	func queue_packet(entry: Dictionary, packet: Dictionary) -> void:
		var bytes: PackedByteArray = codec.encode(packet)
		var stream := StreamPeerBuffer.new()
		stream.big_endian = true
		stream.put_32(bytes.size())
		stream.put_data(bytes)
		entry.out.append_array(stream.data_array)

	func drop_peers() -> void:
		for entry in peers:
			entry.peer.disconnect_from_host()
		peers.clear()

	func _exit_tree() -> void:
		drop_peers()
		server.stop()


func _initialize() -> void:
	_run.call_deferred()


func _check(condition: bool, message: String) -> void:
	checks += 1
	if not condition:
		failures += 1
		push_error("FAIL: " + message)


func _run() -> void:
	_protocol_tests()
	_protobuf_tests()
	await _network_tests()
	await _boundary_tests()
	await _tls_tests()
	print("TESTS: %d checks, %d failures" % [checks, failures])
	quit(0 if failures == 0 else 1)


func _protocol_tests() -> void:
	var codec = Codec.new()
	var packet := {"type": Codec.Type.REQUEST, "request_id": 0x0102030405060708,
		"cmd": 1, "act": 2, "timeout_ms": 3000, "session_id": 9223372036854775807,
		"user_id": 10001, "payload": PackedByteArray([10, 1, 97])}
	var bytes: PackedByteArray = codec.encode(packet)
	var expected := "4b594e5400010000010203040506070800000001000000020000000000000bb8"
	expected += "7fffffffffffffff0000000000002711ffffffffffffffff000000030a0161"
	_check(bytes.hex_encode() == expected, "Java/C# wire golden vector")
	var decoded: Dictionary = codec.decode(bytes)
	_check(decoded.session_id == packet.session_id and decoded.payload == packet.payload, "int64 precision and payload")
	_check(codec.decode(codec.encode({})).payload == null, "null payload")
	decoded = codec.decode(codec.encode({"payload": PackedByteArray(), "error_code": ""}))
	_check(decoded.payload == PackedByteArray() and decoded.error_code == "" and not decoded.ok, "empty payload/error distinct from null")
	for length in range(bytes.size()):
		_check(codec.decode(bytes.slice(0, length)).is_empty(), "reject truncation %d" % length)
	var trailing := bytes.duplicate()
	trailing.append(0)
	_check(codec.decode(trailing).is_empty(), "reject trailing data")
	for index in [0, 5, 6, 48]:
		var malformed := bytes.duplicate()
		malformed[index] = 127
		_check(codec.decode(malformed).is_empty(), "reject malformed header/length")
	var invalid_utf8: PackedByteArray = codec.encode({"error_code": "a"})
	invalid_utf8[52] = 255
	_check(codec.decode(invalid_utf8).is_empty(), "reject invalid UTF-8")
	codec.max_payload_length = 2
	_check(codec.decode(bytes).is_empty(), "decode payload limit")
	_check(codec.encode(packet).is_empty(), "encode payload limit")
	codec.max_string_length = 1
	_check(codec.encode({"error_code": "中文"}).is_empty(), "UTF-8 byte length limit")
	_check(Codec.action_key(65535, 65535) == 4294967295, "unsigned action halves")
	_check(Options.parse_endpoint("tcp://[::1]:30001").host == "::1", "IPv6 endpoint")
	for endpoint in ["http://localhost:1", "tcp://localhost", "tcp://a:0", "tcp://a:65536", "tcp://a:1/path", "ws://u:p@a:1/ws", "ws://a:1/#x"]:
		_check(Options.parse_endpoint(endpoint).is_empty(), "reject endpoint " + endpoint)


func _protobuf_tests() -> void:
	_check(Protobuf.login_message("a", "b", "c").hex_encode() == "0a01611201621a0163", "login contract")
	_check(Protobuf.string_value("0a0777656c636f6d65".hex_decode()).value == "welcome", "Google StringValue")
	_check(Protobuf.encode_primitive(-1, 0).hex_encode() == "080010ffffffffffffffffff01", "negative int32 golden vector")
	var values := [42, -9223372036854775807 - 1, true, 1.25, -2.5]
	for kind in range(5):
		var result := Protobuf.decode_primitive(Protobuf.encode_primitive(values[kind], kind), kind)
		_check(result.ok and result.value == values[kind], "PrimitiveValue kind %d" % kind)
	_check(not Protobuf.decode_primitive(Protobuf.encode_primitive(42, 1), 0).ok, "primitive kind mismatch")
	for bytes in ["00", "0aff", "08ffffffffffffffffffff02", "0a04ff", "0b", "0a01ff"]:
		var result := Protobuf.string_value(bytes.hex_decode())
		_check(not result.ok, "malformed protobuf " + bytes)


func _make_client(endpoint: String):
	var client = Client.new()
	client.options.endpoint = endpoint
	client.options.heartbeat_interval_ms = 0
	client.options.connect_timeout_ms = 1500
	root.add_child(client)
	return client


func _network_tests() -> void:
	var server = FakeGateway.new()
	_check(server.server.listen(0, "127.0.0.1") == OK, "fake gateway listen")
	root.add_child(server)
	var endpoint := "tcp://127.0.0.1:%d" % server.server.get_local_port()
	var client = _make_client(endpoint)
	server.fragment = true
	var connection: Dictionary = await client.connect_async()
	_check(connection.ok and client.session_id > 0, "fragmented TCP handshake")
	var response: Dictionary = await client.request_async(1, 2, "echo".to_utf8_buffer())
	_check(response.ok and response.payload.get_string_from_utf8() == "echo", "fragmented response")
	server.fragment = false
	var first = client.request(1, 2)
	var second = client.request_message(65538)
	_check((await first.wait()).ok and (await second.wait()).ok, "coalesced frames and concurrent requests")
	var events: Array = []
	var handler := func(packet): events.append(packet)
	client.subscribe_message(200001, handler)
	server.queue_packet(server.peers.back(), {"type": Codec.Type.PUSH, "cmd": 3, "act": 3393, "payload": "push".to_utf8_buffer()})
	await create_timer(0.05).timeout
	_check(events.size() == 1, "messageId push subscription")
	client.unsubscribe_message(200001, handler)
	server.queue_packet(server.peers.back(), {"type": Codec.Type.PUSH, "cmd": 3, "act": 3393})
	await create_timer(0.05).timeout
	_check(events.size() == 1, "unsubscribe")
	var timed_out: Dictionary = await client.request_async(1, 99, null, 30)
	_check(timed_out.error_code == "TIMEOUT" and client.pending_request_count == 0, "request timeout cleanup")
	var cancelled = client.request(1, 99)
	cancelled.cancel()
	_check((await cancelled.wait()).error_code == "CANCELLED" and client.pending_request_count == 0, "cancel before waiting")
	_check(client.send_one_way(1, 3) == OK, "one-way queue")
	var outstanding = client.request(1, 99)
	client.disconnect_from_gateway()
	_check((await outstanding.wait()).error_code == "DISCONNECTED", "disconnect completes pending awaiters")
	_check(client.session_id == 0 and client.user_id == 0, "disconnect resets session")
	_check(not (await client.request(1, 2).wait()).ok, "request before connection returns error")
	server.drop_peers()
	_check((await client.connect_async()).ok, "explicit reconnect")
	server.replacement = true
	_check((await client.request_async(1, 2)).error_code == "SESSION_REPLACED", "session replacement invalidates connection")
	server.replacement = false
	server.drop_peers()
	_check((await client.connect_async()).ok, "connect after replacement")
	server.mismatch = true
	_check((await client.request_async(1, 2)).error_code == "PROTOCOL_ERROR", "response command correlation")
	server.mismatch = false
	client.queue_free()
	await process_frame
	server.drop_peers()
	client = _make_client(endpoint)
	client.options.auto_reconnect = true
	client.options.reconnect_initial_delay_ms = 10
	client.options.reconnect_max_delay_ms = 20
	var recoveries: Array = []
	client.session_recovery_handler = func(gateway):
		var result: Dictionary = await gateway.request_async(1, 2)
		recoveries.append(result.ok)
		return result.ok
	_check((await client.connect_async()).ok and recoveries.is_empty(), "initial connect skips recovery")
	var old_session: int = client.session_id
	server.drop_peers()
	await create_timer(0.3).timeout
	_check(client.state == Client.State.CONNECTED and client.session_id != old_session and recoveries == [true], "reconnect and async recovery")
	client.disconnect_from_gateway()
	await create_timer(0.1).timeout
	_check(client.state == Client.State.DISCONNECTED, "manual disconnect stops reconnect")
	server.drop_peers()
	_check((await client.connect_async()).ok, "connect for reentrancy check")
	var operation = client.request(1, 99)
	operation.completed.connect(func(_result): client.connect_to_gateway())
	client.disconnect_from_gateway()
	await create_timer(0.1).timeout
	_check(client.state == Client.State.CONNECTED, "awaiter reconnect during disconnect preserves new state")
	client.disconnect_from_gateway()
	client.queue_free()
	await process_frame
	server.drop_peers()
	client = _make_client(endpoint)
	client.options.heartbeat_interval_ms = 20
	client.options.heartbeat_timeout_ms = 30
	_check((await client.connect_async()).ok, "heartbeat initial connection")
	server.pong_enabled = false
	await create_timer(0.2).timeout
	_check(client.state == Client.State.DISCONNECTED and client.session_id == 0, "heartbeat timeout detects lost connection")
	client.queue_free()
	server.queue_free()
	await process_frame


func _boundary_tests() -> void:
	var server = FakeGateway.new()
	_check(server.server.listen(0, "127.0.0.1") == OK, "boundary gateway listen")
	root.add_child(server)
	var endpoint := "tcp://127.0.0.1:%d" % server.server.get_local_port()
	var client = _make_client(endpoint)
	client.options.max_pending_requests = 1
	client.options.max_pending_writes = 1
	_check((await client.connect_async()).ok, "connect with minimum queue bounds")
	var pending = client.request(1, 99)
	_check((await client.request(1, 2).wait()).error_code == "QUEUE_FULL", "pending request bound")
	pending.cancel()
	_check(client.send_one_way_message(65539) == OK, "first bounded send")
	_check(client.send_one_way(1, 3) == ERR_BUSY, "send queue bound")
	await create_timer(0.03).timeout
	pending = client.request(1, 99)
	var exit_attempts: Array = []
	pending.completed.connect(func(_result): exit_attempts.append(client.connect_to_gateway().result))
	client.queue_free()
	await process_frame
	_check(pending.is_completed and pending.result.error_code == "DISCONNECTED", "freeing node completes pending operation")
	_check(exit_attempts.size() == 1 and exit_attempts[0].error_code == "INVALID_STATE", "exit callback cannot restart a freed client")
	server.drop_peers()
	client = _make_client(endpoint)
	client.options.auto_reconnect = true
	client.options.recovery_timeout_ms = 30
	client.options.reconnect_initial_delay_ms = 10
	client.options.reconnect_max_delay_ms = 20
	client.session_recovery_handler = func(gateway):
		var result: Dictionary = await gateway.request_async(1, 99)
		return result.ok
	var errors: Array = []
	client.connection_error.connect(func(code, _message):
		errors.append(code)
		if code == "RECOVERY_TIMEOUT": client.disconnect_from_gateway())
	_check((await client.connect_async()).ok, "connect for recovery timeout")
	server.drop_peers()
	await create_timer(0.2).timeout
	_check(errors.has("RECOVERY_TIMEOUT") and client.state == Client.State.DISCONNECTED, "bounded recovery and stop from error callback")
	client.queue_free()
	await process_frame
	server.drop_peers()
	server.pong_enabled = false
	client = _make_client(endpoint)
	client.options.connect_timeout_ms = 30
	client.options.auto_reconnect = true
	_check(not (await client.connect_async()).ok and client.state == Client.State.DISCONNECTED, "first handshake failure completes connect without retry")
	client.queue_free()
	server.queue_free()
	await process_frame
	client = _make_client("udp://127.0.0.1:9")
	client.options.connect_timeout_ms = 30
	var operation = client.connect_to_gateway()
	operation.cancel()
	_check(operation.is_completed and client.state == Client.State.DISCONNECTED, "connect cancellation stops connection")
	client.queue_free()
	await process_frame


func _tls_tests() -> void:
	for scheme in ["tcps", "wss"]:
		var server = TlsGateway.new()
		_check(server.start(scheme == "wss") == OK, "TLS fixture listen")
		root.add_child(server)
		var endpoint := "%s://localhost:%d" % [scheme, server.server.get_local_port()]
		if scheme == "wss": endpoint += "/ws"
		var client = _make_client(endpoint)
		var result: Dictionary = await client.connect_async()
		_check(not result.ok and result.error_code == "CONNECTION_LOST" and client.session_id == 0, scheme + " rejects untrusted certificate: " + str(result))
		client.queue_free()
		await process_frame
		client = _make_client(endpoint)
		client.options.tls_options = TLSOptions.client(server.certificate)
		result = await client.connect_async()
		_check(result.ok, scheme + " connects with explicit trusted CA: " + str(result))
		if result.ok:
			var response: Dictionary = await client.request_async(1, 2, "secure".to_utf8_buffer())
			_check(response.ok and response.payload.get_string_from_utf8() == "secure", scheme + " encrypted request/response")
		client.disconnect_from_gateway()
		client.queue_free()
		server.queue_free()
		await process_frame
