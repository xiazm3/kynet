extends Node
## Portable Kynet client. Add to the tree (or use as an Autoload) before connecting.
## All signals, operations and recovery callbacks execute on the main thread.

const Options = preload("options.gd")
const PacketCodec = preload("packet_codec.gd")
const Operation = preload("operation.gd")
const Transport = preload("transport.gd")

enum State { DISCONNECTED, CONNECTING, RECOVERING, CONNECTED, RECONNECTING }

signal state_changed(state: State)
signal session_changed(session_id: int, user_id: int)
signal push_received(packet: Dictionary)
signal connection_error(code: String, message: String)

var options = Options.new()
# Called only after reconnect: func(client) -> bool (may await a login request).
var session_recovery_handler: Callable
var state: State = State.DISCONNECTED
var session_id: int = 0
var user_id: int = 0
var pending_request_count: int:
	get: return _pending.size()

var _config
var _codec = PacketCodec.new()
var _wire
var _connect_operation
var _pending: Dictionary = {}
var _listeners: Dictionary = {}
var _next_id: int = 0
var _generation: int = 0
var _running: bool = false
var _exiting: bool = false
var _connected_once: bool = false
var _handshake_id: int = 0
var _heartbeat_id: int = 0
var _connect_deadline: int = 0
var _recovery_deadline: int = 0
var _next_heartbeat: int = 0
var _retry_at: int = 0
var _retry_delay: int = 0


func _init() -> void:
	process_mode = Node.PROCESS_MODE_ALWAYS
	set_process(false)


func connect_to_gateway(endpoint: String = ""):
	if _exiting or not is_inside_tree():
		var invalid = Operation.new()
		invalid.finish(Operation.failure("INVALID_STATE", "Add the client to the scene tree first."))
		return invalid
	if _running:
		if state == State.CONNECTED:
			var done = Operation.new()
			done.finish(_session_result())
			return done
		if _connect_operation == null or _connect_operation.is_completed:
			_connect_operation = _new_connect_operation()
		return _connect_operation
	var operation = _new_connect_operation()
	_config = options.snapshot()
	if not endpoint.is_empty():
		_config.endpoint = endpoint
	var validation: String = _config.validate()
	if not validation.is_empty():
		operation.finish(Operation.failure("INVALID_OPTIONS", validation))
		return operation
	_connect_operation = operation
	_codec.max_frame_length = _config.max_frame_length
	_codec.max_payload_length = _config.max_payload_length
	_codec.max_string_length = _config.max_string_length
	_running = true
	_connected_once = false
	_retry_delay = _config.reconnect_initial_delay_ms
	set_process(true)
	_begin_connection()
	return operation


func connect_async(endpoint: String = "") -> Dictionary:
	return await connect_to_gateway(endpoint).wait()


func disconnect_from_gateway() -> void:
	_running = false
	_generation += 1
	set_process(false)
	_cleanup("DISCONNECTED", "Gateway client disconnected.")


func request(cmd: int, act: int, payload: Variant = null, timeout_ms: int = -1):
	var packet := {"type": PacketCodec.Type.REQUEST, "cmd": cmd, "act": act, "payload": payload}
	return _request(packet, timeout_ms)


func request_async(cmd: int, act: int, payload: Variant = null, timeout_ms: int = -1) -> Dictionary:
	return await request(cmd, act, payload, timeout_ms).wait()


func request_message(message_id: int, payload: Variant = null, timeout_ms: int = -1):
	return request((message_id >> 16) & 65535, message_id & 65535, payload, timeout_ms)


func send_one_way(cmd: int, act: int, payload: Variant = null) -> Error:
	if not _can_request():
		return ERR_CONNECTION_ERROR
	if cmd < 1 or cmd > 65535 or act < 1 or act > 65535:
		return ERR_INVALID_PARAMETER
	var id := _allocate_id()
	return _send({"type": PacketCodec.Type.REQUEST, "one_way": true,
		"cmd": cmd, "act": act, "payload": payload, "request_id": id,
		"timeout_ms": _config.request_timeout_ms}, Time.get_ticks_msec() + _config.request_timeout_ms)


func ping():
	return _request({"type": PacketCodec.Type.PING}, -1)


func send_one_way_message(message_id: int, payload: Variant = null) -> Error:
	return send_one_way((message_id >> 16) & 65535, message_id & 65535, payload)


func subscribe(cmd: int, act: int, handler: Callable) -> Error:
	if cmd < 0 or cmd > 65535 or act < 0 or act > 65535 or not handler.is_valid():
		return ERR_INVALID_PARAMETER
	var key := PacketCodec.action_key(cmd, act)
	if not _listeners.has(key):
		_listeners[key] = []
	if not _listeners[key].has(handler):
		_listeners[key].append(handler)
	return OK


func subscribe_message(message_id: int, handler: Callable) -> Error:
	return subscribe((message_id >> 16) & 65535, message_id & 65535, handler)


func unsubscribe(cmd: int, act: int, handler: Callable) -> void:
	var key := PacketCodec.action_key(cmd, act)
	if _listeners.has(key):
		_listeners[key].erase(handler)
		if _listeners[key].is_empty():
			_listeners.erase(key)


func unsubscribe_message(message_id: int, handler: Callable) -> void:
	unsubscribe((message_id >> 16) & 65535, message_id & 65535, handler)


func _process(_delta: float) -> void:
	if not _running:
		return
	var now := Time.get_ticks_msec()
	if _wire == null:
		if now >= _retry_at:
			_begin_connection()
		return
	if state == State.RECOVERING and now >= _recovery_deadline:
		_fail_connection("RECOVERY_TIMEOUT", "Session recovery timed out.")
		return
	var generation := _generation
	var packets: Array = _wire.poll()
	if not _wire.last_error.is_empty():
		_fail_connection("CONNECTION_LOST", _wire.last_error)
		return
	if state in [State.CONNECTING, State.RECONNECTING] and _handshake_id == 0:
		if now >= _connect_deadline:
			_fail_connection("CONNECT_TIMEOUT", "Gateway connection timed out.")
			return
		if _wire.ready:
			var handshake = _request({"type": PacketCodec.Type.PING}, _config.connect_timeout_ms, true)
			_handshake_id = handshake.request_id
			if handshake.is_completed:
				_fail_connection(handshake.result.error_code, handshake.result.error_message)
				return
	for bytes in packets:
		var packet: Dictionary = _codec.decode(bytes)
		if packet.is_empty():
			_fail_connection("PROTOCOL_ERROR", _codec.last_error)
			return
		_handle_packet(packet)
		if generation != _generation:
			return
	for id in _pending.keys():
		if _pending.has(id) and now >= _pending[id].deadline:
			if id == _handshake_id or id == _heartbeat_id:
				_fail_connection("HEARTBEAT_TIMEOUT", "Gateway PONG timed out.")
				return
			_cancel_request(id, "TIMEOUT", "Gateway request timed out.")
			if generation != _generation:
				return
	if state == State.CONNECTED and _config.heartbeat_interval_ms > 0 and _heartbeat_id == 0 and now >= _next_heartbeat:
		var heartbeat = _request({"type": PacketCodec.Type.PING}, _config.heartbeat_timeout_ms, true)
		_heartbeat_id = heartbeat.request_id
		if heartbeat.is_completed:
			_fail_connection("HEARTBEAT_ERROR", "Could not send heartbeat.")


func _request(packet: Dictionary, timeout_ms: int, internal: bool = false):
	var operation = Operation.new()
	if not _can_request() and not internal:
		operation.finish(Operation.failure("NOT_CONNECTED", "Gateway client is not ready."))
		return operation
	if packet.type == PacketCodec.Type.REQUEST:
		if packet.cmd < 1 or packet.cmd > 65535 or packet.act < 1 or packet.act > 65535:
			operation.finish(Operation.failure("INVALID_ACTION", "cmd/act must be between 1 and 65535."))
			return operation
	if timeout_ms == -1:
		timeout_ms = _config.request_timeout_ms
	if timeout_ms < 1 or timeout_ms > 2147483647:
		operation.finish(Operation.failure("INVALID_TIMEOUT", "Invalid request timeout."))
		return operation
	if _pending.size() >= _config.max_pending_requests:
		operation.finish(Operation.failure("QUEUE_FULL", "Maximum pending requests reached."))
		return operation
	var id := _allocate_id()
	operation.request_id = id
	packet.request_id = id
	packet.timeout_ms = timeout_ms
	var deadline := Time.get_ticks_msec() + timeout_ms
	var error := _send(packet, deadline)
	if error != OK:
		operation.finish(Operation.failure("SEND_ERROR", error_string(error)))
		return operation
	_pending[id] = {"operation": operation, "deadline": deadline,
		"type": PacketCodec.Type.PONG if packet.type == PacketCodec.Type.PING else PacketCodec.Type.RESPONSE,
		"cmd": packet.get("cmd", 0), "act": packet.get("act", 0)}
	operation.cancellation_requested.connect(_cancel_request.bind(id, "CANCELLED", "Gateway request cancelled."))
	return operation


func _send(packet: Dictionary, deadline: int) -> Error:
	packet.session_id = session_id
	packet.user_id = user_id
	var bytes: PackedByteArray = _codec.encode(packet)
	if bytes.is_empty():
		return ERR_INVALID_DATA
	return _wire.enqueue(bytes, packet.request_id, deadline)


func _handle_packet(packet: Dictionary) -> void:
	if packet.type == PacketCodec.Type.PUSH:
		var key := PacketCodec.action_key(packet.cmd, packet.act)
		var handlers: Array = _listeners.get(key, []).duplicate()
		var generation := _generation
		for handler: Callable in handlers:
			if handler.is_valid():
				handler.call(packet)
			if generation != _generation:
				return
		push_received.emit(packet)
		return
	if packet.type not in [PacketCodec.Type.RESPONSE, PacketCodec.Type.PONG]:
		_fail_connection("PROTOCOL_ERROR", "Unexpected inbound packet type.")
		return
	if session_id > 0 and packet.session_id != session_id:
		_fail_connection("SESSION_REPLACED", "Gateway replaced the session; login must be recovered.")
		return
	if not _pending.has(packet.request_id):
		return
	var entry: Dictionary = _pending[packet.request_id]
	if packet.type != entry.type or packet.cmd != entry.cmd or packet.act != entry.act:
		_fail_connection("PROTOCOL_ERROR", "Response does not match pending request.")
		return
	_pending.erase(packet.request_id)
	var generation := _generation
	_update_session(packet.session_id, packet.user_id)
	if generation != _generation:
		entry.operation.finish(Operation.failure("DISCONNECTED", "Connection changed during response."))
		return
	entry.operation.finish(packet)
	if generation != _generation:
		return
	if packet.request_id == _handshake_id:
		_handshake_id = 0
		if not packet.ok or session_id <= 0:
			_fail_connection("HANDSHAKE_FAILED", "Gateway rejected session handshake.")
			return
		_recover_session(generation)
	elif packet.request_id == _heartbeat_id:
		_heartbeat_id = 0
		_next_heartbeat = Time.get_ticks_msec() + _config.heartbeat_interval_ms


func _recover_session(generation: int) -> void:
	_recovery_deadline = Time.get_ticks_msec() + _config.recovery_timeout_ms
	_set_state(State.RECOVERING)
	if generation != _generation:
		return
	if _connected_once and session_recovery_handler.is_valid():
		var recovered: Variant = await session_recovery_handler.call(self)
		if generation != _generation:
			return
		if recovered != true:
			_fail_connection("RECOVERY_FAILED", "Session recovery failed.")
			return
	_connected_once = true
	_retry_delay = _config.reconnect_initial_delay_ms
	_next_heartbeat = Time.get_ticks_msec() + _config.heartbeat_interval_ms
	_set_state(State.CONNECTED)
	if generation == _generation and _connect_operation != null:
		_connect_operation.finish(_session_result())


func _begin_connection() -> void:
	_generation += 1
	var generation := _generation
	_handshake_id = 0
	_heartbeat_id = 0
	_connect_deadline = Time.get_ticks_msec() + _config.connect_timeout_ms
	_wire = Transport.new()
	_set_state(State.RECONNECTING if _connected_once else State.CONNECTING)
	if generation != _generation:
		return
	_wire.start(_config, _codec)
	if not _wire.last_error.is_empty():
		_fail_connection("CONNECT_FAILED", _wire.last_error)


func _fail_connection(code: String, message: String) -> void:
	_generation += 1
	var generation := _generation
	var retry: bool = _running and _connected_once and _config.auto_reconnect
	_running = retry
	_retry_at = Time.get_ticks_msec() + _retry_delay
	_retry_delay = mini(_retry_delay * 2, _config.reconnect_max_delay_ms)
	set_process(retry)
	_cleanup(code, message, retry, State.RECONNECTING if retry else State.DISCONNECTED)
	if generation != _generation:
		return
	connection_error.emit(code, message)


func _cleanup(code: String, message: String, keep_connect: bool = false, next_state: State = State.DISCONNECTED) -> void:
	if _wire != null:
		_wire.close()
		_wire = null
	_handshake_id = 0
	_heartbeat_id = 0
	var pending := _pending.values()
	_pending.clear()
	var connect_operation = _connect_operation
	if not keep_connect:
		_connect_operation = null
	# Commit all state before emitting signals/resuming awaiters, which can reconnect.
	var session_was_changed := session_id != 0 or user_id != 0
	var state_was_changed := state != next_state
	session_id = 0
	user_id = 0
	state = next_state
	var generation := _generation
	if session_was_changed:
		session_changed.emit(0, 0)
	if state_was_changed and generation == _generation:
		state_changed.emit(next_state)
	for entry in pending:
		entry.operation.finish(Operation.failure(code, message))
	if not keep_connect and connect_operation != null:
		connect_operation.finish(Operation.failure(code, message))


func _cancel_request(id: int, code: String, message: String) -> void:
	if not _pending.has(id):
		return
	var operation = _pending[id].operation
	_pending.erase(id)
	_wire.cancel_write(id)
	if not _wire.last_error.is_empty() or id == _handshake_id or id == _heartbeat_id:
		_fail_connection(code, message)
	operation.finish(Operation.failure(code, message))


func _new_connect_operation():
	var operation = Operation.new()
	operation.cancellation_requested.connect(disconnect_from_gateway)
	return operation


func _can_request() -> bool:
	return _running and _wire != null and _wire.ready and state in [State.CONNECTED, State.RECOVERING]


func _allocate_id() -> int:
	_next_id = 1 if _next_id == 9223372036854775807 else _next_id + 1
	return _next_id


func _session_result() -> Dictionary:
	return {"ok": true, "error_code": null, "error_message": null, "transport_error": false,
		"payload": null, "session_id": session_id, "user_id": user_id}


func _update_session(new_session_id: int, new_user_id: int) -> void:
	if session_id != new_session_id or user_id != new_user_id:
		session_id = new_session_id
		user_id = new_user_id
		session_changed.emit(session_id, user_id)


func _set_state(value: State) -> void:
	if state != value:
		state = value
		state_changed.emit(state)


func _enter_tree() -> void:
	_exiting = false


func _exit_tree() -> void:
	_exiting = true
	disconnect_from_gateway()
