extends SceneTree

const Client = preload("../../addons/kynet/client.gd")
const Protobuf = preload("../../addons/kynet/protobuf.gd")

var endpoint: String
var welcome: String = ""
var broadcasts: int = 0
var checks: int = 0


func _initialize() -> void:
	_run.call_deferred()


func _client():
	var client = Client.new()
	client.options.endpoint = endpoint
	client.options.heartbeat_interval_ms = 100
	client.options.heartbeat_timeout_ms = 1000
	root.add_child(client)
	return client


func _check(condition: bool, message: String) -> bool:
	checks += 1
	if not condition:
		push_error("INTEROP FAIL: " + message)
		quit(1)
	return condition


func _until(condition: Callable, timeout_ms: int = 4000) -> bool:
	var deadline := Time.get_ticks_msec() + timeout_ms
	while not condition.call() and Time.get_ticks_msec() < deadline:
		await process_frame
	return condition.call()


func _run() -> void:
	var arguments := OS.get_cmdline_user_args()
	if arguments.is_empty():
		push_error("Pass an endpoint after --.")
		quit(1)
		return
	endpoint = arguments[0]
	create_timer(30).timeout.connect(func(): push_error("Interop deadline exceeded."); quit(2))
	var client = _client()
	client.subscribe(5, 6, func(packet):
		var result := Protobuf.string_value(packet.payload)
		if result.ok: welcome = result.value)
	client.subscribe_message(200001, func(_packet): broadcasts += 1)
	var connection: Dictionary = await client.connect_async()
	if not _check(connection.ok and client.session_id > 0, "session handshake"): return
	var denied: Dictionary = await client.request_async(1, 2)
	if not _check(denied.error_code == "UNAUTHORIZED" and not denied.transport_error, "protected action requires login"): return
	var invalid: Dictionary = await client.request_async(1, 1, Protobuf.login_message("godot", "invalid"))
	if not _check(invalid.error_code == "LOGIN_FAILED", "business error preserves code"): return
	var credentials := Protobuf.login_message("godot", "123456")
	var login: Dictionary = await client.request_async(1, 1, credentials)
	if not _check(login.ok and client.user_id == 10001, "login updates user id"): return
	if not _check(Protobuf.string_value(login.payload, 3).value == "token-godot", "Protobuf login response"): return
	if not _check(await _until(func(): return welcome == "welcome godot"), "StringValue welcome push"): return
	var hello: Dictionary = await client.request_async(1, 2)
	if not _check(hello.ok and hello.payload.get_string_from_utf8().contains("attachment.username=godot"), "server attachment continuity"): return
	var broadcast: Dictionary = await client.request_async(1, 5)
	if not _check(broadcast.ok and await _until(func(): return broadcasts >= 6), "six broadcasts"): return
	if not _check(client.send_one_way(1, 3, credentials) == OK, "one-way action"): return
	var other = _client()
	if not _check((await other.connect_async()).ok, "second connection"): return
	if not _check((await other.request_async(1, 1, credentials)).ok, "second login"): return
	if not _check(await _until(func(): return client.state == Client.State.DISCONNECTED), "kick closes old connection (UDP detected by PONG)"): return
	if not _check(client.session_id == 0 and client.user_id == 0, "kick clears local session"): return
	if not _check((await other.request_async(1, 4)).ok, "logout"): return
	var after_logout: Dictionary = await other.request_async(1, 2)
	if not _check(after_logout.error_code == "UNAUTHORIZED" and other.user_id == 0, "logout clears authentication"): return
	client.disconnect_from_gateway()
	other.disconnect_from_gateway()
	client.queue_free()
	other.queue_free()
	await process_frame
	print("INTEROP PASS %s (%d checks)" % [endpoint, checks])
	quit(0)
