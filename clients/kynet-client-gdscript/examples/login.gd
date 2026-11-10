extends Node
## Attach to a Node, then run against the repository's gateway/login services.

const Client = preload("../addons/kynet/client.gd")
const Protobuf = preload("../addons/kynet/protobuf.gd")

var client = Client.new()


func _ready() -> void:
	add_child(client)
	client.subscribe(5, 6, _on_welcome)
	client.connection_error.connect(func(code, message): print(code, ": ", message))
	var connected: Dictionary = await client.connect_async("tcp://127.0.0.1:30001")
	if not connected.ok:
		push_error(connected.error_message)
		return
	var login: Dictionary = await client.request_async(1, 1, Protobuf.login_message("godot", "123456"))
	if not login.ok:
		push_error(str(login.error_code, ": ", login.error_message))
		return
	var token := Protobuf.string_value(login.payload, 3)
	print("user_id=", client.user_id, " token=", token.value)
	var hello: Dictionary = await client.request_async(1, 2)
	if hello.ok:
		print(hello.payload.get_string_from_utf8())
	await client.request_async(1, 4)
	client.disconnect_from_gateway()


func _on_welcome(packet: Dictionary) -> void:
	var welcome := Protobuf.string_value(packet.payload)
	if welcome.ok:
		print(welcome.value)
