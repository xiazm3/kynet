extends SceneTree

const Client = preload("../../addons/kynet/client.gd")
const Protobuf = preload("../../addons/kynet/protobuf.gd")
const Messages = preload("../../addons/kynet/generated/kynet_messages.gd")
const SignProxy = preload("../../addons/kynet/generated/sign_action.gd")

func _initialize() -> void:
	_run.call_deferred()


func _check(condition: bool, message: String) -> bool:
	if not condition:
		push_error("CODEGEN FAIL: " + message)
		quit(1)
	return condition


func _run() -> void:
	var arguments := OS.get_cmdline_user_args()
	if arguments.is_empty():
		push_error("Pass an endpoint after --.")
		quit(1)
		return
	create_timer(20).timeout.connect(func(): push_error("Codegen deadline exceeded."); quit(2))
	var integers := Protobuf.varint_field(1, 1)
	integers.append_array(Protobuf.varint_field(3, -1))
	var decoded: Dictionary = Messages.decode_list_int(integers)
	if not _check(decoded.ok and decoded.value == [-1], "int list signed value"): return
	var longs := Protobuf.varint_field(1, 2)
	longs.append_array(Protobuf.varint_field(4, 9007199254740993))
	decoded = Messages.decode_list_long(longs)
	if not _check(decoded.ok and decoded.value == [9007199254740993], "long list exact value"): return
	var booleans := Protobuf.varint_field(1, 3)
	booleans.append_array(Protobuf.varint_field(5, 0))
	booleans.append_array(Protobuf.varint_field(5, 1))
	decoded = Messages.decode_list_bool(booleans)
	if not _check(decoded.ok and decoded.value == [false, true], "bool list values"): return
	var client = Client.new()
	client.options.endpoint = arguments[0]
	root.add_child(client)
	if not _check((await client.connect_async()).ok, "connect"): return

	var response: Dictionary = await SignProxy.sign(client, {"ticket": "abc"})
	if not _check(response.ok, "sign ok"): return
	if not _check(response.data.day == 3 and response.data.bonus.ends_with("abc"), "sign data"): return
	var next_day: Dictionary = await SignProxy.next_day(client, 2)
	if not _check(next_day.ok and next_day.data == 3, "next_day"): return
	var rewards: Dictionary = await SignProxy.rewards(client, "kynet")
	if not _check(rewards.ok and rewards.data.size() == 2 and rewards.data[0].begins_with("kynet-"), "rewards"): return

	client.disconnect_from_gateway()
	client.queue_free()
	await process_frame
	print("CODEGEN GDSCRIPT SMOKE PASS ", arguments[0])
	quit(0)
