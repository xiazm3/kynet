extends RefCounted
## All durations are milliseconds. Configure before connect_to_gateway().

var endpoint: String = "tcp://127.0.0.1:30001"
var connect_timeout_ms: int = 5000
var recovery_timeout_ms: int = 10000
var request_timeout_ms: int = 3000
var heartbeat_interval_ms: int = 5000
var heartbeat_timeout_ms: int = 3000
var auto_reconnect: bool = false
var reconnect_initial_delay_ms: int = 1000
var reconnect_max_delay_ms: int = 15000
var max_pending_requests: int = 1024
var max_pending_writes: int = 256
var max_frame_length: int = 1024 * 1024
var max_payload_length: int = 512 * 1024
var max_string_length: int = 8 * 1024
var max_packets_per_poll: int = 128
# Optional custom CA; TLSOptions.client() uses Godot's trusted CA store by default.
var tls_options: TLSOptions


func snapshot():
	var copy = get_script().new()
	for property in get_property_list():
		if property.usage & PROPERTY_USAGE_SCRIPT_VARIABLE:
			copy.set(property.name, get(property.name))
	return copy


func validate() -> String:
	for value in [connect_timeout_ms, recovery_timeout_ms, request_timeout_ms, heartbeat_timeout_ms,
			reconnect_initial_delay_ms, reconnect_max_delay_ms]:
		if value < 1 or value > 2147483647:
			return "Timeouts must be between 1 and 2147483647 ms."
	if heartbeat_interval_ms < 0 or heartbeat_interval_ms > 2147483647:
		return "Invalid heartbeat interval."
	if reconnect_initial_delay_ms > reconnect_max_delay_ms:
		return "Invalid reconnect delay range."
	if max_pending_requests < 1 or max_pending_writes < 1 or max_packets_per_poll < 1:
		return "Queue limits must be positive."
	if max_frame_length < 64 or max_payload_length < 0 or max_string_length < 0:
		return "Invalid packet size limits."
	if parse_endpoint(endpoint).is_empty():
		return "Use tcp/tcps/udp/ws/wss://host:port (IPv6 hosts require brackets)."
	return ""


static func parse_endpoint(value: String) -> Dictionary:
	var regex := RegEx.new()
	regex.compile("^(tcp|tcps|udp|ws|wss)://(\\[[0-9a-fA-F:%.]+\\]|[^:/?#@\\s]+):([0-9]+)([^#\\s]*)$")
	var match_result := regex.search(value)
	if match_result == null:
		return {}
	var scheme := match_result.get_string(1)
	var host := match_result.get_string(2).trim_prefix("[").trim_suffix("]")
	var port := match_result.get_string(3).to_int()
	var suffix := match_result.get_string(4)
	if port < 1 or port > 65535:
		return {}
	if scheme in ["tcp", "tcps", "udp"] and suffix not in ["", "/"]:
		return {}
	if not suffix.is_empty() and not suffix.begins_with("/") and not suffix.begins_with("?"):
		return {}
	return {"scheme": scheme, "host": host, "port": port}
