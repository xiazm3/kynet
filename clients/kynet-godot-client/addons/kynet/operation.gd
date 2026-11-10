extends RefCounted
## A request/connect handle. wait() also works after synchronous completion.

signal completed(result: Dictionary)
signal cancellation_requested

var request_id: int = 0
var is_completed: bool = false
var result: Dictionary = {}


func wait() -> Dictionary:
	if not is_completed:
		await completed
	return result


func cancel() -> void:
	if not is_completed:
		cancellation_requested.emit()


func finish(value: Dictionary) -> void:
	if is_completed:
		return
	is_completed = true
	result = value
	completed.emit(result)


static func failure(code: String, message: String) -> Dictionary:
	return {"ok": false, "error_code": code, "error_message": message,
		"transport_error": true, "payload": null, "session_id": 0, "user_id": 0}
