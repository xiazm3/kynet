extends Node
## Local self-signed TLS fixture, shared by tcps and wss certificate tests.

const Codec = preload("../addons/kynet/packet_codec.gd")

var server := TCPServer.new()
var key: CryptoKey
var certificate: X509Certificate
var websocket_mode: bool = false
var peers: Array[Dictionary] = []
var codec = Codec.new()


func start(use_websocket: bool) -> Error:
	websocket_mode = use_websocket
	var crypto := Crypto.new()
	key = crypto.generate_rsa(2048)
	certificate = crypto.generate_self_signed_certificate(key, "CN=localhost,O=Kynet,C=CN", "20200101000000", "20400101000000")
	return server.listen(0, "*")


func _process(_delta: float) -> void:
	if server.is_connection_available():
		var tcp := server.take_connection()
		var tls := StreamPeerTLS.new()
		tls.accept_stream(tcp, TLSOptions.server(key, certificate))
		peers.append({"tcp": tcp, "tls": tls, "ws": null, "buffer": PackedByteArray()})
	for entry in peers:
		entry.tcp.poll()
		if entry.tls.get_status() not in [StreamPeerTLS.STATUS_CONNECTED, StreamPeerTLS.STATUS_HANDSHAKING]:
			continue
		entry.tls.poll()
		if entry.tls.get_status() != StreamPeerTLS.STATUS_CONNECTED:
			continue
		if websocket_mode:
			if entry.ws == null:
				entry.ws = WebSocketPeer.new()
				entry.ws.accept_stream(entry.tls)
			entry.ws.poll()
			while entry.ws.get_available_packet_count() > 0:
				var data: PackedByteArray = _response(entry.ws.get_packet())
				entry.ws.put_packet(data)
		else:
			if entry.tls.get_available_bytes() > 0:
				entry.buffer.append_array(entry.tls.get_data(entry.tls.get_available_bytes())[1])
			while entry.buffer.size() >= 4:
				var stream := StreamPeerBuffer.new()
				stream.big_endian = true
				stream.data_array = entry.buffer
				var size := stream.get_32()
				if entry.buffer.size() < size + 4:
					break
				var data: PackedByteArray = _response(entry.buffer.slice(4, size + 4))
				entry.buffer = entry.buffer.slice(size + 4)
				stream.clear()
				stream.put_32(data.size())
				stream.put_data(data)
				entry.tls.put_data(stream.data_array)


func _response(data: PackedByteArray) -> PackedByteArray:
	var packet: Dictionary = codec.decode(data)
	packet.type = Codec.Type.PONG if packet.type == Codec.Type.PING else Codec.Type.RESPONSE
	packet.session_id = 42
	return codec.encode(packet)


func _exit_tree() -> void:
	for entry in peers:
		entry.tls.disconnect_from_stream()
		entry.tcp.disconnect_from_host()
	peers.clear()
	server.stop()
