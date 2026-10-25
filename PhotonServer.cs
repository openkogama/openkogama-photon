using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;

namespace OpenKogama.Photon;

public sealed class PhotonServer : IDisposable
{
    const int TickIntervalMs = 50;
    const int TimeoutMs = 30000;
    static readonly byte[] Probe = "OKUDP?"u8.ToArray();

    readonly UdpClient? _socket;
    readonly ConcurrentDictionary<IPEndPoint, PhotonPeer> _peers = [];
    readonly Stopwatch _clock = Stopwatch.StartNew();
    int _nextPeerId;

    public Action<PhotonPeer>? Connected;
    public Action<PhotonPeer, DisconnectReason>? Disconnected;
    public Action<PhotonPeer, OperationRequest>? Operation;
    public Action<string>? Log;

    public PhotonServer()
    {
    }

    public PhotonServer(int port)
    {
        _socket = new UdpClient(port);

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            _socket.Client.IOControl(unchecked((int)0x9800000C), [0, 0, 0, 0], null);
    }

    public ICollection<PhotonPeer> Peers => _peers.Values;

    public int Now =>(int)_clock.ElapsedMilliseconds;

    public async Task RunAsync(CancellationToken token = default)
    {
        if (_socket is null) throw new InvalidOperationException("no udp socket");
        Task ticks = TickAsync(token);

        while (!token.IsCancellationRequested)
        {
            UdpReceiveResult result;

            try
            {
                result = await _socket.ReceiveAsync(token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException error)
            {
                Log?.Invoke($"receive: {error.SocketErrorCode}");
                continue;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            if (result.Buffer.AsSpan().SequenceEqual(Probe))
            {
                SendRaw(result.RemoteEndPoint, Probe);
                continue;
            }

            try
            {
                Handle(result.RemoteEndPoint, result.Buffer);
            }
            catch (Exception error)
            {
                Log?.Invoke($"{result.RemoteEndPoint}: {error}");
            }
        }

        await ticks;
    }

    public async Task TickAsync(CancellationToken token = default)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(TickIntervalMs, token);

                int now = Now;

                foreach (PhotonPeer peer in _peers.Values)
                {
                    if (now - peer.LastSeen > TimeoutMs)
                    {
                        Remove(peer, DisconnectReason.Timeout);
                        continue;
                    }

                    peer.Tick(now);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    void Handle(IPEndPoint from, byte[] data)
    {
        EnetPacket? packet = EnetPacket.Parse(data);
        if (packet is null) return;

        _peers.TryGetValue(from, out PhotonPeer? peer);

        if (peer is not null && packet.Challenge != peer.Challenge)
        {
            if (!packet.Commands.Any(c => c.Type == CommandType.Connect)) return;

            Remove(peer, DisconnectReason.ClientLeft);
            peer = null;
        }

        foreach (EnetCommand command in packet.Commands)
        {
            if (peer is null || !peer.Connected)
                Log?.Invoke($"{from}: handshake {command.Type} ch{command.Channel} seq {command.SequenceNumber} challenge {packet.Challenge} time {packet.SentTime}");

            if (command.Type == CommandType.Connect)
            {
                peer ??= Add(from, packet.Challenge);
                peer.LastSeen = Now;
                peer.Acknowledge(command, packet.SentTime);
                peer.SendVerifyConnect();
                continue;
            }

            if (peer is null) continue;

            peer.LastSeen = Now;
            if (command.IsReliable) peer.Acknowledge(command, packet.SentTime);

            switch (command.Type)
            {
                case CommandType.Acknowledge:
                    peer.OnAcknowledge(command.Channel, command.AckedSequenceNumber);
                    break;

                case CommandType.Disconnect:
                    Remove(peer, DisconnectReason.ClientLeft);
                    peer = null;
                    break;

                case CommandType.SendReliable:
                case CommandType.SendUnreliable:
                case CommandType.SendFragment:
                    foreach (byte[] payload in peer.Receive(command)) Message(peer, payload);
                    break;
            }
        }

        peer?.Flush();
    }

    void Message(PhotonPeer peer, byte[] payload)
    {
        bool protocol15 = payload.Length >= EnetProtocol.Header15Size && payload[0] == EnetProtocol.PhotonMagic15;
        if (!protocol15 && (payload.Length < 2 || payload[0] != EnetProtocol.PhotonMagic)) return;

        byte messageType = (byte)(payload[protocol15 ? 5 : 1] & 0x7F);

        switch (messageType)
        {
            case EnetProtocol.MsgInitRequest:
                if (!peer.Connected) peer.Protocol = protocol15 ? PhotonProtocol.Protocol15 : PhotonProtocol.Protocol16;
                peer.SendInitResponse();
                if (peer.Connected) break;
                peer.Connected = true;
                Connected?.Invoke(peer);
                break;

            case EnetProtocol.MsgOperationRequest:
                Operation?.Invoke(peer, protocol15 ? Protocol15.ReadOperationRequest(payload) : Protocol16.ReadOperationRequest(payload, 2));
                break;

            default:
                Log?.Invoke($"{peer}: unhandled message type {messageType}");
                break;
        }
    }

    const string WebSocketProtocol = "GpBinaryV16";
    const byte PingCode = 1;
    const byte PingClientTime = 1;
    const byte PingServerTime = 2;

    const byte TcpFrame = 0xFB;
    const byte TcpPing = 0xF0;
    const int TcpHeader = 7;
    const int TcpPingRequest = 5;
    const int TcpPong = 9;
    const int MaxTcpMessage = 8 * 1024 * 1024;
    const int MaxHandshake = 8 * 1024;
    const int HandshakeTimeoutMs = 10000;
    const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    static readonly byte[] HeadersEnd = "\r\n\r\n"u8.ToArray();
    static readonly byte[] BadRequest = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"u8.ToArray();

    public async Task RunTcpAsync(IPEndPoint endpoint, CancellationToken token = default)
    {
        var listener = new TcpListener(endpoint);
        listener.Start();
        token.Register(listener.Stop);

        while (!token.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync(token);
            }
            catch (Exception error) when (error is SocketException or ObjectDisposedException or OperationCanceledException)
            {
                break;
            }
            _ = ServeTcpAsync(client, token);
        }
    }

    async Task ServeTcpAsync(TcpClient client, CancellationToken token)
    {
        client.NoDelay = true;
        var from = (IPEndPoint)client.Client.RemoteEndPoint!;
        NetworkStream stream = client.GetStream();
        var first = new byte[1];
        try
        {
            using (var handshake = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                handshake.CancelAfter(HandshakeTimeoutMs);
                await stream.ReadExactlyAsync(first, handshake.Token);
                if (first[0] is not (TcpFrame or TcpPing))
                {
                    WebSocket? socket = await UpgradeAsync(stream, first[0], handshake.Token);
                    if (socket is null)
                    {
                        await stream.WriteAsync(BadRequest, token);
                        return;
                    }
                    await ServeWebSocketAsync(socket, from, token);
                    return;
                }
            }
            await ServePhotonTcpAsync(stream, from, first[0], token);
        }
        catch (Exception error) when (error is IOException or SocketException or EndOfStreamException or OperationCanceledException or ObjectDisposedException)
        {
        }
        finally
        {
            client.Dispose();
        }
    }

    static async Task<WebSocket?> UpgradeAsync(NetworkStream stream, byte first, CancellationToken token)
    {
        var request = new List<byte>(1024) { first };
        var next = new byte[1];
        while (request.Count < MaxHandshake && !CollectionsMarshal.AsSpan(request).EndsWith(HeadersEnd))
        {
            await stream.ReadExactlyAsync(next, token);
            request.Add(next[0]);
        }
        string[] lines = Encoding.ASCII.GetString(CollectionsMarshal.AsSpan(request)).Split("\r\n");
        if (!lines[0].StartsWith("GET ", StringComparison.Ordinal)) return null;
        Dictionary<string, string> headers = lines.Skip(1).Select(line => line.Split(':', 2)).Where(pair => pair.Length == 2)
            .GroupBy(pair => pair[0].Trim(), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Last()[1].Trim(), StringComparer.OrdinalIgnoreCase);
        if (!headers.TryGetValue("Upgrade", out string? upgrade) || !upgrade.Equals("websocket", StringComparison.OrdinalIgnoreCase)
            || !headers.TryGetValue("Sec-WebSocket-Key", out string? key) || key.Length == 0)
            return null;

        string accept = Convert.ToBase64String(SHA1.HashData(Encoding.ASCII.GetBytes(key + WebSocketGuid)));
        string? protocol = headers.GetValueOrDefault("Sec-WebSocket-Protocol")?.Split(',').Select(name => name.Trim()).FirstOrDefault(name => name == WebSocketProtocol);
        string response = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            + $"Sec-WebSocket-Accept: {accept}\r\n" + (protocol is null ? "" : $"Sec-WebSocket-Protocol: {protocol}\r\n") + "\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(response), token);
        return WebSocket.CreateFromStream(stream, new WebSocketCreationOptions { IsServer = true, SubProtocol = protocol });
    }

    async Task ServeWebSocketAsync(WebSocket socket, IPEndPoint from, CancellationToken token)
    {
        Channel<byte[]> queue = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
        PhotonPeer peer = Accept(from, data => queue.Writer.TryWrite(data), () =>
        {
            queue.Writer.TryComplete();
            _ = socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }, PhotonTransport.WebSocket);
        Task pump = PumpAsync(socket, queue.Reader, token);

        var buffer = new byte[64 * 1024];
        var message = new MemoryStream();
        try
        {
            while (socket.State == WebSocketState.Open && !token.IsCancellationRequested)
            {
                WebSocketReceiveResult result = await socket.ReceiveAsync(buffer, token);
                if (result.MessageType == WebSocketMessageType.Close) break;
                message.Write(buffer, 0, result.Count);
                if (!result.EndOfMessage) continue;

                byte[] payload = message.ToArray();
                message.SetLength(0);
                Receive(peer, payload);
            }
        }
        catch (Exception error) when (error is WebSocketException or OperationCanceledException or IOException)
        {
        }

        Remove(peer, DisconnectReason.ClientLeft);
        queue.Writer.TryComplete();
        await pump;
        try
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }
        catch (Exception error) when (error is WebSocketException or IOException or ObjectDisposedException)
        {
        }
        socket.Dispose();
    }

    static async Task PumpAsync(WebSocket socket, ChannelReader<byte[]> queue, CancellationToken token)
    {
        try
        {
            await foreach (byte[] message in queue.ReadAllAsync(token))
                await socket.SendAsync(message, WebSocketMessageType.Binary, true, token);
        }
        catch (Exception error) when (error is WebSocketException or OperationCanceledException or ObjectDisposedException or IOException)
        {
        }
    }

    async Task ServePhotonTcpAsync(NetworkStream stream, IPEndPoint from, byte first, CancellationToken token)
    {
        Channel<byte[]> queue = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
        PhotonPeer peer = Accept(from, data => queue.Writer.TryWrite(Framed(data)), () =>
        {
            queue.Writer.TryComplete();
            stream.Close();
        }, PhotonTransport.Tcp);
        Task pump = PumpTcpAsync(stream, queue.Reader, token);

        var header = new byte[TcpHeader];
        header[0] = first;
        bool pending = true;
        try
        {
            while (!token.IsCancellationRequested)
            {
                if (!pending) await stream.ReadExactlyAsync(header.AsMemory(0, 1), token);
                pending = false;
                if (header[0] == TcpPing)
                {
                    await stream.ReadExactlyAsync(header.AsMemory(1, TcpPingRequest - 1), token);
                    peer.LastSeen = Now;
                    queue.Writer.TryWrite(Pong(BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1))));
                    continue;
                }
                if (header[0] != TcpFrame) break;
                await stream.ReadExactlyAsync(header.AsMemory(1), token);
                int length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
                if (length <= TcpHeader || length > MaxTcpMessage) break;
                var payload = new byte[length - TcpHeader];
                await stream.ReadExactlyAsync(payload, token);
                Receive(peer, payload);
            }
        }
        catch (Exception error) when (error is IOException or SocketException or EndOfStreamException or OperationCanceledException or ObjectDisposedException)
        {
        }

        Remove(peer, DisconnectReason.ClientLeft);
        queue.Writer.TryComplete();
        await pump;
    }

    public PhotonPeer Accept(IPEndPoint from, Action<byte[]> send, Action? close = null, PhotonTransport transport = PhotonTransport.WebSocket)
    {
        PhotonPeer peer = Add(from, 0);
        peer.Sender = send;
        peer.Closer = close;
        peer.Transport = transport;
        peer.Protocol = PhotonProtocol.Protocol16;
        peer.LastSeen = Now;
        peer.Connected = true;
        if (transport == PhotonTransport.WebSocket) peer.SendInitResponse();
        Connected?.Invoke(peer);
        return peer;
    }

    static byte[] Framed(byte[] payload)
    {
        var frame = new byte[TcpHeader + payload.Length];
        frame[0] = TcpFrame;
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), frame.Length);
        frame[6] = 1;
        payload.CopyTo(frame, TcpHeader);
        return frame;
    }

    byte[] Pong(int clientTime)
    {
        var pong = new byte[TcpPong];
        pong[0] = TcpPing;
        BinaryPrimitives.WriteInt32BigEndian(pong.AsSpan(1), Now);
        BinaryPrimitives.WriteInt32BigEndian(pong.AsSpan(5), clientTime);
        return pong;
    }

    static async Task PumpTcpAsync(NetworkStream stream, ChannelReader<byte[]> queue, CancellationToken token)
    {
        try
        {
            await foreach (byte[] message in queue.ReadAllAsync(token))
                await stream.WriteAsync(message, token);
        }
        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
        {
        }
    }

    public void Receive(PhotonPeer peer, byte[] payload)
    {
        peer.LastSeen = Now;
        try
        {
            WebSocketMessage(peer, payload);
        }
        catch (Exception error)
        {
            Log?.Invoke($"{peer}: {error}");
        }
    }

    public void Close(PhotonPeer peer) => Remove(peer, DisconnectReason.ClientLeft);

    void WebSocketMessage(PhotonPeer peer, byte[] payload)
    {
        if (payload.Length < 2 || payload[0] != EnetProtocol.PhotonMagic)
        {
            Log?.Invoke($"{peer}: websocket message without photon magic, {Convert.ToHexString(payload, 0, Math.Min(payload.Length, 16))}");
            return;
        }
        byte messageType = (byte)(payload[1] & 0x7F);

        switch (messageType)
        {
            case EnetProtocol.MsgInitRequest:
                peer.SendInitResponse();
                break;

            case EnetProtocol.MsgOperationRequest:
                Operation?.Invoke(peer, Protocol16.ReadOperationRequest(payload, 2));
                break;

            case EnetProtocol.MsgInternalOperationRequest:
                OperationRequest ping = Protocol16.ReadOperationRequest(payload, 2);
                if (ping.OperationCode != PingCode) break;
                var pong = new OperationResponse(ping);
                pong.Parameters[PingClientTime] = ping[PingClientTime];
                pong.Parameters[PingServerTime] = Now;
                peer.SendMessage(EnetProtocol.MsgInternalOperationResponse, Protocol16.WriteOperationResponse(pong), 0, true);
                break;

            default:
                Log?.Invoke($"{peer}: unhandled websocket message type {messageType}");
                break;
        }
    }

    PhotonPeer Add(IPEndPoint from, int challenge)
    {
        var peer = new PhotonPeer(this, from, (short)Interlocked.Increment(ref _nextPeerId), challenge) { LastSeen = Now };
        _peers[from] = peer;
        Log?.Invoke($"{peer}: connect");
        return peer;
    }

    internal void Remove(PhotonPeer peer, DisconnectReason reason)
    {
        if (!_peers.TryRemove(peer.EndPoint, out _)) return;

        Log?.Invoke($"{peer}: {reason}");
        peer.Connected = false;
        Disconnected?.Invoke(peer, reason);
    }

    internal void SendRaw(IPEndPoint to, byte[] data)
    {
        try
        {
            _socket?.Send(data, data.Length, to);
        }
        catch (SocketException error)
        {
            Log?.Invoke($"{to}: send failed, {error.SocketErrorCode}");
        }
    }

    public void Dispose() => _socket?.Dispose();
}
