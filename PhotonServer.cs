using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace OpenKogama.Photon;

public sealed class PhotonServer : IDisposable
{
    const int TickIntervalMs = 50;
    const int TimeoutMs = 30000;

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

    public async Task RunWebSocketsAsync(string prefix, CancellationToken token = default)
    {
        var listener = new HttpListener();
        listener.Prefixes.Add(prefix);
        listener.Start();
        token.Register(listener.Stop);

        while (!token.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await listener.GetContextAsync();
            }
            catch (Exception error) when (error is HttpListenerException or ObjectDisposedException)
            {
                break;
            }

            if (!context.Request.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                context.Response.Close();
                continue;
            }

            _ = ServeWebSocketAsync(context, token);
        }
    }

    async Task ServeWebSocketAsync(HttpListenerContext context, CancellationToken token)
    {
        string? protocol = context.Request.Headers["Sec-WebSocket-Protocol"]?.Split(',').Select(p => p.Trim())
            .FirstOrDefault(p => p == WebSocketProtocol);
        HttpListenerWebSocketContext socketContext;
        try
        {
            socketContext = await context.AcceptWebSocketAsync(protocol);
        }
        catch (Exception error)
        {
            Log?.Invoke($"{context.Request.RemoteEndPoint}: websocket upgrade failed, {error.Message}");
            return;
        }

        WebSocket socket = socketContext.WebSocket;
        Channel<byte[]> queue = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
        PhotonPeer peer = Accept(context.Request.RemoteEndPoint, data => queue.Writer.TryWrite(data), () =>
        {
            queue.Writer.TryComplete();
            _ = socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        });
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
        catch (Exception error) when (error is WebSocketException or OperationCanceledException)
        {
        }

        Remove(peer, DisconnectReason.ClientLeft);
        socket.Dispose();
        await pump;
    }

    static async Task PumpAsync(WebSocket socket, ChannelReader<byte[]> queue, CancellationToken token)
    {
        try
        {
            await foreach (byte[] message in queue.ReadAllAsync(token))
                await socket.SendAsync(message, WebSocketMessageType.Binary, true, token);
        }
        catch (Exception error) when (error is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
        }
    }

    public PhotonPeer Accept(IPEndPoint from, Action<byte[]> send, Action? close = null)
    {
        PhotonPeer peer = Add(from, 0);
        peer.Sender = send;
        peer.Closer = close;
        peer.Protocol = PhotonProtocol.Protocol16;
        peer.LastSeen = Now;
        peer.Connected = true;
        peer.SendInitResponse();
        Connected?.Invoke(peer);
        return peer;
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
