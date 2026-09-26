using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace OpenKogama.Photon;

public sealed class PhotonServer : IDisposable
{
    const int TickIntervalMs = 50;
    const int TimeoutMs = 30000;

    readonly UdpClient _socket;
    readonly ConcurrentDictionary<IPEndPoint, PhotonPeer> _peers = [];
    readonly Stopwatch _clock = Stopwatch.StartNew();
    int _nextPeerId;

    public Action<PhotonPeer>? Connected;
    public Action<PhotonPeer, DisconnectReason>? Disconnected;
    public Action<PhotonPeer, OperationRequest>? Operation;
    public Action<string>? Log;

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

    async Task TickAsync(CancellationToken token)
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

    PhotonPeer Add(IPEndPoint from, int challenge)
    {
        var peer = new PhotonPeer(this, from, (short)Interlocked.Increment(ref _nextPeerId), challenge);
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
            _socket.Send(data, data.Length, to);
        }
        catch (SocketException error)
        {
            Log?.Invoke($"{to}: send failed, {error.SocketErrorCode}");
        }
    }

    public void Dispose() => _socket.Dispose();
}
