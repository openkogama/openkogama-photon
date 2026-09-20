using System.Net;

namespace OpenKogama.Photon;

public sealed class PhotonPeer
{
    const int ResendAfterMs = 400;
    const int MaxSendTries = 30;
    const int MaxMessageSize = 16 * 1024 * 1024;
    const int MaxFragmentGroups = 4;

    sealed class Channel
    {
        public int IncomingReliable;
        public int IncomingUnreliable;
        public int OutgoingReliable;
        public int OutgoingUnreliable;
        public readonly Dictionary<int, EnetCommand> Pending = [];
    }

    sealed class Unacked(byte[] bytes, int time)
    {
        public readonly byte[] Bytes = bytes;
        public int Time = time;
        public int Tries = 1;
    }

    readonly PhotonServer _server;
    readonly Dictionary<byte, Channel> _channels = [];
    readonly Dictionary<long, Unacked> _unacked = [];
    readonly Dictionary<int, byte[]> _fragments = [];
    readonly Dictionary<int, int> _fragmentsLeft = [];
    readonly List<byte[]> _outgoing = [];
    readonly object _sync = new();

    internal PhotonPeer(PhotonServer server, IPEndPoint endPoint, short id, int challenge)
    {
        _server = server;
        EndPoint = endPoint;
        Id = id;
        Challenge = challenge;
    }

    public IPEndPoint EndPoint { get; }
    public short Id { get; }
    public int Challenge { get; }
    public object? Tag { get; set; }
    public bool Connected { get; internal set; }

    internal int LastSeen;
    internal byte[]? VerifyCommand;

    public void Send(OperationResponse response, byte channel = 0) =>
        SendMessage(EnetProtocol.MsgOperationResponse, Protocol16.WriteOperationResponse(response), channel, true);

    public void Send(EventData data, byte channel = 0, bool reliable = true) =>
        SendMessage(EnetProtocol.MsgEvent, Protocol16.WriteEventData(data), channel, reliable);

    public void Disconnect()
    {
        lock (_sync)
        {
            int sequence = NextReliable(EnetProtocol.ControlChannel);
            Queue(EnetCommandWriter.Disconnect(sequence), EnetProtocol.ControlChannel, sequence, true);
            Flush();
        }

        _server.Remove(this, DisconnectReason.ServerClosed);
    }

    public void SendMessage(byte messageType, byte[] body, byte channel, bool reliable)
    {
        byte[] payload = new byte[2 + body.Length];
        payload[0] = EnetProtocol.PhotonMagic;
        payload[1] = messageType;
        Buffer.BlockCopy(body, 0, payload, 2, body.Length);

        lock (_sync)
        {
            Channel state = GetChannel(channel);

            if (!reliable)
            {
                state.OutgoingUnreliable++;
                Queue(EnetCommandWriter.SendUnreliable(channel, state.OutgoingReliable, state.OutgoingUnreliable, payload),
                    channel, state.OutgoingUnreliable, false);
            }
            else if (payload.Length <= EnetProtocol.MaxPayloadSize)
            {
                int sequence = NextReliable(channel);
                Queue(EnetCommandWriter.SendReliable(channel, sequence, payload), channel, sequence, true);
            }
            else
            {
                Fragment(payload, channel);
            }

            Flush();
        }
    }

    void Fragment(byte[] payload, byte channel)
    {
        int size = EnetProtocol.MaxPayloadSize;
        int count = (payload.Length + size - 1) / size;
        int start = PeekReliable(channel) + 1;

        for (int index = 0; index < count; index++)
        {
            int offset = index * size;
            int length = Math.Min(size, payload.Length - offset);
            int sequence = NextReliable(channel);

            byte[] command = EnetCommandWriter.SendFragment(channel, sequence, start, count, index,
                payload.Length, offset, payload.AsSpan(offset, length));

            Queue(command, channel, sequence, true);
        }
    }

    internal void Acknowledge(EnetCommand command, int sentTime)
    {
        lock (_sync)
            _outgoing.Add(EnetCommandWriter.Acknowledge(command.Channel, command.SequenceNumber, sentTime));
    }

    internal void SendVerifyConnect()
    {
        lock (_sync)
        {
            if (VerifyCommand is null)
            {
                int sequence = NextReliable(EnetProtocol.ControlChannel);
                VerifyCommand = EnetCommandWriter.VerifyConnect(sequence, Id);
                Queue(VerifyCommand, EnetProtocol.ControlChannel, sequence, true);
            }
            else
            {
                _outgoing.Add(VerifyCommand);
            }
        }
    }

    internal void SendInitResponse()
    {
        SendMessage(EnetProtocol.MsgInitResponse, [], 0, true);
    }

    internal void OnAcknowledge(byte channel, int sequenceNumber)
    {
        lock (_sync) _unacked.Remove(Key(channel, sequenceNumber));
    }

    internal List<byte[]> Receive(EnetCommand command)
    {
        List<byte[]> ready = [];

        lock (_sync)
        {
            Channel state = GetChannel(command.Channel);

            if (command.Type == CommandType.SendUnreliable)
            {
                if (command.UnreliableSequenceNumber > state.IncomingUnreliable)
                {
                    state.IncomingUnreliable = command.UnreliableSequenceNumber;
                    ready.Add(command.Payload);
                }
                return ready;
            }

            // already handled, the client just did not get our ack in time
            if (command.SequenceNumber <= state.IncomingReliable) return ready;
            if (!state.Pending.TryAdd(command.SequenceNumber, command)) return ready;

            while (state.Pending.Remove(state.IncomingReliable + 1, out EnetCommand? next))
            {
                state.IncomingReliable++;

                if (next.Type != CommandType.SendFragment)
                {
                    ready.Add(next.Payload);
                    continue;
                }

                byte[]? whole = Reassemble(next);
                if (whole is not null) ready.Add(whole);
            }
        }

        return ready;
    }

    byte[]? Reassemble(EnetCommand command)
    {
        if (command.TotalLength is <= 0 or > MaxMessageSize) return null;
        if (command.FragmentCount <= 0 || command.FragmentNumber >= command.FragmentCount) return null;
        if (command.FragmentOffset < 0) return null;

        if (!_fragments.TryGetValue(command.StartSequenceNumber, out byte[]? buffer))
        {
            if (_fragments.Count >= MaxFragmentGroups) return null;

            buffer = new byte[command.TotalLength];
            _fragments[command.StartSequenceNumber] = buffer;
            _fragmentsLeft[command.StartSequenceNumber] = command.FragmentCount;
        }

        if (command.FragmentOffset + command.Payload.Length > buffer.Length) return null;

        command.Payload.CopyTo(buffer, command.FragmentOffset);

        if (--_fragmentsLeft[command.StartSequenceNumber] > 0) return null;

        _fragments.Remove(command.StartSequenceNumber);
        _fragmentsLeft.Remove(command.StartSequenceNumber);
        return buffer;
    }

    internal void Tick(int now)
    {
        bool giveUp = false;

        lock (_sync)
        {
            foreach (Unacked sent in _unacked.Values)
            {
                if (now - sent.Time < ResendAfterMs) continue;

                if (++sent.Tries > MaxSendTries)
                {
                    giveUp = true;
                    break;
                }

                sent.Time = now;
                _outgoing.Add(sent.Bytes);
            }

            Flush();
        }

        if (giveUp) _server.Remove(this, DisconnectReason.Timeout);
    }

    internal void Flush()
    {
        lock (_sync)
        {
            if (_outgoing.Count == 0) return;

            List<byte[]> batch = [];
            int size = EnetProtocol.HeaderSize;

            foreach (byte[] command in _outgoing)
            {
                if (batch.Count > 0 && size + command.Length > EnetProtocol.Mtu)
                {
                    SendPacket(batch);
                    batch = [];
                    size = EnetProtocol.HeaderSize;
                }

                batch.Add(command);
                size += command.Length;
            }

            SendPacket(batch);
            _outgoing.Clear();
        }
    }

    void SendPacket(List<byte[]> commands)
    {
        if (commands.Count == 0) return;
        _server.SendRaw(EndPoint, EnetPacket.Build(Id, _server.Now, Challenge, commands));
    }

    void Queue(byte[] command, byte channel, int sequenceNumber, bool reliable)
    {
        _outgoing.Add(command);
        if (reliable) _unacked[Key(channel, sequenceNumber)] = new Unacked(command, _server.Now);
    }

    Channel GetChannel(byte channel)
    {
        if (!_channels.TryGetValue(channel, out Channel? state))
        {
            state = new Channel();
            _channels[channel] = state;
        }
        return state;
    }

    int NextReliable(byte channel) => ++GetChannel(channel).OutgoingReliable;

    int PeekReliable(byte channel) => GetChannel(channel).OutgoingReliable;

    static long Key(byte channel, int sequenceNumber) => ((long)channel << 32) | (uint)sequenceNumber;

    public override string ToString() => $"peer {Id} {EndPoint}";
}
