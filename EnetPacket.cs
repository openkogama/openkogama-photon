using System.Buffers.Binary;

namespace OpenKogama.Photon;

public sealed class EnetPacket
{
    public short PeerId;
    public int SentTime;
    public int Challenge;
    public List<EnetCommand> Commands = [];

    public static EnetPacket? Parse(byte[] data)
    {
        if (data.Length < EnetProtocol.HeaderSize) return null;
        if (data[EnetProtocol.OffsetCrcFlag] != 0) return null;   // this client never enables CRC

        var packet = new EnetPacket
        {
            PeerId = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(EnetProtocol.OffsetPeerId)),
            SentTime = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(EnetProtocol.OffsetSentTime)),
            Challenge = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(EnetProtocol.OffsetChallenge)),
        };

        int count = data[EnetProtocol.OffsetCommandCount];
        int offset = EnetProtocol.HeaderSize;

        for (int i = 0; i < count; i++)
        {
            if (offset + EnetProtocol.CmdHeaderSize > data.Length) return null;

            int length = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset + EnetProtocol.CmdLength));
            if (length < EnetProtocol.CmdHeaderSize || offset + length > data.Length) return null;

            var command = new EnetCommand
            {
                Type = (CommandType)data[offset + EnetProtocol.CmdType],
                Channel = data[offset + EnetProtocol.CmdChannel],
                Flags = data[offset + EnetProtocol.CmdFlags],
                SequenceNumber = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset + EnetProtocol.CmdSequence)),
            };

            int body = offset + EnetProtocol.CmdHeaderSize;
            int end = offset + length;

            switch (command.Type)
            {
                case CommandType.Acknowledge:
                    if (length < EnetProtocol.AckSize) return null;
                    command.AckedSequenceNumber = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body));
                    command.AckedSentTime = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body + 4));
                    break;

                case CommandType.SendReliable:
                    command.Payload = data[body..end];
                    break;

                case CommandType.SendUnreliable:
                    if (length < EnetProtocol.UnreliableHeaderSize) return null;
                    command.UnreliableSequenceNumber = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body));
                    command.Payload = data[(offset + EnetProtocol.UnreliableHeaderSize)..end];
                    break;

                case CommandType.SendFragment:
                    if (length < EnetProtocol.FragmentHeaderSize) return null;
                    command.StartSequenceNumber = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body));
                    command.FragmentCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body + 4));
                    command.FragmentNumber = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body + 8));
                    command.TotalLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body + 12));
                    command.FragmentOffset = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(body + 16));
                    command.Payload = data[(offset + EnetProtocol.FragmentHeaderSize)..end];
                    break;
            }

            packet.Commands.Add(command);
            offset += length;
        }

        return packet;
    }

    public static byte[] Build(short peerId, int sentTime, int challenge, List<byte[]> commands)
    {
        int size = EnetProtocol.HeaderSize;
        foreach (byte[] command in commands) size += command.Length;

        byte[] data = new byte[size];
        BinaryPrimitives.WriteInt16BigEndian(data.AsSpan(EnetProtocol.OffsetPeerId), peerId);
        data[EnetProtocol.OffsetCommandCount] = (byte)commands.Count;
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(EnetProtocol.OffsetSentTime), sentTime);
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(EnetProtocol.OffsetChallenge), challenge);

        int offset = EnetProtocol.HeaderSize;
        foreach (byte[] command in commands)
        {
            Buffer.BlockCopy(command, 0, data, offset, command.Length);
            offset += command.Length;
        }

        return data;
    }
}

public static class EnetCommandWriter
{
    static byte[] NewCommand(CommandType type, byte channel, byte flags, int size, int sequenceNumber)
    {
        byte[] command = new byte[size];
        command[EnetProtocol.CmdType] = (byte)type;
        command[EnetProtocol.CmdChannel] = channel;
        command[EnetProtocol.CmdFlags] = flags;
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(EnetProtocol.CmdLength), size);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(EnetProtocol.CmdSequence), sequenceNumber);
        return command;
    }

    public static byte[] Acknowledge(byte channel, int ackedSequenceNumber, int ackedSentTime)
    {
        byte[] command = NewCommand(CommandType.Acknowledge, channel, 0, EnetProtocol.AckSize, 0);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(EnetProtocol.CmdHeaderSize), ackedSequenceNumber);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(EnetProtocol.CmdHeaderSize + 4), ackedSentTime);
        return command;
    }

    public static byte[] VerifyConnect(int sequenceNumber, short peerId)
    {
        byte[] command = NewCommand(CommandType.VerifyConnect, EnetProtocol.ControlChannel,
            EnetProtocol.FlagReliable, EnetProtocol.VerifyConnectSize, sequenceNumber);
        BinaryPrimitives.WriteInt16BigEndian(command.AsSpan(EnetProtocol.CmdHeaderSize), peerId);
        return command;
    }

    public static byte[] Disconnect(int sequenceNumber) =>
        NewCommand(CommandType.Disconnect, EnetProtocol.ControlChannel,
            EnetProtocol.FlagReliable, EnetProtocol.DisconnectSize, sequenceNumber);

    public static byte[] SendReliable(byte channel, int sequenceNumber, byte[] payload)
    {
        byte[] command = NewCommand(CommandType.SendReliable, channel, EnetProtocol.FlagReliable,
            EnetProtocol.CmdHeaderSize + payload.Length, sequenceNumber);
        Buffer.BlockCopy(payload, 0, command, EnetProtocol.CmdHeaderSize, payload.Length);
        return command;
    }

    public static byte[] SendUnreliable(byte channel, int sequenceNumber, int unreliableSequenceNumber, byte[] payload)
    {
        byte[] command = NewCommand(CommandType.SendUnreliable, channel, 0,
            EnetProtocol.UnreliableHeaderSize + payload.Length, sequenceNumber);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(EnetProtocol.CmdHeaderSize), unreliableSequenceNumber);
        Buffer.BlockCopy(payload, 0, command, EnetProtocol.UnreliableHeaderSize, payload.Length);
        return command;
    }

    public static byte[] SendFragment(byte channel, int sequenceNumber, int startSequenceNumber,
        int fragmentCount, int fragmentNumber, int totalLength, int fragmentOffset, ReadOnlySpan<byte> payload)
    {
        byte[] command = NewCommand(CommandType.SendFragment, channel, EnetProtocol.FlagReliable,
            EnetProtocol.FragmentHeaderSize + payload.Length, sequenceNumber);

        int offset = EnetProtocol.CmdHeaderSize;
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(offset), startSequenceNumber);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(offset + 4), fragmentCount);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(offset + 8), fragmentNumber);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(offset + 12), totalLength);
        BinaryPrimitives.WriteInt32BigEndian(command.AsSpan(offset + 16), fragmentOffset);

        payload.CopyTo(command.AsSpan(EnetProtocol.FragmentHeaderSize));
        return command;
    }
}
