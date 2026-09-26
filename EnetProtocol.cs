namespace OpenKogama.Photon;

public static class EnetProtocol
{
    public const int HeaderSize = 12;
    public const int OffsetPeerId = 0;          // 2 bytes
    public const int OffsetCrcFlag = 2;         // 1 byte, 0xCC when CRC is on
    public const int OffsetCommandCount = 3;    // 1 byte
    public const int OffsetSentTime = 4;        // 4 bytes, sender uptime in ms
    public const int OffsetChallenge = 8;       // 4 bytes, random per connection

    public const int CmdType = 0;
    public const int CmdChannel = 1;
    public const int CmdFlags = 2;
    public const int CmdReserved = 3;
    public const int CmdLength = 4;             // 4 bytes, whole command incl. header
    public const int CmdSequence = 8;           // 4 bytes
    public const int CmdHeaderSize = 12;

    public const int AckSize = 20;
    public const int ConnectSize = 44;
    public const int VerifyConnectSize = 44;
    public const int DisconnectSize = 12;
    public const int UnreliableHeaderSize = 16;
    public const int FragmentHeaderSize = 32;

    public const byte FlagReliable = 1;
    public const byte ControlChannel = 0xFF;

    public const int Mtu = 1200;
    public const int MaxPayloadSize = Mtu - 12 - 36;

    public const byte PhotonMagic = 0xF3;
    public const byte PhotonMagic15 = 0xF1;
    public const int Header15Size = 6;
    public const byte MsgInitRequest = 0;
    public const byte MsgInitResponse = 1;
    public const byte MsgOperationRequest = 2;
    public const byte MsgOperationResponse = 3;
    public const byte MsgEvent = 4;
    public const byte MsgInternalOperationRequest = 6;
    public const byte MsgInternalOperationResponse = 7;
}

public enum PhotonProtocol
{
    Protocol16,
    Protocol15,
}

public enum CommandType : byte
{
    Acknowledge = 1,
    Connect = 2,
    VerifyConnect = 3,
    Disconnect = 4,
    Ping = 5,
    SendReliable = 6,
    SendUnreliable = 7,
    SendFragment = 8,
    ServerTime = 12,
}

public sealed class EnetCommand
{
    public CommandType Type;
    public byte Channel;
    public byte Flags;
    public int SequenceNumber;
    public byte[] Payload = [];

    public int UnreliableSequenceNumber;

    public int StartSequenceNumber;
    public int FragmentCount;
    public int FragmentNumber;
    public int TotalLength;
    public int FragmentOffset;

    public int AckedSequenceNumber;
    public int AckedSentTime;

    public bool IsReliable => (Flags & EnetProtocol.FlagReliable) != 0;

    public override string ToString() => $"{Type} ch={Channel} seq={SequenceNumber} payload={Payload.Length}";
}
