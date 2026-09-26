namespace OpenKogama.Photon;

public static class Protocol15
{
    const byte CodeKey = 60;
    const byte ReturnCodeKey = 0;
    const byte DebugMessageKey = 1;

    public static OperationRequest ReadOperationRequest(byte[] data) =>
        Protocol16.ReadOperationRequest15(data, EnetProtocol.Header15Size);

    public static byte[] WriteInitResponse() => Frame(EnetProtocol.MsgInitResponse, []);

    public static byte[] WriteOperationResponse(OperationResponse response)
    {
        var parameters = new Dictionary<byte, object?>(response.Parameters)
        {
            [CodeKey] = response.OperationCode,
            [ReturnCodeKey] = (int)response.ReturnCode,
        };
        if (!string.IsNullOrEmpty(response.DebugMessage)) parameters[DebugMessageKey] = response.DebugMessage;
        return Frame(EnetProtocol.MsgOperationResponse, Protocol16.WriteParameterTable15(parameters));
    }

    public static byte[] WriteEventData(EventData data)
    {
        var parameters = new Dictionary<byte, object?>(data.Parameters) { [CodeKey] = data.Code };
        return Frame(EnetProtocol.MsgEvent, Protocol16.WriteParameterTable15(parameters));
    }

    static byte[] Frame(byte messageType, byte[] body)
    {
        byte[] payload = new byte[EnetProtocol.Header15Size + body.Length];
        payload[0] = EnetProtocol.PhotonMagic15;
        payload[3] = (byte)(payload.Length >> 8);
        payload[4] = (byte)payload.Length;
        payload[5] = messageType;
        Buffer.BlockCopy(body, 0, payload, EnetProtocol.Header15Size, body.Length);
        return payload;
    }
}
