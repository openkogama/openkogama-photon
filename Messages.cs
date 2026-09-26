namespace OpenKogama.Photon;

public sealed class OperationRequest
{
    public byte OperationCode;
    public Dictionary<byte, object?> Parameters = [];

    public object? this[byte key] => Parameters.GetValueOrDefault(key);

    public T? Get<T>(byte key) => Parameters.TryGetValue(key, out object? value) && value is T typed ? typed : default;

    public override string ToString() =>
        $"op {OperationCode} {{{string.Join(", ", Parameters.Select(p => $"{p.Key}={Describe(p.Value)}"))}}}";

    static string Describe(object? value) => value switch
    {
        null => "null",
        string text => $"\"{text}\"",
        byte[] bytes => $"byte[{bytes.Length}]",
        Array array => $"{array.GetType().GetElementType()?.Name}[{array.Length}]",
        _ => value.ToString() ?? "?",
    };
}

public sealed class OperationResponse
{
    public byte OperationCode;
    public short ReturnCode;
    public string? DebugMessage;
    public Dictionary<byte, object?> Parameters = [];

    public OperationResponse() { }

    public OperationResponse(OperationRequest request) => OperationCode = request.OperationCode;

    public OperationResponse(byte operationCode) => OperationCode = operationCode;

    public object? this[byte key]
    {
        get => Parameters.GetValueOrDefault(key);
        set => Parameters[key] = value;
    }

    public T? Get<T>(byte key) => Parameters.TryGetValue(key, out object? value) && value is T typed ? typed : default;
}

public sealed class EventData
{
    public byte Code;
    public Dictionary<byte, object?> Parameters = [];

    public EventData() { }

    public EventData(byte code) => Code = code;

    public object? this[byte key]
    {
        get => Parameters.GetValueOrDefault(key);
        set => Parameters[key] = value;
    }

    public T? Get<T>(byte key) => Parameters.TryGetValue(key, out object? value) && value is T typed ? typed : default;
}

public interface IMessageTranslator
{
    OperationResponse? Outgoing(OperationResponse response);
    EventData? Outgoing(EventData data);
}

public enum DisconnectReason
{
    ClientLeft,
    Timeout,
    ServerClosed,
}
