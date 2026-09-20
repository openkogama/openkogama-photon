namespace OpenKogama.Photon;

public sealed class PhotonDictionary
{
    public GpType KeyType = GpType.Unknown;
    public GpType ValueType = GpType.Unknown;
    public Dictionary<object, object?> Entries = [];

    public static PhotonDictionary Untyped() => new();

    public static PhotonDictionary ByteKeyed() => new() { KeyType = GpType.Byte };

    public object? this[object key]
    {
        get => Entries[key];
        set => Entries[key] = value;
    }

    public void Add(object key, object? value) => Entries[key] = value;
}
