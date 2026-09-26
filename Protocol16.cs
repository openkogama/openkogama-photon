using System.Buffers.Binary;
using System.Text;

namespace OpenKogama.Photon;

public static class Protocol16
{
    public static OperationRequest ReadOperationRequest(byte[] data, int offset)
    {
        var reader = new Reader(data, offset);
        return new OperationRequest
        {
            OperationCode = reader.ReadByte(),
            Parameters = reader.ReadParameterTable(),
        };
    }

    internal static OperationRequest ReadOperationRequest15(byte[] data, int offset)
    {
        var reader = new Reader(data, offset);
        byte code = reader.ReadByte();
        reader.ReadByte();
        return new OperationRequest { OperationCode = code, Parameters = reader.ReadParameterTable() };
    }

    internal static byte[] WriteParameterTable15(Dictionary<byte, object?> parameters)
    {
        var writer = new Writer(protocol15: true);
        writer.WriteParameterTable(parameters);
        return writer.ToArray();
    }

    public static EventData ReadEventData(byte[] data, int offset)
    {
        var reader = new Reader(data, offset);
        return new EventData
        {
            Code = reader.ReadByte(),
            Parameters = reader.ReadParameterTable(),
        };
    }

    public static OperationResponse ReadOperationResponse(byte[] data, int offset)
    {
        var reader = new Reader(data, offset);
        return new OperationResponse
        {
            OperationCode = reader.ReadByte(),
            ReturnCode = reader.ReadShort(),
            DebugMessage = reader.ReadValue() as string,
            Parameters = reader.ReadParameterTable(),
        };
    }

    public static byte[] WriteOperationResponse(OperationResponse response)
    {
        var writer = new Writer();
        writer.WriteByte(response.OperationCode);
        writer.WriteShortRaw(response.ReturnCode);

        if (string.IsNullOrEmpty(response.DebugMessage))
            writer.WriteByte((byte)GpType.Null);
        else
            writer.WriteValue(response.DebugMessage);

        writer.WriteParameterTable(response.Parameters);
        return writer.ToArray();
    }

    public static byte[] WriteEventData(EventData data)
    {
        var writer = new Writer();
        writer.WriteByte(data.Code);
        writer.WriteParameterTable(data.Parameters);
        return writer.ToArray();
    }

    ref struct Reader(byte[] data, int offset)
    {
        readonly byte[] _data = data;
        int _offset = offset;

        public byte ReadByte() => _data[_offset++];

        bool ReadBoolean() => _data[_offset++] != 0;

        public short ReadShort()
        {
            short value = BinaryPrimitives.ReadInt16BigEndian(_data.AsSpan(_offset));
            _offset += 2;
            return value;
        }

        int ReadInt()
        {
            int value = BinaryPrimitives.ReadInt32BigEndian(_data.AsSpan(_offset));
            _offset += 4;
            return value;
        }

        long ReadLong()
        {
            long value = BinaryPrimitives.ReadInt64BigEndian(_data.AsSpan(_offset));
            _offset += 8;
            return value;
        }

        float ReadFloat()
        {
            float value = BinaryPrimitives.ReadSingleBigEndian(_data.AsSpan(_offset));
            _offset += 4;
            return value;
        }

        double ReadDouble()
        {
            double value = BinaryPrimitives.ReadDoubleBigEndian(_data.AsSpan(_offset));
            _offset += 8;
            return value;
        }

        string ReadString()
        {
            int length = ReadShort();
            string value = Encoding.UTF8.GetString(_data, _offset, length);
            _offset += length;
            return value;
        }

        byte[] ReadByteArray()
        {
            int length = ReadInt();
            byte[] value = _data[_offset..(_offset + length)];
            _offset += length;
            return value;
        }

        public Dictionary<byte, object?> ReadParameterTable()
        {
            int count = ReadShort();
            var table = new Dictionary<byte, object?>(count);
            for (int i = 0; i < count; i++)
            {
                byte key = ReadByte();
                table[key] = ReadValue();
            }
            return table;
        }

        public object? ReadValue() => ReadValue((GpType)ReadByte());

        object? ReadValue(GpType type) => type switch
        {
            GpType.Null => null,
            GpType.Byte => ReadByte(),
            GpType.Boolean => ReadBoolean(),
            GpType.Short => ReadShort(),
            GpType.Integer => ReadInt(),
            GpType.Long => ReadLong(),
            GpType.Float => ReadFloat(),
            GpType.Double => ReadDouble(),
            GpType.String => ReadString(),
            GpType.ByteArray => ReadByteArray(),
            GpType.Hashtable => ReadHashtable(),
            GpType.ObjectArray => ReadObjectArray(),
            GpType.StringArray => ReadStringArray(),
            GpType.IntegerArray => ReadIntegerArray(),
            GpType.Array => ReadTypedArray(),
            GpType.Dictionary => ReadDictionary(),
            _ => throw new NotSupportedException($"protocol16: unsupported type {(byte)type}"),
        };

        PhotonDictionary ReadDictionary()
        {
            var dictionary = new PhotonDictionary
            {
                KeyType = (GpType)ReadByte(),
                ValueType = (GpType)ReadByte(),
            };

            if (dictionary.ValueType == GpType.Dictionary)
                throw new NotSupportedException("protocol16: nested dictionary headers are not handled");

            int count = ReadShort();
            for (int i = 0; i < count; i++)
            {
                object? key = dictionary.KeyType == GpType.Unknown ? ReadValue() : ReadValue(dictionary.KeyType);
                object? value = dictionary.ValueType == GpType.Unknown ? ReadValue() : ReadValue(dictionary.ValueType);
                if (key is not null) dictionary.Entries[key] = value;
            }

            return dictionary;
        }

        Dictionary<object, object?> ReadHashtable()
        {
            int count = ReadShort();
            var table = new Dictionary<object, object?>(count);
            for (int i = 0; i < count; i++)
            {
                object? key = ReadValue();
                object? value = ReadValue();
                if (key is not null) table[key] = value;
            }
            return table;
        }

        object?[] ReadObjectArray()
        {
            int length = ReadShort();
            var array = new object?[length];
            for (int i = 0; i < length; i++) array[i] = ReadValue();
            return array;
        }

        string[] ReadStringArray()
        {
            int length = ReadShort();
            var array = new string[length];
            for (int i = 0; i < length; i++) array[i] = ReadString();
            return array;
        }

        int[] ReadIntegerArray()
        {
            int length = ReadInt();
            var array = new int[length];
            for (int i = 0; i < length; i++) array[i] = ReadInt();
            return array;
        }

        // array with the element type written once, before the elements
        object ReadTypedArray()
        {
            int length = ReadShort();
            var type = (GpType)ReadByte();

            switch (type)
            {
                case GpType.Byte:
                {
                    var array = new byte[length];
                    for (int i = 0; i < length; i++) array[i] = ReadByte();
                    return array;
                }
                case GpType.Integer:
                {
                    var array = new int[length];
                    for (int i = 0; i < length; i++) array[i] = ReadInt();
                    return array;
                }
                case GpType.Short:
                {
                    var array = new short[length];
                    for (int i = 0; i < length; i++) array[i] = ReadShort();
                    return array;
                }
                case GpType.Float:
                {
                    var array = new float[length];
                    for (int i = 0; i < length; i++) array[i] = ReadFloat();
                    return array;
                }
                case GpType.String:
                {
                    var array = new string[length];
                    for (int i = 0; i < length; i++) array[i] = ReadString();
                    return array;
                }
                case GpType.ByteArray:
                {
                    var array = new byte[length][];
                    for (int i = 0; i < length; i++) array[i] = ReadByteArray();
                    return array;
                }
                default:
                {
                    var array = new object?[length];
                    for (int i = 0; i < length; i++) array[i] = ReadValue(type);
                    return array;
                }
            }
        }
    }

    sealed class Writer(bool protocol15 = false)
    {
        readonly List<byte> _bytes = [];
        readonly bool _protocol15 = protocol15;

        public byte[] ToArray() => [.. _bytes];

        public void WriteByte(byte value) => _bytes.Add(value);

        public void WriteShortRaw(short value)
        {
            _bytes.Add((byte)(value >> 8));
            _bytes.Add((byte)value);
        }

        void WriteIntRaw(int value)
        {
            _bytes.Add((byte)(value >> 24));
            _bytes.Add((byte)(value >> 16));
            _bytes.Add((byte)(value >> 8));
            _bytes.Add((byte)value);
        }

        void WriteLongRaw(long value)
        {
            for (int shift = 56; shift >= 0; shift -= 8) _bytes.Add((byte)(value >> shift));
        }

        void WriteStringRaw(string value)
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(value);
            WriteShortRaw((short)utf8.Length);
            _bytes.AddRange(utf8);
        }

        public void WriteParameterTable(Dictionary<byte, object?> parameters)
        {
            var written = _protocol15 ? parameters.Where(pair => pair.Value is not null).ToList() : [.. parameters];
            WriteShortRaw((short)written.Count);
            foreach ((byte key, object? value) in written)
            {
                WriteByte(key);
                WriteValue(value);
            }
        }

        void WriteHashtable(IEnumerable<KeyValuePair<object, object?>> entries)
        {
            var written = _protocol15 ? entries.Where(pair => pair.Value is not null).ToList() : [.. entries];
            WriteByte((byte)GpType.Hashtable);
            WriteShortRaw((short)written.Count);
            foreach ((object key, object? item) in written)
            {
                WriteValue(key);
                WriteValue(item);
            }
        }

        void WriteVector(object?[] items)
        {
            WriteByte((byte)Vector);
            WriteShortRaw((short)items.Length);
            for (int i = 0; i < items.Length; i++)
            {
                if (items[i] is null || items[i]!.GetType() != items[0]!.GetType())
                    throw new NotSupportedException("protocol15: vectors need items of one type");
                if (i == 0) WriteValue(items[i]); else WriteRaw(items[i]);
            }
        }

        const byte Vector = 118;

        void WriteDictionaryBody(PhotonDictionary dictionary)
        {
            WriteByte((byte)dictionary.KeyType);
            WriteByte((byte)dictionary.ValueType);
            WriteShortRaw((short)dictionary.Entries.Count);

            bool writeKeyType = dictionary.KeyType == GpType.Unknown;
            bool writeValueType = dictionary.ValueType == GpType.Unknown;

            foreach ((object key, object? value) in dictionary.Entries)
            {
                if (writeKeyType) WriteValue(key); else WriteRaw(key);
                if (writeValueType) WriteValue(value); else WriteRaw(value);
            }
        }

        // value without its type code, used when a dictionary already declared the type
        void WriteRaw(object? value)
        {
            switch (value)
            {
                case byte number: WriteByte(number); break;
                case bool flag: WriteByte(flag ? (byte)1 : (byte)0); break;
                case short number: WriteShortRaw(number); break;
                case int number: WriteIntRaw(number); break;
                case long number: WriteLongRaw(number); break;
                case float number: WriteIntRaw(BitConverter.SingleToInt32Bits(number)); break;
                case double number: WriteLongRaw(BitConverter.DoubleToInt64Bits(number)); break;
                case string text: WriteStringRaw(text); break;
                case PhotonDictionary dictionary: WriteDictionaryBody(dictionary); break;
                default: throw new NotSupportedException($"protocol16: cannot write {value?.GetType()} untyped");
            }
        }

        public void WriteValue(object? value)
        {
            switch (value)
            {
                case null when _protocol15:
                    throw new NotSupportedException("protocol15: null cannot be written");
                case null:
                    WriteByte((byte)GpType.Null);
                    break;
                case byte number:
                    WriteByte((byte)GpType.Byte);
                    WriteByte(number);
                    break;
                case bool flag:
                    WriteByte((byte)GpType.Boolean);
                    WriteByte(flag ? (byte)1 : (byte)0);
                    break;
                case short number:
                    WriteByte((byte)GpType.Short);
                    WriteShortRaw(number);
                    break;
                case int number:
                    WriteByte((byte)GpType.Integer);
                    WriteIntRaw(number);
                    break;
                case long number:
                    WriteByte((byte)GpType.Long);
                    WriteLongRaw(number);
                    break;
                case float number:
                    WriteByte((byte)GpType.Float);
                    WriteIntRaw(BitConverter.SingleToInt32Bits(number));
                    break;
                case double number:
                    WriteByte((byte)GpType.Double);
                    WriteLongRaw(BitConverter.DoubleToInt64Bits(number));
                    break;
                case string text:
                    WriteByte((byte)GpType.String);
                    WriteStringRaw(text);
                    break;
                case byte[] bytes:
                    WriteByte((byte)GpType.ByteArray);
                    WriteIntRaw(bytes.Length);
                    _bytes.AddRange(bytes);
                    break;
                case int[] numbers:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)numbers.Length);
                    WriteByte((byte)GpType.Integer);
                    foreach (int number in numbers) WriteIntRaw(number);
                    break;
                case float[] numbers:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)numbers.Length);
                    WriteByte((byte)GpType.Float);
                    foreach (float number in numbers) WriteIntRaw(BitConverter.SingleToInt32Bits(number));
                    break;
                case short[] numbers:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)numbers.Length);
                    WriteByte((byte)GpType.Short);
                    foreach (short number in numbers) WriteShortRaw(number);
                    break;
                case bool[] flags:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)flags.Length);
                    WriteByte((byte)GpType.Boolean);
                    foreach (bool flag in flags) WriteByte(flag ? (byte)1 : (byte)0);
                    break;
                case long[] numbers:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)numbers.Length);
                    WriteByte((byte)GpType.Long);
                    foreach (long number in numbers) WriteLongRaw(number);
                    break;
                case byte[][] blobs:
                    WriteByte((byte)GpType.Array);
                    WriteShortRaw((short)blobs.Length);
                    WriteByte((byte)GpType.ByteArray);
                    foreach (byte[] blob in blobs)
                    {
                        WriteIntRaw(blob.Length);
                        _bytes.AddRange(blob);
                    }
                    break;
                case string[] texts:
                    WriteByte((byte)GpType.StringArray);
                    WriteShortRaw((short)texts.Length);
                    foreach (string text in texts) WriteStringRaw(text);
                    break;
                case object?[] items when _protocol15:
                    WriteVector(items);
                    break;
                case object?[] items:
                    WriteByte((byte)GpType.ObjectArray);
                    WriteShortRaw((short)items.Length);
                    foreach (object? item in items) WriteValue(item);
                    break;
                case Dictionary<object, object?> table:
                    WriteHashtable(table);
                    break;
                case PhotonDictionary dictionary when _protocol15:
                    WriteHashtable(dictionary.Entries);
                    break;
                case PhotonDictionary dictionary:
                    WriteByte((byte)GpType.Dictionary);
                    WriteDictionaryBody(dictionary);
                    break;
                default:
                    throw new NotSupportedException($"protocol16: cannot write {value.GetType()}");
            }
        }
    }
}
