namespace Donald

open System
open System.Data
open System.IO

[<AutoOpen>]
module IDataReaderExtensions =
    type IDataReader with
        member private x.GetOrdinalOption(name : string) =
            try
                // Some vendors will return a -1 index instead of throwing an
                // IndexOfOutRangeException
                match x.GetOrdinal name with
                | i when i < 0 -> raise (IndexOutOfRangeException(name + " is not a valid field name"))
                | i when x.IsDBNull(i) -> None
                | i -> Some i
            with
            | :? IndexOutOfRangeException as ex -> raise (DbReaderException(name, ex))

        member private x.GetOption(map : int -> 'a when 'a : struct) (name : string) =
            let fn v =
                try
                    map v
                with
                | :? InvalidCastException as ex -> raise (DbReaderException(name, ex))

            x.GetOrdinalOption(name)
            |> Option.map fn

        /// Safely retrieve String Option
        member x.ReadStringOption(name : string) = name |> x.GetOrdinalOption |> Option.map(fun i -> x.GetString(i))

        /// Safely retrieve Boolean Option
        member x.ReadBooleanOption(name : string) = name |> x.GetOption(fun i -> x.GetBoolean(i))

        /// Safely retrieve Byte Option
        member x.ReadByteOption(name : string) = name |> x.GetOption(fun i -> x.GetByte(i))

        /// Safely retrieve Char Option
        member x.ReadCharOption(name : string) = name |> x.GetOption(fun i -> x.GetString(i).[0])

        /// Safely retrieve DateTime Option
        member x.ReadDateTimeOption(name : string) = name |> x.GetOption(fun i -> x.GetDateTime(i))

        /// Safely retrieve Decimal Option
        member x.ReadDecimalOption(name : string) = name |> x.GetOption(fun i -> x.GetDecimal(i))

        /// Safely retrieve Double Option
        member x.ReadDoubleOption(name : string) = name |> x.GetOption(fun i -> x.GetDouble(i))

        /// Safely retrieve Float Option
        member x.ReadFloatOption(name : string) = x.ReadDoubleOption name

        /// Safely retrieve Guid Option
        member x.ReadGuidOption(name : string) = name |> x.GetOption(fun i -> x.GetGuid(i))

        /// Safely retrieve Int16 Option
        member x.ReadInt16Option (name : string) = name |> x.GetOption(fun i -> x.GetInt16(i))

        /// Safely retrieve Int32 Option
        member x.ReadInt32Option (name : string) = name |> x.GetOption(fun i -> x.GetInt32(i))

        /// Safely retrieve Int64 Option
        member x.ReadInt64Option (name : string) = name |> x.GetOption(fun i -> x.GetInt64(i))

        // ------------
        // Defaults
        // ------------

        /// Safely retrieve String or return provided default
        member x.ReadString(name : string) = x.ReadStringOption name |> Option.defaultValue String.Empty

        /// Safely retrieve Boolean or return provided default
        member x.ReadBoolean(name : string) = x.ReadBooleanOption name |> Option.defaultValue false

        /// Safely retrieve Byte or return provided default
        member x.ReadByte(name : string) = x.ReadByteOption name |> Option.defaultValue Byte.MinValue

        /// Safely retrieve Char or return provided default
        member x.ReadChar(name : string) = x.ReadCharOption name |> Option.defaultValue Char.MinValue

        /// Safely retrieve DateTime or return provided default
        member x.ReadDateTime(name : string) = x.ReadDateTimeOption name |> Option.defaultValue DateTime.MinValue

        /// Safely retrieve Decimal or return provided default
        member x.ReadDecimal(name : string) = x.ReadDecimalOption name |> Option.defaultValue 0.0M

        /// Safely retrieve Double or return provided default
        member x.ReadDouble(name : string) = x.ReadDoubleOption name |> Option.defaultValue 0.0

        /// Safely retrieve Float or return provided default
        member x.ReadFloat(name : string) = x.ReadFloatOption name |> Option.defaultValue 0.0

        /// Safely retrieve Guid or return provided default
        member x.ReadGuid(name : string) = x.ReadGuidOption name |> Option.defaultValue Guid.Empty

        /// Safely retrieve Int16 or return provided default
        member x.ReadInt16 (name : string) = x.ReadInt16Option name |> Option.defaultValue 0s

        /// Safely retrieve Int32 or return provided default
        member x.ReadInt32 (name : string) = x.ReadInt32Option name |> Option.defaultValue 0

        /// Safely retrieve Int64 or return provided default
        member x.ReadInt64 (name : string) = x.ReadInt64Option name |> Option.defaultValue 0L

        /// Safely retrieve byte[]
        member x.ReadBytesOption(name : string) : byte[] option =
            match name |> x.GetOrdinalOption with
            | None   -> None
            | Some i ->
                use ms = new MemoryStream()
                let bufferSize = 1024
                let buffer = Array.zeroCreate bufferSize
                let rec chunkValue (position: int64) (str : Stream) (rd : IDataReader) =
                    match rd.GetBytes(i, position, buffer, 0, buffer.Length) with
                    | 0L   -> ()
                    | read ->
                        ms.Write(buffer, 0, int read)
                        chunkValue (position + read) str rd

                chunkValue 0L ms x |> ignore
                Some (ms.ToArray())

        /// Safely retrieve byte[] or return provided default
        member x.ReadBytes(name : string) : byte[] =
            match x.ReadBytesOption name with
            | None       -> Array.zeroCreate 0
            | Some bytes -> bytes
