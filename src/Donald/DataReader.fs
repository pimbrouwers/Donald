[<AutoOpen>]
module Donald.DataReader

open System
open System.Data
open System.IO

#nowarn "44"

/// Option type extensions
module Option =
    let asNullable<'a when 'a : struct and 'a : (new : unit -> 'a)  and 'a :> ValueType> 
        (v : 'a option) : Nullable<'a> = 
        v
        |> Option.map (fun v -> Nullable<'a>(v))
        |> Option.defaultValue (Nullable<'a>())

/// IDataReader extensions
type IDataReader with
    member private this.GetOrdinalOption (name : string) = 
        let i = this.GetOrdinal(name)        
        match this.IsDBNull(i) with 
        | true  -> None
        | false -> Some(i)

    member private this.GetOption (map : int -> 'a when 'a : struct) (name : string) = 
        let fn v = 
            try
                map v
            with 
            | :? InvalidCastException as ex -> raise (FailiedCastException { FieldName = name; Error = ex })
                
        this.GetOrdinalOption(name)
        |> Option.map fn

    /// Safely retrieve String Option
    member this.ReadStringOption (name : string) = 
        name |> this.GetOrdinalOption |> Option.map (fun i -> this.GetString(i))
    
    /// Safely retrieve Boolean Option
    member this.ReadBooleanOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetBoolean(i)) 
    
    /// Safely retrieve Byte Option
    member this.ReadByteOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetByte(i))
    
    /// Safely retrieve Char Option
    member this.ReadCharOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetString(i).[0])
    
    /// Safely retrieve DateTime Option
    member this.ReadDateTimeOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDateTime(i))    
    
    /// Safely retrieve DateTimeOffset Option
    [<Obsolete>]
    member this.ReadDateTimeOffsetOption (name : string) = 
        this.ReadStringOption name |> Option.map (fun dt -> snd(DateTimeOffset.TryParse dt))
    
    /// Safely retrieve Decimal Option
    member this.ReadDecimalOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDecimal(i))
    
    /// Safely retrieve Double Option
    member this.ReadDoubleOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDouble(i))
    
    /// Safely retrieve Float Option
    member this.ReadFloatOption (name : string) = 
        this.ReadDoubleOption name
    
    /// Safely retrieve Guid Option
    member this.ReadGuidOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetGuid(i))
    
    /// Safely retrieve Int16 Option
    member this.ReadInt16Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt16(i))
    
    /// Safely retrieve Int32 Option
    member this.ReadInt32Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt32(i))
    
    /// Safely retrieve Int64 Option
    member this.ReadInt64Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt64(i))
    
    // ------------
    // Defaults 
    // ------------

    /// Safely retrieve String or return provided default 
    member this.ReadString (name : string) =
        this.ReadStringOption name |> Option.defaultValue String.Empty
    
    /// Safely retrieve Boolean or return provided default 
    member this.ReadBoolean (name : string) =
        this.ReadBooleanOption name |> Option.defaultValue false
    
    /// Safely retrieve Byte or return provided default 
    member this.ReadByte (name : string) =
        this.ReadByteOption name |> Option.defaultValue Byte.MinValue
    
    /// Safely retrieve Char or return provided default 
    member this.ReadChar (name : string) =
        this.ReadCharOption name |> Option.defaultValue Char.MinValue
    
    /// Safely retrieve DateTime or return provided default 
    member this.ReadDateTime (name : string) =
        this.ReadDateTimeOption name |> Option.defaultValue DateTime.MinValue
    
    /// Safely retrieve DateTimeOffset or return provided default 
    [<Obsolete>]
    member this.ReadDateTimeOffset (name : string) =
        this.ReadDateTimeOffsetOption name |> Option.defaultValue DateTimeOffset.MinValue
    
    /// Safely retrieve Decimal or return provided default 
    member this.ReadDecimal (name : string) =
        this.ReadDecimalOption name |> Option.defaultValue 0.0M
    
    /// Safely retrieve Double or return provided default 
    member this.ReadDouble (name : string) =
        this.ReadDoubleOption name |> Option.defaultValue 0.0
    
    /// Safely retrieve Float or return provided default 
    member this.ReadFloat (name : string) =
        this.ReadFloatOption name |> Option.defaultValue 0.0
    
    /// Safely retrieve Guid or return provided default 
    member this.ReadGuid (name : string) =
        this.ReadGuidOption name |> Option.defaultValue Guid.Empty
    
    /// Safely retrieve Int16 or return provided default 
    member this.ReadInt16 (name : string) =
        this.ReadInt16Option name |> Option.defaultValue 0s
    
    /// Safely retrieve Int32 or return provided default 
    member this.ReadInt32 (name : string) =
        this.ReadInt32Option name |> Option.defaultValue 0
    
    /// Safely retrieve Int64 or return provided default 
    member this.ReadInt64 (name : string) =
        this.ReadInt64Option name |> Option.defaultValue 0L    
    
    /// Safely retrieve byte[]
    member this.ReadBytesOption (name : string) : byte[] option =
        match name |> this.GetOrdinalOption with
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

            chunkValue 0L ms this |> ignore               
            Some (ms.ToArray())

    /// Safely retrieve byte[] or return provided default
    member this.ReadBytes (name : string) : byte[] =
        match this.ReadBytesOption name with
        | None       -> Array.zeroCreate 0
        | Some bytes -> bytes