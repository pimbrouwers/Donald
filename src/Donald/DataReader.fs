[<AutoOpen>]
module Donald.DataReader

open System
open System.Data
open System.IO

/// Option type extensions
module private Option =
    let asNullable<'a when 'a : struct and 'a : (new : unit -> 'a)  and 'a :> ValueType> (v : 'a option) = 
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
        this.GetOrdinalOption(name)
        |> Option.map map

    /// Safely retrieve String Option
    member this.GetStringOption (name : string) = 
        name |> this.GetOrdinalOption |> Option.map (fun i -> this.GetString(i))
    
    /// Safely retrieve Boolean Option
    member this.GetBooleanOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetBoolean(i)) 
    
    /// Safely retrieve Byte Option
    member this.GetByteOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetByte(i))
    
    /// Safely retrieve Char Option
    member this.GetCharOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetString(i).[0])
    
    /// Safely retrieve DateTime Option
    member this.GetDateTimeOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDateTime(i))    
    
    /// Safely retrieve DateTimeOffset Option
    member this.GetDateTimeOffsetOption (name : string) = 
        this.GetStringOption name |> Option.map (fun dt -> snd(DateTimeOffset.TryParse dt))
    
    /// Safely retrieve Decimal Option
    member this.GetDecimalOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDecimal(i))
    
    /// Safely retrieve Double Option
    member this.GetDoubleOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetDouble(i))
    
    /// Safely retrieve Float Option
    member this.GetFloatOption (name : string) = 
        this.GetDoubleOption name
    
    /// Safely retrieve Guid Option
    member this.GetGuidOption (name : string) = 
        name |> this.GetOption (fun i -> this.GetGuid(i))
    
    /// Safely retrieve Int16 Option
    member this.GetInt16Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt16(i))
    
    /// Safely retrieve Int32 Option
    member this.GetInt32Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt32(i))
    
    /// Safely retrieve Int64 Option
    member this.GetInt64Option (name : string) = 
        name |> this.GetOption (fun i -> this.GetInt64(i))
    
    // ------------
    // Defaults 
    // ------------

    /// Safely retrieve String or return provided default 
    member this.GetStringOrDefault (name : string) (defaultValue : String) =
        this.GetStringOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Boolean or return provided default 
    member this.GetBooleanOrDefault (name : string) (defaultValue : Boolean) =
        this.GetBooleanOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Byte or return provided default 
    member this.GetByteOrDefault (name : string) (defaultValue : Byte) =
        this.GetByteOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Char or return provided default 
    member this.GetCharOrDefault (name : string) (defaultValue : Char) =
        this.GetCharOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve DateTime or return provided default 
    member this.GetDateTimeOrDefault (name : string) (defaultValue : DateTime) =
        this.GetDateTimeOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve DateTimeOffset or return provided default 
    member this.GetDateTimeOffsetOrDefault (name : string) (defaultValue : DateTimeOffset) =
        this.GetDateTimeOffsetOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Decimal or return provided default 
    member this.GetDecimalOrDefault (name : string) (defaultValue : Decimal) =
        this.GetDecimalOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Double or return provided default 
    member this.GetDoubleOrDefault (name : string) (defaultValue : Double) =
        this.GetDoubleOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Float or return provided default 
    member this.GetFloatOrDefault (name : string) (defaultValue : float) =
        this.GetFloatOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Guid or return provided default 
    member this.GetGuidOrDefault (name : string) (defaultValue : Guid) =
        this.GetGuidOption name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Int16 or return provided default 
    member this.GetInt16OrDefault (name : string) (defaultValue : Int16) =
        this.GetInt16Option name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Int32 or return provided default 
    member this.GetInt32OrDefault (name : string) (defaultValue : Int32) =
        this.GetInt32Option name |> Option.defaultValue defaultValue
    
    /// Safely retrieve Int64 or return provided default 
    member this.GetInt64OrDefault (name : string) (defaultValue : Int64) =
        this.GetInt64Option name |> Option.defaultValue defaultValue    
        

    // ------------
    // Nullable
    // ------------
    /// Safely retrieve a nullable Boolean
    member this.GetNullableBoolean (name : string) =
        this.GetBooleanOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Byte
    member this.GetNullableByte (name : string) =
        this.GetByteOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Char
    member this.GetNullableChar (name : string) =
        this.GetCharOption name |> Option.asNullable
    
    /// Safely retrieve a nullable DateTime
    member this.GetNullableDateTime (name : string) =
        this.GetDateTimeOption name |> Option.asNullable
    
    /// Safely retrieve a nullable DateTimeOffset
    member this.GetNullableDateTimeOffset (name : string) =
        this.GetDateTimeOffsetOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Decimal
    member this.GetNullableDecimal (name : string) =
        this.GetDecimalOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Double
    member this.GetNullableDouble (name : string) =
        this.GetDoubleOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Float
    member this.GetNullableFloat (name : string) =
        this.GetNullableDouble name
    
    /// Safely retrieve a nullable Guid
    member this.GetNullableGuid (name : string) =
        this.GetGuidOption name |> Option.asNullable
    
    /// Safely retrieve a nullable Int16
    member this.GetNullableInt16 (name : string) =
        this.GetInt16Option name |> Option.asNullable
    
    /// Safely retrieve a nullable Int32
    member this.GetNullableInt32 (name : string) =
        this.GetInt32Option name |> Option.asNullable
    
    /// Safely retrieve a nullable Int64
    member this.GetNullableInt64 (name : string) =
        this.GetInt64Option name |> Option.asNullable  
    
    /// Safely retrieve byte[]
    member this.GetBytesOption (name : string) : byte[] option =
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
    member this.GetBytesOrDefault (name : string) (defaultValue : byte[]) : byte[] =
        match this.GetBytesOption name with
        | None       -> defaultValue
        | Some bytes -> bytes