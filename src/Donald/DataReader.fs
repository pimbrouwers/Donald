[<AutoOpen>]
module Donald.DataReader

open System
open System.Data
open System.IO

/// Option type extensions
module Option =
    let asNullable<'a when 'a : struct and 'a : (new : unit -> 'a)  and 'a :> ValueType> (v : 'a option) = 
        v
        |> Option.map (fun v -> Nullable<'a>(v))
        |> Option.defaultValue (Nullable<'a>())

/// IDataReader extensions
type IDataReader with
    member this.GetOrdinalOption (name : string) = 
        let i = this.GetOrdinal(name)        
        match this.IsDBNull(i) with 
        | true  -> None
        | false -> Some(i)

    member this.GetOption (map : int -> 'a when 'a : struct) (name : string) = 
        this.GetOrdinalOption(name)
        |> Option.map map

    member _.GetNullable<'a when 'a : struct and 'a : (new : unit -> 'a)  and 'a :> ValueType> (v : 'a option) = 
        v
        |> Option.map (fun v -> Nullable<'a>(v))
        |> Option.defaultValue (Nullable<'a>())
    
    member this.GetStringOption (name : string)           = name |> this.GetOrdinalOption |> Option.map (fun i -> this.GetString(i))
    member this.GetBooleanOption (name : string)          = name |> this.GetOption (fun i -> this.GetBoolean(i)) 
    member this.GetByteOption (name : string)             = name |> this.GetOption (fun i -> this.GetByte(i))
    member this.GetCharOption (name : string)             = name |> this.GetOption (fun i -> this.GetString(i).[0])
    member this.GetDateTimeOption (name : string)         = name |> this.GetOption (fun i -> this.GetDateTime(i))    
    member this.GetDateTimeOffsetOption (name : string)   = this.GetStringOption name |> Option.map (fun dt -> snd(DateTimeOffset.TryParse dt))
    member this.GetDecimalOption (name : string)          = name |> this.GetOption (fun i -> this.GetDecimal(i))
    member this.GetDoubleOption (name : string)           = name |> this.GetOption (fun i -> this.GetDouble(i))
    member this.GetFloatOption (name : string)            = this.GetDoubleOption name
    member this.GetGuidOption (name : string)             = name |> this.GetOption (fun i -> this.GetGuid(i))
    member this.GetInt16Option (name : string)            = name |> this.GetOption (fun i -> this.GetInt16(i))
    member this.GetInt32Option (name : string)            = name |> this.GetOption (fun i -> this.GetInt32(i))
    member this.GetInt64Option (name : string)            = name |> this.GetOption (fun i -> this.GetInt64(i))  
        
    member this.GetString (name : string)                 = this.GetStringOption name         |> Option.defaultValue null
    member this.GetBoolean (name : string)                = this.GetBooleanOption name        |> Option.defaultValue false
    member this.GetByte (name : string)                   = this.GetByteOption name           |> Option.defaultValue Byte.MinValue
    member this.GetChar (name : string)                   = this.GetCharOption name           |> Option.defaultValue Char.MinValue
    member this.GetDateTime (name : string)               = this.GetDateTimeOption name       |> Option.defaultValue DateTime.MinValue
    member this.GetDateTimeOffset (name : string)         = this.GetDateTimeOffsetOption name |> Option.defaultValue DateTimeOffset.MinValue
    member this.GetDecimal (name : string)                = this.GetDecimalOption name        |> Option.defaultValue 0.0M
    member this.GetDouble (name : string)                 = this.GetDoubleOption name         |> Option.defaultValue 0.0
    member this.GetFloat (name : string)                  = this.GetFloatOption name          |> Option.defaultValue 0.0
    member this.GetGuid (name : string)                   = this.GetGuidOption name           |> Option.defaultValue Guid.Empty
    member this.GetInt16 (name : string)                  = this.GetInt16Option name          |> Option.defaultValue 0s
    member this.GetInt32 (name : string)                  = this.GetInt32Option name          |> Option.defaultValue 0
    member this.GetInt64 (name : string)                  = this.GetInt64Option name          |> Option.defaultValue 0L  
        
    member this.GetNullableBoolean (name : string)        = this.GetBooleanOption name        |> Option.asNullable
    member this.GetNullableByte (name : string)           = this.GetByteOption name           |> Option.asNullable
    member this.GetNullableChar (name : string)           = this.GetCharOption name           |> Option.asNullable
    member this.GetNullableDateTime (name : string)       = this.GetDateTimeOption name       |> Option.asNullable
    member this.GetNullableDateTimeOffset (name : string) = this.GetDateTimeOffsetOption name |> Option.asNullable
    member this.GetNullableDecimal (name : string)        = this.GetDecimalOption name        |> Option.asNullable
    member this.GetNullableDouble (name : string)         = this.GetDoubleOption name         |> Option.asNullable
    member this.GetNullableFloat (name : string)          = this.GetNullableDouble name
    member this.GetNullableGuid (name : string)           = this.GetGuidOption name           |> Option.asNullable
    member this.GetNullableInt16 (name : string)          = this.GetInt16Option name          |> Option.asNullable
    member this.GetNullableInt32 (name : string)          = this.GetInt32Option name          |> Option.asNullable
    member this.GetNullableInt64 (name : string)          = this.GetInt64Option name          |> Option.asNullable  
        
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

    member this.GetBytes (name : string) : byte[] =
        match this.GetBytesOption name with
        | None       -> [||]
        | Some bytes -> bytes


