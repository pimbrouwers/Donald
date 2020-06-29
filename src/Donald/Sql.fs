module Donald 

open System
open System.Data
open System.Data.Common
open System.IO
open FSharp.Control.Tasks

/// Represents the ability to create a new IDbConnection
type DbConnectionFactory = unit -> IDbConnection

/// Represents the result of an action against the database
/// or, an encapsulation of the exception thrown
type DbResult<'a> =
    | DbResult of 'a    
    | DbError  of Exception

/// Represents the supported data types for database IO
type SqlType =
    | Null       
    | String         of String
    | AnsiString     of String
    | Boolean        of Boolean
    | Byte           of Byte
    | Char           of Char
    | AnsiChar       of Char
    | Decimal        of Decimal
    | Double         of Double
    | Float          of float
    | Guid           of Guid
    | Int16          of Int16
    | Int32          of Int32
    | Int            of int32
    | Int64          of Int64
    | DateTime       of DateTime
    | DateTimeOffset of DateTimeOffset
    | Bytes          of Byte[]

/// Specifies an input parameter for an IDbCommand
[<Struct>]
type DbParam = 
    { 
        Name : String
        Value : SqlType
    }


/// Create new instance of IDbConnection using provided DbConnectionFactory
let createConn (createConnection : DbConnectionFactory) =
    createConnection ()    
  
/// Create a new IDbTransaction
let beginTran (conn : IDbConnection) = 
    if conn.State <> ConnectionState.Open then conn.Open()
    conn.BeginTransaction()

/// Rollback IDbTransaction
let rollbackTran (tran : IDbTransaction) =
    try        
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Rollback()
    with            
        | _ -> 
            reraise() 

/// Attempt to commit IDbTransaction, rollback if failed.
let commitTran (tran : IDbTransaction) =
    try
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Commit() 
    with
        /// Is supposed to throw System.InvalidOperationException
        /// when commmited or rolled back already, but most
        /// implementations do not. So in all cases try rolling back
        | _ -> 
            rollbackTran tran
            reraise()


/// Create a new IDbCommand  
let newIDbCommand (sql : string) (tran : IDbTransaction) =
    let cmd = tran.Connection.CreateCommand()
    cmd.CommandType <- CommandType.Text
    cmd.CommandText <- sql
    cmd.Transaction <- tran
    cmd 

/// Assign DbParam to IDbCommand
let assignDbParams (cmd : IDbCommand) (dbParams : DbParam list) =
    dbParams
    |> Seq.iter (fun param ->
        let p = cmd.CreateParameter()        
        p.ParameterName <- param.Name
        p.Value <- param.Value

        match param.Value with
        | Null -> 
            p.Value <- DBNull.Value

        | String v -> 
            p.DbType <- DbType.String
            p.Value <- v

        | AnsiString v ->
            p.DbType <- DbType.AnsiString
            p.Value <- v

        | Boolean v -> 
            p.DbType <- DbType.Boolean
            p.Value <- v

        | Byte v -> 
            p.DbType <- DbType.Byte
            p.Value <- v

        | Char v -> 
            p.DbType <- DbType.AnsiString
            p.Value <- v

        | AnsiChar v ->
            p.DbType <- DbType.String
            p.Value <- v

        | Decimal v -> 
            p.DbType <- DbType.Decimal
            p.Value <- v

        | Double v
        | Float v ->
            p.DbType <- DbType.Double
            p.Value <- v 

        | Int16 v -> 
            p.DbType <- DbType.Int16
            p.Value <- v

        | Int32 v 
        | Int v -> 
            p.DbType <- DbType.Int32
            p.Value <- v

        | Int64 v -> 
            p.DbType <- DbType.Int64
            p.Value <- v
            
        | Guid v -> 
            p.DbType <- DbType.Guid
            p.Value <- v

        | DateTime v -> 
            p.DbType <- DbType.DateTime
            p.Value <- v

        | DateTimeOffset v ->
            p.DbType <- DbType.DateTimeOffset
            p.Value <- v

        | Bytes v -> 
            p.DbType <- DbType.Binary
            p.Value <- v

        cmd.Parameters.Add(p) |> ignore)

/// Clear all parameters from IDbCommand
let clearParameters (cmd : IDbCommand) =
    cmd.Parameters.Clear()
    
/// Create a new IDbCommand  
let newCommand (sql : string) (dbParams : DbParam list) (tran : IDbTransaction) =
    let cmd = newIDbCommand sql tran
    assignDbParams cmd dbParams
    cmd

/// DbParam constructor
let newParam (name : string) (value : SqlType) =
    { Name = name; Value = value }


/// Query for multiple results within transaction scope
let tranQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    use rd = cmd.ExecuteReader()
    let results = [ while rd.Read() do yield map rd ]
    rd.Close() |> ignore
    results

/// Query async for multiple results within transaction scope
let tranQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand
        use! rd = cmd.ExecuteReaderAsync()        

        let rec loopAsync (acc : 'a list) (rd : DbDataReader) =
            task {
                let! canRead = rd.ReadAsync()
                match canRead with
                | false -> return acc
                | true  -> 
                    let result = map rd
                    let results = result :: acc
                    return! loopAsync results rd
            }

        let! results = loopAsync [] rd
        rd.Close() |> ignore
        return results
    }

/// Query for multiple results
let query (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let results = tranQuery sql param map tran
    commitTran tran
    results
    
/// Query async for multiple results
let queryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        let! results = tranQueryAsync sql param map tran
        commitTran tran
        return results
    }

/// Try to query for multiple results within transaction scope
let tryTranQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    try
        tranQuery sql param map tran
        |> DbResult 
    with ex -> DbError ex

/// Try to query for multiple results within transaction scope
let tryTranQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        try
            let! result = tranQueryAsync sql param map tran
            return DbResult result
        with ex -> return DbError ex
    }

/// Query for multiple results
let tryQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =   
    try
        use tran = beginTran conn
        let results = tranQuery sql param map tran
        commitTran tran
        DbResult results
    with ex -> DbError ex

/// Query async for multiple results
let tryQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =   
    task {
        try
            use tran = beginTran conn
            let! results = tranQueryAsync sql param map tran
            commitTran tran
            return DbResult results
        with ex -> return DbError ex
    }
    

/// Query for single result within transaction scope
let tranQuerySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    use rd = cmd.ExecuteReader()
    if rd.Read() then Some(map rd) else None

/// Query async for single result within transaction scope
let tranQuerySingleAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand
        use! rd = cmd.ExecuteReaderAsync()
        let! canRead = rd.ReadAsync()
        return if canRead then Some(map rd) else None
    }

/// Query for single result
let querySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let result = tranQuerySingle sql param map tran
    commitTran tran
    result

/// Query for single result
let querySingleAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        let! result = tranQuerySingleAsync sql param map tran
        commitTran tran
        return result
    }

/// Try to query for single result within transaction scope
let tryTranQuerySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    try
        tranQuerySingle sql param map tran
        |> DbResult
    with ex -> DbError ex

/// Try to query async for single result within transaction scope
let tryTranQuerySingleAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        try
            let! result = tranQuerySingleAsync sql param map tran
            return DbResult result
        with ex -> return DbError ex
    }

/// Query for single result
let tryQuerySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    try
        use tran = beginTran conn
        let result = tranQuerySingle sql param map tran 
        commitTran tran
        DbResult result
    with ex -> DbError ex

/// Query for single result
let tryQuerySingleAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    task {
        try
            use tran = beginTran conn
            let! result = tranQuerySingleAsync sql param map tran 
            commitTran tran
            return DbResult result
        with ex -> return DbError ex
    }


/// Execute query with no results within transction scope
let tranExec (sql : string) (param : DbParam list) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran    
    cmd.ExecuteNonQuery() |> ignore

/// Execute async query with no results within transction scope
let tranExecAsync (sql : string) (param : DbParam list) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand   
        let! _ = cmd.ExecuteNonQueryAsync()
        return ()
    }

/// Execute query with no results
let exec (sql : string) (param : DbParam list) (conn : IDbConnection) =
    use tran = beginTran conn
    tranExec sql param tran
    commitTran tran

/// Execute async query with no results
let execAsync (sql : string) (param : DbParam list) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        use cmd = newCommand sql param tran :?> DbCommand
        let! _ = cmd.ExecuteNonQueryAsync()
        commitTran tran
    }

/// Try to execute query with no results within transction scope
let tryTranExec (sql : string) (param : DbParam list) (tran : IDbTransaction) =
    try
        tranExec sql param tran
        |> DbResult
    with ex -> DbError ex

/// Try to execute async query with no results within transction scope
let tryTranExecAsync (sql : string) (param : DbParam list) (tran : IDbTransaction) =
    task {
        try
            do! tranExecAsync sql param tran 
            return DbResult ()
        with ex -> return DbError ex
    }

/// Try to execute query with no results
let tryExec (sql : string) (param : DbParam list) (conn : IDbConnection) =
    try
        use tran = beginTran conn
        tranExec sql param tran 
        commitTran tran
        DbResult ()
    with ex -> DbError ex

/// Try to execute query with no results
let tryExecAsync (sql : string) (param : DbParam list) (conn : IDbConnection) =
    task {
        try
            use tran = beginTran conn
            do! tranExecAsync sql param tran 
            commitTran tran
            return DbResult ()
        with ex -> return DbError ex
    }


/// Execute query with no results many times within transction scope
let tranExecMany (sql : string) (manyParam : DbParam list list) (tran : IDbTransaction) =    
    use cmd = newIDbCommand sql tran    
    for param in manyParam do
        clearParameters cmd
        assignDbParams cmd param
        cmd.ExecuteNonQuery() |> ignore

/// Execute query async with no results many times within transction scope
let tranExecManyAsync (sql : string) (manyParam : DbParam list list) (tran : IDbTransaction) =    
    task {
        use cmd = newIDbCommand sql tran :?> DbCommand
        for param in manyParam do
            clearParameters cmd
            assignDbParams cmd param
            cmd.ExecuteNonQueryAsync() |> ignore
    }
        
/// Execute a query with no results many times
let execMany (sql : string) (manyParam : DbParam list list) (conn : IDbConnection) =
    use tran = beginTran conn
    tranExecMany sql manyParam tran
    commitTran tran

/// Execute a query async with no results many times
let execManyAsync (sql : string) (manyParam : DbParam list list) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        do! tranExecManyAsync sql manyParam tran
        commitTran tran
    }

/// Try to execute query with no results many times within transction scope
let tryTranExecMany (sql : string) (manyParam : DbParam list list) (tran : IDbTransaction) =    
    try
        tranExecMany sql manyParam tran
        |> DbResult
    with ex -> DbError ex

/// Try to execute query async with no results many times within transction scope
let tryTranExecManyAsync (sql : string) (manyParam : DbParam list list) (tran : IDbTransaction) =    
    task { 
        try
            do! tranExecManyAsync sql manyParam tran
            return DbResult ()
        with ex -> return DbError ex
    }
    
/// Try to execute a query with no results many times
let tryExecMany (sql : string) (manyParam : DbParam list list) (conn : IDbConnection) =
    try
        use tran = beginTran conn
        tranExecMany sql manyParam tran
        commitTran tran
        DbResult ()
    with ex -> DbError ex

/// Try to execute a query async with no results many times
let tryExecManyAsync (sql : string) (manyParam : DbParam list list) (conn : IDbConnection) =
    task {
        try
            use tran = beginTran conn
            do! tranExecManyAsync sql manyParam tran
            commitTran tran
            return DbResult ()
        with ex -> return DbError ex
    }


/// Execute query that returns scalar result within transcation scope
let tranScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    convert (cmd.ExecuteScalar())

/// Execute query async that returns scalar result within transcation scope
let tranScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand
        let! result = cmd.ExecuteScalarAsync()
        return convert (result)
    }

/// Execute query with scalar result
let scalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let v = tranScalar sql param convert tran
    commitTran tran
    v

/// Execute query async with scalar result
let scalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        let! v = tranScalarAsync sql param convert tran
        commitTran tran
        return v
    }

/// Try to execute query that returns scalar result within transcation scope
let tryTranScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    try
        tranScalar sql param convert tran
        |> DbResult
    with ex -> DbError ex

/// Try to execute query async that returns scalar result within transcation scope
let tryTranScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    task {
        try
            let! result = tranScalarAsync sql param convert tran
            return DbResult result
        with ex -> return DbError ex
    }

/// Try to execute query with scalar result
let tryScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    try
        use tran = beginTran conn
        let result = tranScalar sql param convert tran
        commitTran tran
        DbResult result
    with ex -> DbError ex

/// Try to execute query async with scalar result
let tryScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    task {
        try
            use tran = beginTran conn
            let! result = tranScalarAsync sql param convert tran
            commitTran tran
            return DbResult result
        with ex -> return DbError ex
    }


// DataReader extensions
type IDataReader with
    member this.GetOrdinalOption (name : string) = 
        let i = this.GetOrdinal(name)        
        match this.IsDBNull(i) with 
        | true  -> None
        | false -> Some(i)

    member this.GetOption (map : int -> 'a when 'a : struct) (name : string) = 
        this.GetOrdinalOption(name)
        |> Option.map map
    
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
    
    member this.GetString (name : string)                 = match this.GetStringOption name         with Some v -> v | None -> null
    member this.GetBoolean (name : string)                = match this.GetBooleanOption name        with Some v -> v | None -> false
    member this.GetByte (name : string)                   = match this.GetByteOption name           with Some v -> v | None -> Byte.MinValue
    member this.GetChar (name : string)                   = match this.GetCharOption name           with Some v -> v | None -> Char.MinValue
    member this.GetDateTime (name : string)               = match this.GetDateTimeOption name       with Some v -> v | None -> DateTime.MinValue
    member this.GetDateTimeOffset (name : string)         = match this.GetDateTimeOffsetOption name with Some v -> v | None -> DateTimeOffset.MinValue
    member this.GetDecimal (name : string)                = match this.GetDecimalOption name        with Some v -> v | None -> 0.0M
    member this.GetDouble (name : string)                 = match this.GetDoubleOption name         with Some v -> v | None -> 0.0
    member this.GetFloat (name : string)                  = match this.GetFloatOption name          with Some v -> v | None -> 0.0
    member this.GetGuid (name : string)                   = match this.GetGuidOption name           with Some v -> v | None -> Guid.Empty
    member this.GetInt16 (name : string)                  = match this.GetInt16Option name          with Some v -> v | None -> 0s
    member this.GetInt32 (name : string)                  = match this.GetInt32Option name          with Some v -> v | None -> 0
    member this.GetInt64 (name : string)                  = match this.GetInt64Option name          with Some v -> v | None -> 0L  
    
    member this.GetNullableBoolean (name : string)        = match this.GetBooleanOption name with Some v -> Nullable<Boolean>(v) | None -> Nullable<Boolean>()
    member this.GetNullableByte (name : string)           = match this.GetByteOption name with Some v -> Nullable<Byte>(v) | None -> Nullable<Byte>()
    member this.GetNullableChar (name : string)           = match this.GetCharOption name with Some v -> Nullable<Char>(v) | None -> Nullable<Char>()
    member this.GetNullableDateTime (name : string)       = match this.GetDateTimeOption name with Some v -> Nullable<DateTime>(v) | None -> Nullable<DateTime>()
    member this.GetNullableDateTimeOffset (name : string) = match this.GetDateTimeOffsetOption name with Some v -> Nullable<DateTimeOffset>(v) | None -> Nullable<DateTimeOffset>()
    member this.GetNullableDecimal (name : string)        = match this.GetDecimalOption name with Some v -> Nullable<Decimal>(v) | None -> Nullable<Decimal>()
    member this.GetNullableDouble (name : string)         = match this.GetDoubleOption name with Some v -> Nullable<Double>(v) | None -> Nullable<Double>()
    member this.GetNullableFloat (name : string)          = this.GetNullableDouble name
    member this.GetNullableGuid (name : string)           = match this.GetGuidOption name with Some v -> Nullable<Guid>(v) | None -> Nullable<Guid>()
    member this.GetNullableInt16 (name : string)          = match this.GetInt16Option name with Some v -> Nullable<Int16>(v) | None -> Nullable<Int16>()
    member this.GetNullableInt32 (name : string)          = match this.GetInt32Option name with Some v -> Nullable<Int32>(v) | None -> Nullable<Int32>()
    member this.GetNullableInt64 (name : string)          = match this.GetInt64Option name with Some v -> Nullable<Int64>(v) | None -> Nullable<Int64>()  
        
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
        