module Donald 

open System
open System.Data

// Represents the ability to create a new IDbConnection
type DbConnectionFactory = unit -> IDbConnection

// Specifies an input parameter for an IDbCommand
[<Struct>]
type DbParam = 
    { 
        Name : string
        Value : obj 
    }

// Create new instance of IDbConnection using provided DbConnectionFactory
let createConn (createConnection : DbConnectionFactory) =
    createConnection ()    
  
// Create a new IDbTransaction
let beginTran (conn : IDbConnection) = 
    if conn.State <> ConnectionState.Open then conn.Open()
    conn.BeginTransaction()

// Rollback IDbTransaction
let rollbackTran (tran : IDbTransaction) =
    try        
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Rollback()
    with            
        | _ -> 
            reraise() 

// Attempt to commit IDbTransaction, rollback if failed.
let commitTran (tran : IDbTransaction) =
    try
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Commit() 
    with
        // Is supposed to throw System.InvalidOperationException
        // when commmited or rolled back already, but most
        // implementations do not. So in all cases try rolling back
        | _ -> 
            rollbackTran tran
            reraise()

// Create a new IDbCommand  
let newDbCommand (sql : string) (tran : IDbTransaction) =
    let cmd = tran.Connection.CreateCommand()
    cmd.CommandType <- CommandType.Text
    cmd.CommandText <- sql
    cmd.Transaction <- tran
    cmd 

// Assign DbParam to IDbCommand
let assignDbParams (cmd : IDbCommand) (dbParams : DbParam list) =
    dbParams
    |> Seq.iter (fun param ->
        let p = cmd.CreateParameter()
        p.ParameterName <- param.Name
        p.Value <- param.Value
        cmd.Parameters.Add(p) |> ignore)

let clearParameters (cmd : IDbCommand) =
    cmd.Parameters.Clear()
    
// Create a new IDbCommand  
let newCommand (sql : string) (dbParams : DbParam list) (tran : IDbTransaction) =
    let cmd = newDbCommand sql tran
    assignDbParams cmd dbParams
    cmd

// DbParam constructor
let newParam (name : string) (value : 'a) =
    { Name = name; Value = value }

// Query for multiple results within transaction scope
let tranQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    use rd = cmd.ExecuteReader()
    let results = [ while rd.Read() do yield map rd ]
    rd.Close() |> ignore
    results

// Query for single result within transaction scope
let tranQuerySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    use rd = cmd.ExecuteReader()
    if rd.Read() then Some(map rd) else None

// Execute query with no results within transction scope
let tranExec (sql : string) (param : DbParam list) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran    
    cmd.ExecuteNonQuery() |> ignore

// Execute query with no results many times within transction scope
let tranExecMany (sql : string) (manyParam : DbParam list list) (tran : IDbTransaction) =    
    use cmd = newDbCommand sql tran    
    for param in manyParam do
        clearParameters cmd
        assignDbParams cmd param
        cmd.ExecuteNonQuery() |> ignore
    commitTran tran

// Execute query that returns scalar result within transcation scope
let tranScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    convert (cmd.ExecuteScalar())

// Query for multiple results
let query (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let results = tranQuery sql param map tran
    commitTran tran
    results

// Query for single result
let querySingle (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let result = tranQuerySingle sql param map tran
    commitTran tran
    result

// Execute query with no results
let exec (sql : string) (param : DbParam list) (conn : IDbConnection) =
    use tran = beginTran conn
    tranExec sql param tran
    commitTran tran

// Execute a query with no results many times
let execMany (sql : string) (manyParam : DbParam list list) (conn : IDbConnection) =
    use tran = beginTran conn
    tranExecMany sql manyParam tran

// Execute query with scalar result
let scalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let v = tranScalar sql param convert tran
    commitTran tran
    v

type IDataReader with
    member this.GetOrdinalOption (name : string) = 
        let i = this.GetOrdinal(name)
        match this.IsDBNull(i) with 
        | true  -> None
        | false -> Some(i)

    member this.GetOption (map : int -> 'a when 'a : struct) (name : string) = 
        this.GetOrdinalOption(name)
        |> Option.map map

    member this.GetNullable (map : int -> 'a when 'a : struct)  (name : string) =
        name
        |> this.GetOption map
        |> Option.map (fun v -> Nullable<'a>(v))
        |> Option.defaultValue (Nullable<'a>())

    member this.Get (map : int -> 'a when 'a : struct) (name : string) = 
        name 
        |> this.GetOption map
        |> Option.defaultValue Unchecked.defaultof<'a>

    member this.GetString (name : string)           = this.GetOrdinalOption(name) |> Option.map (fun i -> this.GetString(i)) |> Option.defaultValue String.Empty
    member this.GetBoolean (name : string)          = name |> this.Get (fun i -> this.GetBoolean(i)) 
    member this.GetByte (name : string)             = name |> this.Get (fun i -> this.GetByte(i))
    member this.GetChar (name : string)             = name |> this.Get (fun i -> this.GetChar(i))
    member this.GetDateTime (name : string)         = name |> this.Get (fun i -> this.GetDateTime(i))
    member this.GetDecimal (name : string)          = name |> this.Get (fun i -> this.GetDecimal(i))
    member this.GetDouble (name : string)           = name |> this.Get (fun i -> this.GetDouble(i))
    member this.GetFloat (name : string)            = name |> this.Get (fun i -> this.GetFloat(i))
    member this.GetGuid (name : string)             = name |> this.Get (fun i -> this.GetGuid(i))
    member this.GetInt16 (name : string)            = name |> this.Get (fun i -> this.GetInt16(i))
    member this.GetInt32 (name : string)            = name |> this.Get (fun i -> this.GetInt32(i))
    member this.GetInt64 (name : string)            = name |> this.Get (fun i -> this.GetInt64(i))    
    
    member this.GetStringOption (name : string)     = this.GetOrdinalOption(name) |> Option.map (fun i -> this.GetString(i))
    member this.GetBooleanOption (name : string)    = name |> this.GetOption (fun i -> this.GetBoolean(i)) 
    member this.GetByteOption (name : string)       = name |> this.GetOption (fun i -> this.GetByte(i))
    member this.GetCharOption (name : string)       = name |> this.GetOption (fun i -> this.GetChar(i))
    member this.GetDateTimeOption (name : string)   = name |> this.GetOption (fun i -> this.GetDateTime(i))
    member this.GetDecimalOption (name : string)    = name |> this.GetOption (fun i -> this.GetDecimal(i))
    member this.GetDoubleOption (name : string)     = name |> this.GetOption (fun i -> this.GetDouble(i))
    member this.GetFloatOption (name : string)      = name |> this.GetOption (fun i -> this.GetFloat(i))
    member this.GetGuidOption (name : string)       = name |> this.GetOption (fun i -> this.GetGuid(i))
    member this.GetInt16Option (name : string)      = name |> this.GetOption (fun i -> this.GetInt16(i))
    member this.GetInt32Option (name : string)      = name |> this.GetOption (fun i -> this.GetInt32(i))
    member this.GetInt64Option (name : string)      = name |> this.GetOption (fun i -> this.GetInt64(i))  
  
    member this.GetNullableBoolean (name : string)  = name |> this.GetNullable (fun i -> this.GetBoolean(i)) 
    member this.GetNullableByte (name : string)     = name |> this.GetNullable (fun i -> this.GetByte(i))
    member this.GetNullableChar (name : string)     = name |> this.GetNullable (fun i -> this.GetChar(i))
    member this.GetNullableDateTime (name : string) = name |> this.GetNullable (fun i -> this.GetDateTime(i))
    member this.GetNullableDecimal (name : string)  = name |> this.GetNullable (fun i -> this.GetDecimal(i))
    member this.GetNullableDouble (name : string)   = name |> this.GetNullable (fun i -> this.GetDouble(i))
    member this.GetNullableFloat (name : string)    = name |> this.GetNullable (fun i -> this.GetFloat(i))
    member this.GetNullableGuid (name : string)     = name |> this.GetNullable (fun i -> this.GetGuid(i))
    member this.GetNullableInt16 (name : string)    = name |> this.GetNullable (fun i -> this.GetInt16(i))
    member this.GetNullableInt32 (name : string)    = name |> this.GetNullable (fun i -> this.GetInt32(i))
    member this.GetNullableInt64 (name : string)    = name |> this.GetNullable (fun i -> this.GetInt64(i))  
  