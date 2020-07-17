[<AutoOpen>]
module Donald.DbCommand

open System
open System.Data

/// Assign DbParam to IDbCommand
let assignDbParams (cmd : IDbCommand) (dbParams : DbParam list) =
    let setParamValue (p : IDbDataParameter) (v : obj) =
        if v = null then p.Value <- DBNull.Value
        else p.Value <- v

    dbParams
    |> Seq.iter (fun param ->
        let p = cmd.CreateParameter()        
        p.ParameterName <- param.Name
        
        match param.Value with
        | Null -> 
            p.Value <- DBNull.Value

        | String v -> 
            p.DbType <- DbType.String
            setParamValue p v

        | AnsiString v ->
            p.DbType <- DbType.AnsiString
            setParamValue p v

        | Boolean v -> 
            p.DbType <- DbType.Boolean
            setParamValue p v

        | Byte v -> 
            p.DbType <- DbType.Byte
            setParamValue p v

        | Char v -> 
            p.DbType <- DbType.AnsiString
            setParamValue p v

        | AnsiChar v ->
            p.DbType <- DbType.String
            setParamValue p v

        | Decimal v -> 
            p.DbType <- DbType.Decimal
            setParamValue p v

        | Double v
        | Float v ->
            p.DbType <- DbType.Double
            setParamValue p v 

        | Int16 v -> 
            p.DbType <- DbType.Int16
            setParamValue p v

        | Int32 v 
        | Int v -> 
            p.DbType <- DbType.Int32
            setParamValue p v

        | Int64 v -> 
            p.DbType <- DbType.Int64
            setParamValue p v
            
        | Guid v -> 
            p.DbType <- DbType.Guid
            setParamValue p v

        | DateTime v -> 
            p.DbType <- DbType.DateTime
            setParamValue p v

        | DateTimeOffset v ->
            p.DbType <- DbType.DateTimeOffset
            setParamValue p v

        | Bytes v -> 
            p.DbType <- DbType.Binary
            setParamValue p v

        cmd.Parameters.Add(p) |> ignore)

/// Clear all parameters from IDbCommand
let clearParameters (cmd : IDbCommand) =
    cmd.Parameters.Clear()

/// DbParam constructor
let newParam (name : string) (value : SqlType) =
    { Name = name; Value = value }
   
/// Create a new IDbCommand  
let newIDbCommand (commandType : CommandType) (sql : string) (tran : IDbTransaction) =
    let cmd = tran.Connection.CreateCommand()
    cmd.CommandType <- commandType
    cmd.CommandText <- sql
    cmd.Transaction <- tran
    cmd 

/// Create a new CommanType.Text IDbCommand  
let newTextDbCommand (sql : string) (tran : IDbTransaction) =
    newIDbCommand CommandType.Text sql tran

/// Create a new CommanType.StoredProcedure IDbCommand  
let newSprocDbCommand (sprocName : string) (tran : IDbTransaction) =
    newIDbCommand CommandType.StoredProcedure sprocName tran

/// Create a new CommandType.Text IDbCommand  
let newCommand (sql : string) (dbParams : DbParam list) (tran : IDbTransaction) =
    let cmd = newTextDbCommand sql tran
    assignDbParams cmd dbParams
    cmd

/// Create a new CommandType.Text IDbCommand  
let newSproc (sprocName : string) (dbParams : DbParam list) (tran : IDbTransaction) =
    let cmd = newSprocDbCommand sprocName tran
    assignDbParams cmd dbParams
    cmd



