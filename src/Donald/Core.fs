[<AutoOpen>]
module Donald.Core

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

type DbExecutionError = 
    { Statement : string
      Error     : DbException }

type DbResult<'a> = Result<'a, DbExecutionError>

exception ConnectionBusyError
exception CouldNotOpenConnectionError of exn
exception CouldNotBeginTransactionError of exn
exception CouldNotCommitTransactionError of exn
exception CouldNotRollbackTransactionError of exn
exception FailedExecutionError of DbExecutionError

  
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

module DbParam = 
    let create (name : string) (value : SqlType) =
        { Name = name; Value = value }

/// Type abbreviation for DbParam list
type DbParams = DbParam list

module DbParams =
    let create (lst : (string * SqlType) list) =
        [ for k, v in lst -> DbParam.create k v ]

///
type DbRecordMap<'a> = IDataRecord -> 'a

type IDbConnection with
    member this.NewCommand(commandType : CommandType, sql : string) =
        let cmd = this.CreateCommand()
        cmd.CommandType <- commandType
        cmd.CommandText <- sql
        cmd

    member this.TryOpenConnection()  =        
        try
            if this.State = ConnectionState.Closed then 
                this.Open()             
        with ex -> 
            raise (CouldNotOpenConnectionError ex) 

    member this.TryBeginTransaction()  =        
        try
            this.TryOpenConnection()
            this.BeginTransaction()
        with 
        | CouldNotOpenConnectionError ex -> reraise()
        | ex -> raise (CouldNotBeginTransactionError ex)
 
type IDbTransaction with
    member this.NewCommand(commandType : CommandType, sql : string) =
        let cmd = this.Connection.NewCommand(commandType, sql)
        cmd.Transaction <- this        
        cmd

    member internal this.NewDbCommand(commandType : CommandType, sql : string) =
        this.NewCommand(commandType, sql) :?> DbCommand

    member this.TryRollback() =
        try        
            if not(isNull this) 
               && not(isNull this.Connection) then this.Rollback()
        with ex  -> 
            raise (CouldNotRollbackTransactionError ex) 

    member this.TryCommit() =
        try
            if not(isNull this) 
               && not(isNull this.Connection) then this.Commit() 
        with ex  -> 
            /// Is supposed to throw System.InvalidOperationException
            /// when commmited or rolled back already, but most
            /// implementations do not. So in all cases try rolling back
            this.TryRollback()
            raise (CouldNotCommitTransactionError ex)             

type IDbCommand with
    member this.SetDbParams(dbParams : DbParams) =
        let setParamValue (p : IDbDataParameter) (v : obj) =
            if v = null then p.Value <- DBNull.Value
            else p.Value <- v

        this.Parameters.Clear() // clear to ensure a clean working set

        for param in dbParams do
            let p = this.CreateParameter()
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

            this.Parameters.Add(p)
            |> ignore
        this

    member private this.TryDo (fn : IDbCommand -> 'a) : 'a =
        try 
            fn this
        with
        | :? DbException as ex -> raise (FailedExecutionError ({ Statement = this.CommandText; Error = ex }))

    member this.Exec() =
        this.TryDo (fun this -> this.ExecuteNonQuery() |> ignore)

    member this.ExecReader() =
        this.TryDo (fun this -> this.ExecuteReader())

    member this.ExecScalar() =
        this.TryDo (fun this -> this.ExecuteScalar())

type DbCommand with
    member private this.TryDoAsync (fn : DbCommand -> Task<'a>) : Task<'a> = task {
        try 
            return! fn this             
        with
        | :? DbException as ex -> 
            return raise (FailedExecutionError ({ Statement = this.CommandText; Error = ex }))
    }

    member this.SetDbParams(param : DbParams) =
        (this :> IDbCommand).SetDbParams(param) :?> DbCommand
            
    member this.ExecAsync() =
        this.TryDoAsync (fun this -> this.ExecuteNonQueryAsync())

    member this.ExecReaderAsync() =
        this.TryDoAsync (fun this -> this.ExecuteReaderAsync())

    member this.ExecScalarAsync() =
        this.TryDoAsync (fun cmd -> cmd.ExecuteScalarAsync())
