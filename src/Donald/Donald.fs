namespace Donald

open System
open System.Data
open System.Data.Common
open System.IO
open System.Threading.Tasks
open FSharp.Control.Tasks

/// Details of failure to connection to a database/server.
type DbConnectionError = 
    { ConnectionString : string
      Error            : exn }

/// Details of failure to execute database command.
type DbExecutionError = 
    { Statement : string
      Error     : DbException }

/// Details of failure to cast a IDataRecord field.
type DataReaderCastError = 
    { FieldName : string 
      Error     : InvalidCastException }

exception CouldNotOpenConnectionException of DbConnectionError
exception CouldNotBeginTransactionException of exn
exception CouldNotCommitTransactionException of exn
exception CouldNotRollbackTransactionException of exn
exception FailedExecutionException of DbExecutionError
exception FailiedCastException of DataReaderCastError

/// Represents the supported data types for database IO.
[<RequireQualifiedAccess>]
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

/// Specifies an input parameter for an IDbCommand.
[<Struct>]
type DbParam = 
    { Name : String
      Value : SqlType }

/// Type abbreviation for (string * SqlType) list.
type RawDbParams = (string * SqlType) list

/// Type abbreviation for DbParam list.
type DbParams = DbParam list

module DbParams =
    /// Create a new DbParam list from raw inputs.
    let create (lst : RawDbParams) =
        [ for k, v in lst -> { Name = k; Value = v } ]

[<AutoOpen>]        
module Connection = 
    type IDbConnection with
        /// Safely attempt to open a new IDbTransaction or
        /// return CouldNotOpenConnectionException.
        member this.TryOpenConnection()  =        
            try
                if this.State = ConnectionState.Closed then 
                    this.Open()             
            with ex -> 
                let error = 
                    { ConnectionString = this.ConnectionString 
                      Error = ex }
                raise (CouldNotOpenConnectionException error) 

        /// Safely attempt to create a new IDbTransaction or
        /// return CouldNotBeginTransactionException.
        member this.TryBeginTransaction()  =        
            try
                this.TryOpenConnection()
                this.BeginTransaction()
            with         
            | ex -> raise (CouldNotBeginTransactionException ex)

[<AutoOpen>]
module Transaction = 
    type IDbTransaction with
        /// Safely attempt to rollback an IDbTransaction.
        member this.TryRollback() =
            try        
                if not(isNull this) 
                   && not(isNull this.Connection) then this.Rollback()
            with ex  -> 
                raise (CouldNotRollbackTransactionException ex) 

        /// Safely attempt to commit an IDbTransaction.
        /// Will rollback in the case of Exception.
        member this.TryCommit() =
            try
                if not(isNull this) 
                   && not(isNull this.Connection) then this.Commit() 
            with ex -> 
                /// Is supposed to throw System.InvalidOperationException
                /// when commmited or rolled back already, but most
                /// implementations do not. So in all cases try rolling back
                this.TryRollback()
                raise (CouldNotCommitTransactionException ex)      

[<AutoOpen>]
module Command = 
    type IDbCommand with
        member internal this.SetDbParams(dbParams : DbParams) =
            let setParamValue (p : IDbDataParameter) (v : obj) =
                if isNull v then p.Value <- DBNull.Value
                else p.Value <- v

            this.Parameters.Clear() // clear to ensure a clean working set

            for param in dbParams do
                let p = this.CreateParameter()
                p.ParameterName <- param.Name
                
                match param.Value with
                | SqlType.Null -> 
                    p.Value <- DBNull.Value

                | SqlType.String v -> 
                    p.DbType <- DbType.String
                    setParamValue p v

                | SqlType.AnsiString v ->
                    p.DbType <- DbType.AnsiString
                    setParamValue p v

                | SqlType.Boolean v -> 
                    p.DbType <- DbType.Boolean
                    setParamValue p v

                | SqlType.Byte v -> 
                    p.DbType <- DbType.Byte
                    setParamValue p v

                | SqlType.Char v -> 
                    p.DbType <- DbType.AnsiString
                    setParamValue p v

                | SqlType.AnsiChar v ->
                    p.DbType <- DbType.String
                    setParamValue p v

                | SqlType.Decimal v -> 
                    p.DbType <- DbType.Decimal
                    setParamValue p v

                | SqlType.Double v
                | SqlType.Float v ->
                    p.DbType <- DbType.Double
                    setParamValue p v 

                | SqlType.Int16 v -> 
                    p.DbType <- DbType.Int16
                    setParamValue p v

                | SqlType.Int32 v 
                | SqlType.Int v -> 
                    p.DbType <- DbType.Int32
                    setParamValue p v

                | SqlType.Int64 v -> 
                    p.DbType <- DbType.Int64
                    setParamValue p v
                    
                | SqlType.Guid v -> 
                    p.DbType <- DbType.Guid
                    setParamValue p v

                | SqlType.DateTime v -> 
                    p.DbType <- DbType.DateTime
                    setParamValue p v

                | SqlType.DateTimeOffset v ->
                    p.DbType <- DbType.DateTimeOffset
                    setParamValue p v

                | SqlType.Bytes v -> 
                    p.DbType <- DbType.Binary
                    setParamValue p v

                this.Parameters.Add(p)
                |> ignore
            this

        member private this.TryDo (fn : IDbCommand -> 'a) : 'a =        
            try 
                fn this
            with
            | :? DbException as ex -> raise (FailedExecutionException ({ Statement = this.CommandText; Error = ex }))

        member internal this.Exec () =
            this.TryDo (fun this -> this.ExecuteNonQuery() |> ignore)

        member internal this.ExecReader (?cmdBehaviour : CommandBehavior) =        
            this.TryDo (fun this -> this.ExecuteReader(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

    type DbCommand with
        member private this.TryDoAsync (fn : DbCommand -> Task<'a>) : Task<'a> = 
            try 
                fn this             
            with
            | :? DbException as ex -> raise (FailedExecutionException ({ Statement = this.CommandText; Error = ex }))    

        member internal this.SetDbParams(param : DbParams) =
            (this :> IDbCommand).SetDbParams(param) :?> DbCommand
                
        member internal this.ExecAsync() =
            this.TryDoAsync (fun this -> this.ExecuteNonQueryAsync())

        member internal this.ExecReaderAsync(?cmdBehaviour : CommandBehavior) =
            this.TryDoAsync (fun this -> this.ExecuteReaderAsync(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

[<AutoOpen>]
module DataReader =
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

[<RequireQualifiedAccess>]
module Db =
    /// Create a new IDbCommand instance using the provided IDbConnection.
    let newCommand (commandText : string) (conn : IDbConnection) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- commandText
        cmd

    /// Configure the CommandType for the provided IDbCommand
    let setCommandType (commandType : CommandType) (cmd : IDbCommand) =
        cmd.CommandType <- commandType
        cmd

    /// Configure the command parameters for the provided IDbCommand
    let setParams (param : RawDbParams) (cmd : IDbCommand) =
        cmd.SetDbParams(DbParams.create param)

    /// Configure the timeout for the provided IDbCommand
    let setTimeout (commandTimeout : int) (cmd : IDbCommand) =
        cmd.CommandTimeout <- commandTimeout
        cmd

    /// Configure the transaction for the provided IDbCommand
    let setTransaction (tran : IDbTransaction) (cmd : IDbCommand) =
        cmd.Transaction <- tran
        cmd

    let private tryDo (fn : IDbCommand -> 'a) (cmd : IDbCommand) : Result<'a, DbExecutionError> =
        try
            cmd.Connection.TryOpenConnection() |> ignore
            let result = fn cmd
            cmd.Dispose()
            Ok result
        with
        | FailedExecutionException e -> Error e

    /// Execute parameterized query with no results.
    let exec (cmd : IDbCommand) : Result<unit, DbExecutionError> =
        tryDo (fun cmd -> cmd.Exec()) cmd

    /// Execute parameterized query many times with no results.
    let execMany (param : RawDbParams list) (cmd : IDbCommand) : Result<unit, DbExecutionError> =
        try
            for p in param do
                let dbParams = DbParams.create p
                cmd.SetDbParams(dbParams).Exec() |> ignore

            Ok ()
        with
        | FailedExecutionException e -> Error e

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (cmd : IDbCommand) : Result<'a, DbExecutionError> =
        tryDo (fun cmd ->
            let value = cmd.ExecuteScalar()
            convert value)
            cmd

    /// Execute parameterized query, enumerate all records and apply mapping.
    let query (map : 'reader -> 'a when 'reader :> IDataReader) (cmd : IDbCommand) : Result<'a list, DbExecutionError> =
        tryDo (fun cmd ->
            use rd = cmd.ExecReader() :?> 'reader
            let results = [ while rd.Read() do yield map rd ]
            rd.Close() |> ignore
            results)
            cmd

    /// Execute paramterized query, read only first record and apply mapping.
    let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (cmd : IDbCommand) : Result<'a option, DbExecutionError> =
        tryDo
            (fun cmd ->
                use rd = cmd.ExecReader() :?> 'reader
                let result = if rd.Read() then Some(map rd) else None
                rd.Close() |> ignore
                result)
            cmd

    /// Execute paramterized query and return IDataReader
    let read (cmd : IDbCommand) : IDataReader =
        cmd.ExecReader(CommandBehavior.Default)

    module Async =        
        let private tryDoAsync (fn : DbCommand -> Task<'a>) (cmd : IDbCommand) : Task<Result<'a, DbExecutionError>> =
            task {
                try
                    cmd.Connection.TryOpenConnection() |> ignore
                    let! result = fn (cmd :?> DbCommand)
                    return (Ok result)
                with
                | FailedExecutionException e -> return Error e
            }

        /// Asynchronously execute parameterized query with no results.
        let exec (cmd : IDbCommand) : Task<Result<unit, DbExecutionError>> =
            let inner = fun (cmd : DbCommand) -> task {
                let! _ = cmd.ExecAsync()
                return ()
            }
            tryDoAsync inner cmd

        /// Asynchronously execute parameterized query many times with no results
        let execMany (param : RawDbParams list) (cmd : IDbCommand) : Task<Result<unit, DbExecutionError>> =
            let inner = fun (cmd : DbCommand) -> task {
                for p in param do
                    let dbParams = DbParams.create p
                    let! _ = cmd.SetDbParams(dbParams).ExecAsync()
                    ()

                return ()
            }
            tryDoAsync inner cmd

        /// Execute scalar query and box the result.
        let scalar (convert : obj -> 'a) (cmd : IDbCommand) : Task<Result<'a, DbExecutionError>> =
            let inner = fun (cmd : DbCommand) -> task {
                let! value = cmd.ExecuteScalarAsync()
                return convert value
            }
            tryDoAsync inner cmd

        /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
        let query (map : 'reader -> 'a when 'reader :> IDataReader) (cmd : IDbCommand) : Task<Result<'a list, DbExecutionError>> =
            let inner = fun (cmd : IDbCommand) -> task {
                use! rd = (cmd :?> DbCommand).ExecReaderAsync()
                let rd' = rd :?> 'reader
                let results = [ while rd.Read() do map rd' ]
                rd.Close() |> ignore
                return results
            }
            tryDoAsync inner cmd

        /// Asynchronously execute paramterized query, read only first record and apply mapping.
        let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (cmd : IDbCommand) : Task<Result<'a option, DbExecutionError>> =
            let inner = fun (cmd : DbCommand) -> task {
                use! rd = cmd.ExecReaderAsync()
                let rd' = rd :?> 'reader
                let result = if rd.Read() then Some(map rd') else None
                rd.Close() |> ignore
                return result
            }
            tryDoAsync inner cmd

        /// Asynchronously execute paramterized query and return IDataReader
        let read (cmd : IDbCommand) : Task<IDataReader> =
            let cmd' = cmd :?> DbCommand

            task {
                let! rd = cmd'.ExecReaderAsync(CommandBehavior.Default)
                let result = rd :> IDataReader
                return result
            }            

[<AutoOpen>]
module CommandBuilder = 
    type CommandSpec<'a> = 
        {
            Connection     : IDbConnection
            Transaction    : IDbTransaction option
            CommandType    : CommandType
            CommandTimeout : int option
            Statement      : string 
            Param          : RawDbParams
        }
        static member Default (conn : IDbConnection) = 
            {
                Connection     = conn
                Transaction    = None
                CommandType    = CommandType.Text
                CommandTimeout = None
                Statement      = ""
                Param          = []
            }

    /// Computation expression for generating IDbCommand instances.
    type DbCommandBuilder<'a>(conn : IDbConnection) =
        member _.Yield(_) = CommandSpec<'a>.Default (conn)

        member _.Run(spec : CommandSpec<'a>) =         
            let cmd = 
                spec.Connection
                |> Db.newCommand spec.Statement
                |> Db.setCommandType spec.CommandType
                |> Db.setParams spec.Param
                
            match spec.Transaction, spec.CommandTimeout with 
            | Some tran, Some timeout -> cmd |> Db.setTimeout timeout |> Db.setTransaction tran 
            | Some tran, None         -> Db.setTransaction tran cmd
            | None, Some timeout      -> Db.setTimeout timeout cmd
            | None, None              -> cmd
            
        [<CustomOperation("cmdParam")>]
        /// Add DbParams.
        member _.DbParams (spec : CommandSpec<'a>, param : RawDbParams) =
            { spec with Param = param }

        [<CustomOperation("cmdText")>]
        /// Set statement text.
        member _.Statement (spec : CommandSpec<'a>, statement : string) =
            { spec with Statement = statement }

        [<CustomOperation("cmdTran")>]
        /// Set transaction.
        member _.Transaction (spec : CommandSpec<'a>, tran : IDbTransaction) =
            { spec with Transaction = Some tran }
        
        [<CustomOperation("cmdType")>]
        /// Set command type (default: CommandType.Text).
        member _.CommandType (spec : CommandSpec<'a>, commandType : CommandType) =
            { spec with CommandType = commandType }
        
        [<CustomOperation("cmdTimeout")>]
        /// Set command timeout.
        member _.CommandTimeout (spec : CommandSpec<'a>, timeout : TimeSpan) =
            { spec with CommandTimeout = Some <| int timeout.TotalSeconds }

    /// Computation expression for generating IDbCommand instances.
    let dbCommand<'a> (conn : IDbConnection) = DbCommandBuilder<'a>(conn)