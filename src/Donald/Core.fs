namespace Donald

open System
open System.Data
open System.Runtime.Serialization
open System.Threading

/// Represents a configurable database command.
type DbUnit (cmd : IDbCommand) =
    let commandBehavior = CommandBehavior.SequentialAccess

    member _.Command = cmd
    member val CommandBehavior = CommandBehavior.SequentialAccess with get, set
    member val CancellationToken = CancellationToken.None with get,set

    interface IDisposable with
        member x.Dispose () =
            x.Command.Dispose ()

/// Details of failure to connection to a database/server.
type DbConnectionException =
    inherit Exception
    val ConnectionString : string option
    new() = { inherit Exception(); ConnectionString = None }
    new(message : string) = { inherit Exception(message); ConnectionString = None }
    new(message : string, inner : Exception) = { inherit Exception(message, inner); ConnectionString = None }
    new(info : SerializationInfo, context : StreamingContext) = { inherit Exception(info, context); ConnectionString = None }
    new(connection : IDbConnection, inner : Exception) = { inherit Exception("Failed to establish database connection", inner); ConnectionString = Some connection.ConnectionString}

/// Details the steps of database a transaction.
type DbTransactionStep =  TxBegin | TxCommit | TxRollback

/// Details of failure to execute database command or transaction.
type DbExecutionException =
    inherit Exception
    val Statement : string option
    val Step : DbTransactionStep option
    new() = { inherit Exception(); Statement = None; Step = None }
    new(message : string) = { inherit Exception(message); Statement = None; Step = None }
    new(message : string, inner : Exception) = { inherit Exception(message, inner); Statement = None; Step = None }
    new(info : SerializationInfo, context : StreamingContext) = { inherit Exception(info, context); Statement = None; Step = None }
    new(cmd : IDbCommand, inner : Exception) = { inherit Exception("Failed to process database command", inner); Statement = Some cmd.CommandText; Step = None }
    new(step : DbTransactionStep, inner : Exception) = { inherit Exception("Failed to process transaction", inner); Statement = None; Step = Some step }

/// Details of failure to access and/or cast an IDataRecord field.
type DbReaderException =
    inherit Exception
    val FieldName : string option
    new() = { inherit Exception(); FieldName = None }
    new(message : string) = { inherit Exception(message); FieldName = None }
    new(message : string, inner : Exception) = { inherit Exception(message, inner); FieldName = None }
    new(info : SerializationInfo, context : StreamingContext) = { inherit Exception(info, context); FieldName = None }
    new(fieldName : string, inner : IndexOutOfRangeException) = { inherit Exception($"Failed to read database field: '{fieldName}'", inner); FieldName = Some fieldName }
    new(fieldName : string, inner : InvalidCastException) = { inherit Exception($"Failed to read database field: '{fieldName}'", inner); FieldName = Some fieldName }

/// Represents the supported data types for database IO.
[<RequireQualifiedAccess>]
type SqlType =
    | Null
    | String     of string
    | AnsiString of string
    | Boolean    of bool
    | Byte       of byte
    | Char       of char
    | AnsiChar   of char
    | Decimal    of decimal
    | Double     of double
    | Float      of float
    | Guid       of Guid
    | Int16      of int16
    | Int32      of int32
    | Int        of int32
    | Int64      of int64
    | DateTime   of DateTime
    | Bytes      of byte[]

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