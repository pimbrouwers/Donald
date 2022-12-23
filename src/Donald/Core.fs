namespace Donald

open System
open System.Data
open System.Data.Common
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
type DbConnectionError =
    { ConnectionString : string
      Error : exn }

/// Details the steps of database a transaction.
type DbTransactionStep =  TxBegin | TxCommit | TxRollback

/// Details of transaction failure.
type DbTransactionError =
    { Step : DbTransactionStep
      Error : exn }

/// Details of failure to execute database command.
type DbExecutionError =
    { Statement : string
      Error : DbException }

/// Details of failure to cast a IDataRecord field.
type DataReaderCastError =
    { FieldName : string
      Error : InvalidCastException }

type DataReaderOutOfRangeError =
    { FieldName : string
      Error : IndexOutOfRangeException }

type DbError =
    | DbConnectionError of DbConnectionError
    | DbTransactionError of DbTransactionError
    | DbExecutionError of DbExecutionError
    | DataReaderCastError of DataReaderCastError
    | DataReaderOutOfRangeError of DataReaderOutOfRangeError

exception DbFailureException of DbError

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