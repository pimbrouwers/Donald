[<AutoOpen>]
module Donald.Core

open System
open System.Data.Common
open System.Threading.Tasks

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
