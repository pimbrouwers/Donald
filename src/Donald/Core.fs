[<AutoOpen>]
module Donald.Core

open System
open System.Data.Common

/// Details of failure to execute database command.
type DbExecutionError = 
    { Statement : string
      Error     : DbException }

/// Represents the success or failure of a database command execution.
type DbResult<'a> = Result<'a, DbExecutionError>

/// Details of failure to cast a IDataRecord field.
type DataReaderCastError = 
    { FieldName : string 
      Error     : InvalidCastException }

exception ConnectionBusyError
exception CouldNotOpenConnectionError of exn
exception CouldNotBeginTransactionError of exn
exception CouldNotCommitTransactionError of exn
exception CouldNotRollbackTransactionError of exn
exception FailedExecutionError of DbExecutionError
exception FailiedCastException of DataReaderCastError
  
/// Represents the supported data types for database IO.
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
    { 
        Name : String
        Value : SqlType
    }

module DbParam = 
    /// Create a new DbParam from raw inputs.
    let create (name : string) (value : SqlType) =
        { Name = name; Value = value }

/// Type abbreviation for (string * SqlType) list.
type RawDbParams = (string * SqlType) list

/// Type abbreviation for DbParam list.
type DbParams = DbParam list

module DbParams =
    /// Create a new DbParam list from raw inputs.
    let create (lst : RawDbParams) =
        [ for k, v in lst -> DbParam.create k v ]
