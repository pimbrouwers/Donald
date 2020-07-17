[<AutoOpen>]
module Donald.Core

open System
open System.Data

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

