# Donald

[![NuGet Version](https://img.shields.io/nuget/v/Donald.svg)](https://www.nuget.org/packages/Donald)
[![build](https://github.com/pimbrouwers/Donald/actions/workflows/build.yml/badge.svg)](https://github.com/pimbrouwers/Donald/actions/workflows/build.yml)

Meet [Donald](https://en.wikipedia.org/wiki/Donald_D._Chamberlin) (Chamberlin).

If you're a programmer and have used a database, he's impacted your life in a big way.

This library is named after him.

> Honorable mention goes to [@dsyme](https://github.com/dsyme) another important Donald and F#'s [BDFL](https://en.wikipedia.org/wiki/Benevolent_dictator_for_life).

## Key Features

Donald is a generic library that aims to make working with [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) safer and more succinct. It is an entirely generic abstraction, and will work with all ADO.NET implementations.

## Design Goals

- Support all ADO implementations
- Provide a succinct, type-safe API for interacting with databases
- Enable asynchronuos workflows
- Make object mapping easier
- Improve data access performance
- Provide additional context during exceptions

## Getting Started

Install the [Donald](https://www.nuget.org/packages/Donald/) NuGet package:

```
PM>  Install-Package Donald
```

Or using the dotnet CLI
```cmd
dotnet add package Donald
```

### Quick Start

```fsharp
open Donald

type Author =
    { FullName : string }

let authors (conn : IDbConnection) : Author list =
    conn
    |> Db.newCommand "
        SELECT  full_name
        FROM    author
        WHERE   author_id = @author_id"
    |> Db.setParams [
        "author_id", SqlType.Int32 1 ]
    |> Db.query (fun rd ->
        { FullName = rd.ReadString "full_name" })
```

## An Example using SQLite

For this example, assume we have an `IDbConnection` named `conn`:

> Reminder: Donald will work with __any__ ADO implementation (SQL Server, SQLite, MySQL, Postgresql etc.).

Consider the following model:

```fsharp
type Author =
    { AuthorId : int
      FullName : string }

module Author -
    let ofDataReader (rd : IDataReader) : Author =
        { AuthorId = rd.ReadInt32 "author_id"
          FullName = rd.ReadString "full_name" }
```

### Query for multiple strongly-typed results

> Important: Donald is set to use `CommandBehavior.SequentialAccess` by default. See [performance](#performance) for more information.

```fsharp
conn
|> Db.newCommand "SELECT author_id, full_name FROM author"
|> Db.query Author.ofDataReader // Author list

// Async
conn
|> Db.newCommand "SELECT author_id, full_name FROM author"
|> Db.Async.query Author.ofDataReader // Task<Author list>
```

### Query for a single strongly-typed result

```fsharp
conn
|> Db.newCommand "SELECT author_id, full_name FROM author"
|> Db.setParams [ "author_id", SqlType.Int32 1 ]
|> Db.querySingle Author.ofDataReader // Author option

// Async
conn
|> Db.newCommand "SELECT author_id, full_name FROM author"
|> Db.setParams [ "author_id", SqlType.Int32 1 ]
|> Db.Async.querySingle Author.ofDataReader // Task<Author option>
```

### Execute a statement

```fsharp
conn
|> Db.newCommand "INSERT INTO author (full_name)"
|> Db.setParams [ "full_name", SqlType.String "John Doe" ]
|> Db.exec // unit

// Async
conn
|> Db.newCommand "INSERT INTO author (full_name)"
|> Db.setParams [ "full_name", SqlType.String "John Doe" ]
|> Db.Async.exec // Task<unit>
```

### Execute a statement many times

```fsharp
conn
|> Db.newCommand "INSERT INTO author (full_name)"
|> Db.execMany [
    "full_name", SqlType.String "John Doe"
    "full_name", SqlType.String "Jane Doe" ] // unit

// Async
conn
|> Db.newCommand "INSERT INTO author (full_name)"
|> Db.Async.execMany [
    "full_name", SqlType.String "John Doe"
    "full_name", SqlType.String "Jane Doe" ] //Task<unit>
```

### Execute statements within an explicit transaction

This can be accomplished in two ways:

1. Using `Db.batch` or `Db.Async.batch` which processes the action in an *all-or-none* fashion.

```fsharp
conn
|> Db.batch (fun tran ->
    for fullName in [ "John Doe"; "Jane Doe" ] do
        tran
        |> Db.newCommandForTransaction "INSERT INTO author (full_name) VALUES (@full_name)"
        |> Db.setParams ["full_name", SqlType.String fullName ]
        |> Db.exec)
```

2. Using the extension methods: `TryBeginTransaction()`, `TryCommit()` and `TryRollback()`.

```fsharp
// Safely begin transaction or throw CouldNotBeginTransactionError on failure
use tran = conn.TryBeginTransaction()

for fullName in [ "John Doe"; "Jane Doe" ] do
    tran
    |> Db.newCommandForTransaction "INSERT INTO author (full_name) VALUES (@full_name)"
    |> Db.setParams ["full_name", SqlType.String fullName ]
    |> Db.exec

// Attempt to commit, will rollback automatically on failure, or throw DbTransactionException
tran.TryCommit ()

// Will rollback or throw DbTransactionException
// tran.TryRollback ()
```

## Command Parameters

Command parameters are represented by `SqlType` which contains a case for all relevant types.

```fsharp
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

let p1 : SqlType = SqlType.Null
let p2 : SqlType = SqlType.Int32 1
```

Helpers also exist which implicitly call the respective F# conversion function. Which can be especially useful when you are working with value types in your program.

```fsharp
let p1 : SqlType = sqlInt32 "1" // equivalent to SqlType.Int32 (int "1")
```

###

## Reading Values

To make obtaining values from reader more straight-forward, 2 sets of extension methods are available for:
1. Get value, automatically defaulted
2. Get value as `option<'a>`

Assuming we have an active `IDataReader` called `rd` and are currently reading a row, the following extension methods are available to simplify reading values:

```fsharp
rd.ReadString "some_field"         // string -> string
rd.ReadBoolean "some_field"        // string -> bool
rd.ReadByte "some_field"           // string -> byte
rd.ReadChar "some_field"           // string -> char
rd.ReadDateTime "some_field"       // string -> DateTime
rd.ReadDecimal "some_field"        // string -> Decimal
rd.ReadDouble "some_field"         // string -> Double
rd.ReadFloat "some_field"          // string -> float32
rd.ReadGuid "some_field"           // string -> Guid
rd.ReadInt16 "some_field"          // string -> int16
rd.ReadInt32 "some_field"          // string -> int32
rd.ReadInt64 "some_field"          // string -> int64
rd.ReadBytes "some_field"          // string -> byte[]

rd.ReadStringOption "some_field"   // string -> string option
rd.ReadBooleanOption "some_field"  // string -> bool option
rd.ReadByteOption "some_field"     // string -> byte option
rd.ReadCharOption "some_field"     // string -> char option
rd.ReadDateTimeOption "some_field" // string -> DateTime option
rd.ReadDecimalOption "some_field"  // string -> Decimal option
rd.ReadDoubleOption "some_field"   // string -> Double option
rd.ReadFloatOption "some_field"    // string -> float32 option
rd.ReadGuidOption "some_field"     // string -> Guid option
rd.ReadInt16Option "some_field"    // string -> int16 option
rd.ReadInt32Option "some_field"    // string -> int32 option
rd.ReadInt64Option "some_field"    // string -> int64 option
rd.ReadBytesOption "some_field"    // string -> byte[] option
```

> If you need an explicit `Nullable<'a>` you can use `Option.asNullable`.

## Exceptions

Several custom exceptions exist which interleave the exceptions thrown by ADO.NET with contextually relevant metadata.

```fsharp
/// Details of failure to connection to a database/server.
type DbConnectionException =
    inherit Exception
    val ConnectionString : string option

/// Details of failure to execute database command or transaction.
type DbExecutionException =
    inherit Exception
    val Statement : string option
    val Step : DbTransactionStep option

/// Details of failure to access and/or cast an IDataRecord field.
type DbReaderException =
    inherit Exception
    val FieldName : string option

/// Details of failure to commit or rollback an IDbTransaction
type DbTransactionException =
    inherit Exception
    val Step : DbTransactionStep
```

## Performance

By default, the `IDataReader` is consumed using `CommandBehavior.SequentialAccess`. This allows the rows and columns to be read in chunks (i.e., streamed), but forward-only. As opposed to being completely read into memory all at once, and readable in any direction. The benefits of this are particular felt when reading large CLOB (string) and BLOB (binary) data. But is also a measureable performance gain for standard query results as well.

The only nuance to sequential access is that **columns must be read in the same order found in the `SELECT` clause**. Aside from that, there is no noticeable difference from the perspective of a library consumer.

Configuring `CommandBehavior` can be done two ways:

```fsharp
let sql = "SELECT author_id, full_name FROM author"

conn
|> Db.newCommand sql
|> Db.setCommandBehavior CommandBehavior.Default
|> Db.query Author.ofDataReader
```

## Find a bug?

There's an [issue](https://github.com/pimbrouwers/Donald/issues) for that.

## License

Built with ♥ by [Pim Brouwers](https://github.com/pimbrouwers) in Toronto, ON. Licensed under [Apache License 2.0](https://github.com/pimbrouwers/Donald/blob/master/LICENSE).
