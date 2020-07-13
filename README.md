# Donald

[![NuGet Version](https://img.shields.io/nuget/v/Donald.svg)](https://www.nuget.org/packages/Donald)
[![Build Status](https://travis-ci.org/pimbrouwers/Donald.svg?branch=master)](https://travis-ci.org/pimbrouwers/Donald)

Meet [Donald](https://en.wikipedia.org/wiki/Donald_D._Chamberlin). 

If you're a programmer and have used a database, he's impacted your life in a big way. 

This library is named after him.

> Honorable mention goes to [@dysme](https://github.com/dsyme) another important Donald and F#'s [BDFL](https://en.wikipedia.org/wiki/Benevolent_dictator_for_life).

## Features

Donald is a well-tested library that aims to make working with [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) a little bit more succinct. It is an entirely generic abstraction, and will work with all ADO implementations.

Functional wrappers are available for all the `IDbCommand` methods: `ExecuteNonQuery()`, `ExecuteScalar()` & `ExecuteReader()` and a full-suite of `IDataReader` extension methods to make retrieving values safer and more direct.

> If you came looking for an ORM, this is not your light saber. And may the force be with you.

Key features:

- Generic, supports all ADO implementations
- A natural DSL for interacting with databases
- Full async support
- Opt-in explicit error flow control
- `IDataReader` extensions to facilitate mapping

## Getting Started

Install the [Donald](https://www.nuget.org/packages/Donald/) NuGet package:

```
PM>  Install-Package Donald
```

Or using the dotnet CLI
```cmd
dotnet add package Donald
```

## An example using SQL Server

> Reminder: Donald will work with __any__ ADO implementation (SQL Server, SQLite, MySQL, Postgresql etc.).

Consider the following model:

```f#
type Author = 
    {
        AuthorId : int
        FullName : string
    }
    
module Author
    let fromDataReader (rd : IDataReader) = 
          {
              // IDataReader extension method (see below)
              AuthorId = rd.GetInt32("author_id")  
              FullName = rd.GetString("full_name")
          }
```

### Define a `DbConnectionFactory`
```f#
open System.Data.SqlClient
open Donald

let connectionString = 
    "Server=MY_SERVER;Database=MyDatabase;Trusted_Connection=True;"

let connectionFactory : DbConnectionFactory = 
    fun _ -> new SqlConnection(connectionString) :> IDbConnection
```

### Query for multiple strongly-typed results

```f#
let findAuthors connectionFactory search =
    use conn = createConn connectionFactory

    query
         "SELECT author_id, full_name
          FROM   author
          WHERE  full_name LIKE @search"
          [ newParam "search" (SqlType.String search) ]
          Author.fromDataReader
          conn
```

Or async:

```f#
let findAuthors connectionFactory search =
    task {
        use conn = createConn connectionFactory

        return! 
            queryAsync
                "SELECT author_id, full_name
                 FROM   author
                 WHERE  full_name LIKE @search"
                [ newParam "search" (SqlType.String search) ]
                Author.fromDataReader
                conn
    }
```

### Query for exactly one strongly-type result

```f#
let getAuthor connectionFactory authorId =
    use conn = createConn connectionFactory

    querySingle // Returns Option<Author>
        "SELECT author_id, full_name
         FROM   author
         WHERE  author_id = @author_id"
         [ newParam "author_id" (SqlType.Int authorId) ]
         Author.fromDataReader 
         conn
```

Or async:

```f#
let getAuthor connectionFactory authorId =
    task {
        use conn = createConn connectionFactory

        return! 
            querySingleAsync 
                "SELECT author_id, full_name
                 FROM   author
                 WHERE  author_id = @author_id"
                [ newParam "author_id" (SqlType.Int authorId) ]
                Author.fromDataReader 
                conn
    }
```

### Doing work transactionally

The five main API functions: `query`, `querySingle`, `scalar`, `exec` and `execMany`, all have transactional sister functions: `tranQuery`, `tranQuerySingle`, `tranScalar`, `tranExec` and `tranExecMany`. 

As opposed to an `IDbConnection`, these functions expect an `IDbTransaction` as the final paramter.

### Execute a statement

```f#
let updateAuthor connectionFactory author =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    tranExec // ExecuteNonQuery() within scope of transaction
        "UPDATE author 
         SET    full_name = @full_name 
         WHERE  author_id = @author_id"
         [ 
             newParam "author_id" (SqlType.Int author.AuthorId)
             newParam "full_name" (SqlType.String author.FullName)
         ]
         tran

    commitTran tran // safely commit transaction
```

### Execute a statement many times

```f#
let insertDefaultAuthors connectionFactory =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    tranExecMany
        "INSERT INTO author (full_name) VALUES (@full_name);"                
        [
            [ newParam "full_name" (SqlType.String "Bugs Bunny") ]
            [ newParam "full_name" (SqlType.String "Donald Duck") ]
        ]                        
        tran    
  
    commitTran tran
```


### Execute a statement that returns a value

```f#
let insertAuthor connectionFactory fullName =
    use conn = createConn connectionFactory
    use tran = beginTran conn // Base function's are transaction-oriented
    
    let authorId = 
        tranScalar // ExecuteScalar() within scope of transaction
            "INSERT INTO author (full_name) VALUES (@full_name);
             SELECT SCOPE_IDENTITY();"
             [ newParam "full_name" (SqlType.String fullName) ]
             Convert.ToInt32 // Any obj -> int function would do here
       tran

    commitTran tran

    authorId 
```

## Handling Database Errors/Exceptions

There are times when the database engine will error. For example, when receiving an invalid SQL statement, missing input variable etc. In these cases, you'll likely be rewarded with an `Exception`. Yay!

To make this actuality more explicit, forcing the consumer to handle the possibility of failure, all 10 statement functions have a "try" implementation. These encapsulate not only the result, but also the success or failure of the operation using the following type:

```f#
type DbResult<'a> =
    | DbResult of 'a    
    | DbError  of Exception
```

To illustrate dealing with this new type, consider these examples:

```f#
let tryFindAuthors connectionFactory search =
  use conn = createConn connectionFactory

  // "try" functions are available for all statement types
  tryQuery
       "SELECT author_id, full_name
        FROM   author
        WHERE  full_name LIKE @search"
        [ newParam "search" (SqlType.String search) ]
        Author.fromDataReader
        conn

// Consuming the database call elsewhere in the code
let result = "Doe" |> (connectionFactory |> tryFindAuthors)
match result with
| DbError ex -> 
    if isNull ex.InnerException then ex.Message 
    else ex.InnerException.Message
    |> Error
| DbResult authors -> // Do something meaningful wuth authors ...
```

## `IDataReader` Extension Methods

To make obtaining values from reader more straight-forward, 3 sets of extension methods are available for:
1. Get value, automatically defaulted
2. Get value as `option<'a>`
3. Get value as `Nullable<'a>`

Assume we have an active `IDataReader` called `rd` and are currently reading a row, the following extension methods are available to simplify reading values:

```f#
rd.GetString "some_field"           // string -> string
rd.GetBoolean "some_field"          // string -> bool
rd.GetByte "some_field"             // string -> byte
rd.GetChar "some_field"             // string -> char
rd.GetDateTime "some_field"         // string -> DateTime
rd.GetDateTimeOffset "some_field"   // string -> DateTime
rd.GetDecimal "some_field"          // string -> Decimal
rd.GetDouble "some_field"           // string -> Double
rd.GetFloat "some_field"            // string -> float32
rd.GetGuid "some_field"             // string -> Guid
rd.GetInt16 "some_field"            // string -> int16
rd.GetInt32 "some_field"            // string -> int32
rd.GetInt64 "some_field"            // string -> int64
rd.GetBytes "some_field"            // string -> byte[]

rd.GetStringOption "some_field"         // string -> string option
rd.GetBooleanOption "some_field"        // string -> bool option
rd.GetByteOption "some_field"           // string -> byte option
rd.GetCharOption "some_field"           // string -> char option
rd.GetDateTimeOption "some_field"       // string -> DateTime option
rd.GetDateTimeOffsetOption "some_field" // string -> DateTime option
rd.GetDecimalOption "some_field"        // string -> Decimal option
rd.GetDoubleOption "some_field"         // string -> Double option
rd.GetFloatOption "some_field"          // string -> float32 option
rd.GetGuidOption "some_field"           // string -> Guid option
rd.GetInt16Option "some_field"          // string -> int16 option
rd.GetInt32Option "some_field"          // string -> int32 option
rd.GetInt64Option "some_field"          // string -> int64 option
rd.GetBytesOption "some_field"          // string -> byte[] option

rd.GetNullableBoolean "some_field"        // string -> Nullable<bool>
rd.GetNullableByte "some_field"           // string -> Nullable<byte>
rd.GetNullableChar "some_field"           // string -> Nullable<char>
rd.GetNullableDateTime "some_field"       // string -> Nullable<DateTime>
rd.GetNullableDateTimeOffset "some_field" // string -> Nullable<DateTime>
rd.GetNullableDecimal "some_field"        // string -> Nullable<Decimal>
rd.GetNullableDouble "some_field"         // string -> Nullable<Double>
rd.GetNullableFloat "some_field"          // string -> Nullable<float32>
rd.GetNullableGuid "some_field"           // string -> Nullable<Guid>
rd.GetNullableInt16 "some_field"          // string -> Nullable<int16>
rd.GetNullableInt32 "some_field"          // string -> Nullable<int32>
rd.GetNullableInt64 "some_field"          // string -> Nullable<int64>
```

## Find a bug?

There's an [issue](https://github.com/pimbrouwers/Donald/issues) for that.

## License

Built with ♥ by [Pim Brouwers](https://github.com/pimbrouwers) in Toronto, ON. Licensed under [Apache License 2.0](https://github.com/pimbrouwers/Donald/blob/master/LICENSE).
