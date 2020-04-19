# Donald

[![NuGet Version](https://img.shields.io/nuget/v/Donald.svg)](https://www.nuget.org/packages/Donald)
[![Build Status](https://travis-ci.org/pimbrouwers/Donald.svg?branch=master)](https://travis-ci.org/pimbrouwers/Donald)

Meet [Donald](https://en.wikipedia.org/wiki/Donald_D._Chamberlin). 

If you're a programmer and have used a database, he's impacted your life in a big way. 

This library is named after him.

## Getting Started

Donald is a well-tested library that aims to make working with [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) a little bit more succinct. 

Providing basic functional wrappers for the `IDbCommand` methods `ExecuteNonQuery()`, `ExecuteScalar()` & `ExecuteReader()` and a full-suite of `IDataReader` extension methods to make retrieving values safer and more direct.

> If you came looking for an ORM, this is not your light saber. And may the force be with you.

Install the [Donald](https://www.nuget.org/packages/Donald/) NuGet package:

```
PM>  Install-Package Donald
```

Or using the dotnet CLI
```cmd
dotnet add package Donald
```

## An example using SQL Server

Consider the following model:

```f#
type Author = 
    {
        AuthorId : int
        FullName : string
    }
    // Not mandatory, but helpful
    static member FromReader (rd : IDataReader) = 
        {
            AuthorId = rd.GetInt32("author_id")  // IDataReader extension method
            FullName = rd.GetString("full_name") // IDataReader extension method
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
let findAuthor search =
    use conn = createConn connectionFactory

    query
         "SELECT author_id, full_name
          FROM   author
          WHERE  full_name LIKE @search"
          [ newParam "search" search ]
          Author.fromReader
	  conn
```

### Query for exactly one strongly-type result

```f#
let getAuthor authorId =
    use conn = createConn connectionFactory

    querySingle // Returns Option<Author>
        "SELECT author_id, full_name
         FROM   author
         WHERE  author_id = @author_id"
         [ newParam "author_id" authorId ]
         Author.fromReader 
         conn
```

### Doing work transactionally

The five main API functions: `query`, `querySingle`, `scalar`, `exec` and `execMany`, all have transactional sister functions: `tranQuery`, `tranQuerySingle`, `tranScalar`, `tranExec` and `tranExecMany`. 

As opposed to an `IDbConnection`, these functions expect an `IDbTransaction` as the final paramter.

### Execute a statement

```f#
let updateAuthor author =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    tranExec // ExecuteNonQuery() within scope of transaction
        "UPDATE author 
         SET    full_name = @full_name 
         WHERE  author_id = @author_id"
        [ 
            newParam "author_id" author.AuthorId
            newParam "full_name" author.FullName
        ]
        tran

    commitTran tran // safely commit transaction
```

### Execute a statement many times

```f#
let insertAuthors =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    tranExecMany
        "INSERT INTO author (full_name) VALUES (@full_name);"                
        [
            [ newParam "full_name" "Bugs Bunny" ]
            [ newParam "full_name" "Donald Duck" ]
        ]                        
        tran    
  
    commitTran tran
```


### Execute a statement that returns a value

```f#
let insertAuthor fullName =
    use conn = createConn connectionFactory
    use tran = beginTran conn // Base function's are transaction-oriented
    
    let authorId = 
        tranScalar // ExecuteScalar() within scope of transaction
            "INSERT INTO author (full_name) 
             VALUES (@full_name);

             SELECT SCOPE_IDENTITY();"
            [ newParam "full_name" fullName]
            Convert.ToInt32 // Any obj -> int function would do here
	    tran

    commitTran tran

    authorId 
```

## `IDataReader` Extension Methods

To make obtaining values from reader more straight-forward, 3 sets of extension methods are available for:
1. Get value, automatically defaulted
2. Get value as `option<'a>`
3. Get value as `Nullable<'a>`

Assume we have an open `IDataReader` and are currently reading a row, the `IDataRecord`:

```f#
rd.GetString "some_field"           // string -> string
rd.GetBoolean "some_field"          // string -> bool
rd.GetByte "some_field"             // string -> byte
rd.GetChar "some_field"             // string -> char
rd.GetDateTime "some_field"         // string -> DateTime
rd.GetDecimal "some_field"          // string -> Decimal
rd.GetDouble "some_field"           // string -> Double
rd.GetFloat "some_field"            // string -> float32
rd.GetGuid "some_field"             // string -> Guid
rd.GetInt16 "some_field"            // string -> int16
rd.GetInt32 "some_field"            // string -> int32
rd.GetInt64 "some_field"            // string -> int64

rd.GetStringOption "some_field"     // string -> string option
rd.GetBooleanOption "some_field"    // string -> bool option
rd.GetByteOption "some_field"       // string -> byte option
rd.GetCharOption "some_field"       // string -> char option
rd.GetDateTimeOption "some_field"   // string -> DateTime option
rd.GetDecimalOption "some_field"    // string -> Decimal option
rd.GetDoubleOption "some_field"     // string -> Double option
rd.GetFloatOption "some_field"      // string -> float32 option
rd.GetGuidOption "some_field"       // string -> Guid option
rd.GetInt16Option "some_field"      // string -> int16 option
rd.GetInt32Option "some_field"      // string -> int32 option
rd.GetInt64Option "some_field"      // string -> int64 option

rd.GetNullableBoolean "some_field"  // string -> Nullable<bool>
rd.GetNullableByte "some_field"     // string -> Nullable<byte>
rd.GetNullableChar "some_field"     // string -> Nullable<char>
rd.GetNullableDateTime "some_field" // string -> Nullable<DateTime>
rd.GetNullableDecimal "some_field"  // string -> Nullable<Decimal>
rd.GetNullableDouble "some_field"   // string -> Nullable<Double>
rd.GetNullableFloat "some_field"    // string -> Nullable<float32>
rd.GetNullableGuid "some_field"     // string -> Nullable<Guid>
rd.GetNullableInt16 "some_field"    // string -> Nullable<int16>
rd.GetNullableInt32 "some_field"    // string -> Nullable<int32>
rd.GetNullableInt64 "some_field"    // string -> Nullable<int64>
```

## Find a bug?

There's an [issue](https://github.com/pimbrouwers/Donald/issues) for that.

## License

Built with ♥ by [Pim Brouwers](https://github.com/pimbrouwers) in Toronto, ON. Licensed under [Apache License 2.0](https://github.com/pimbrouwers/Donald/blob/master/LICENSE).
