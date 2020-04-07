# Donald

![NuGet Version](https://img.shields.io/nuget/v/Donald.svg)
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
    static member fromReader (rd : IDataReader) = 
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

The four main API functions: `query`, `querySingle`, `scalar` and `exec`, all have transactional sister functions: `tranQuery`, `tranQuerySingle`, `tranScalar`, `tranExec`. 

As opposed to an `IDbConnection`, these function expect an `IDbTransaction` as the final paramter.

### Execute a statement

```f#
let updateAuthor author =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    tranExec // ExecuteNonQuery() within scope of transaction
        "UPDATE author SET full_name = @full_name WHERE author_id = @author_id"
        [ 
            newParam "author_id" author.AuthorId
            newParam "full_name" author.FullName
        ]
        tran

    commitTran tran // safely commit transaction
```

### Execute a statement that returns a value

```f#
let insertAuthor fullName =
    use conn = createConn connectionFactory
    use tran = beginTran conn // Base function's are transaction-oriented
    
    let authorId = 
        tranScalar // ExecuteScalar() within scope of transaction
            "INSERT INTO author (full_name) VALUES (@full_name);
             SELECT SCOPE_IDENTITY();"
            [ newParam "full_name" fullName]
            Convert.ToInt32 // Any obj -> int function would do here
	    tran

    commitTran tran

    authorId 
```

## Find a bug?

There's an [issue](https://github.com/pimbrouwers/Donald/issues) for that.

## License

Built with ♥ by [Pim Brouwers](https://github.com/pimbrouwers) in Toronto, ON. Licensed under [Apache License 2.0](https://github.com/pimbrouwers/Donald/blob/master/LICENSE).
