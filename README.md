# Donald

[![Build Status](https://travis-ci.org/pimbrouwers/Donald.svg?branch=master)](https://travis-ci.org/pimbrouwers/Donald)

Meet [Donald](https://en.wikipedia.org/wiki/Donald_D._Chamberlin). 

If you're a programmer and have used a database, he's impacted your life in a big way. 

This library is named in his honour.

## Getting Started

> If you came looking for an ORM, this is not your light saber and may the force be with you.

Donald is a library that aims to make working with [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) a little bit simpler. 

Providing basic functional wrappers for the `IDbCommand` methods `ExecuteNonQuery()`, `ExecuteScalar()` & `ExecuteReader()`.

A script is worth a thousand words:

```fsharp
// ------------
// An example using SQL Server
// ------------
open System.Data.SqlClient

let connectionString = 
    "Server=myServerAddress;Database=myDataBase;Trusted_Connection=True;"


// Created our strongly typed DbConnectionFactory
let connectionFactory : DbConnectionFactory = 
    fun _ -> new SqlConnection(connectionString) :> IDbConnection


// Define our model
type Author = 
    {
        AuthorId : int
        FullName : string
    }
    static member fromReader (rd : IDataReader) = // Not mandatory, but helpful
        {
            AuthorId = rd.GetInt32("author_id")  // IDataReader extension method
            FullName = rd.GetString("full_name") // IDataReader extension method
        }


// Find author's by name
let findAuthor search =
    use conn = createConn connectionFactory

    conn.Query // IDbConnection extension method
        "SELECT author_id, full_name
         FROM   author
         WHERE  full_name LIKE @search"
         [ newParam "search" search ]
         Author.fromReader


// Create a new author
let insertAuthor fullName =
    use conn = createConn connectionFactory
    use tran = beginTran conn // Base IDbCommand function's are transaction-oriented
    
    let authorId = 
        scalar // ExecuteScalar() within scope of transaction
            "INSERT INTO author (full_name) VALUES (@full_name);
             SELECT LAST_INSERT_ROWID();"
            [ newParam "full_name" fullName]
            Convert.ToInt32 // Any obj -> int function would do here

    commitTran tran

    authorId 


// Update an existing author
let updateAuthor author =
    use conn = createConn connectionFactory
    use tran = beginTran conn 

    exec // ExecuteNonQuery() within scope of transaction
        "UPDATE author SET full_name = @full_name WHERE author_id = @author_id"
        [ 
            newParam "author_id" author.AuthorId
            newParam "full_name" author.FullName
        ]
        tran

    commitTran tran // safely commit transaction


// Retrieve author by id
let getAuthor authorId =
    use conn = createConn connectionFactory

    conn.QuerySingle // Returns Option<Author>
        "SELECT author_id, full_name
         FROM   author
         WHERE  author_id = @author_id"
         [ newParam "author_id" authorId ]
         Author.fromReader  
```

## Find a bug?

There's an [issue](https://github.com/pimbrouwers/Donald/issues) for that.

## License

Built with ♥ by [Pim Brouwers](https://github.com/pimbrouwers) in Toronto, ON. Licensed under [Apache License 2.0](https://github.com/pimbrouwers/Donald/blob/master/LICENSE).
