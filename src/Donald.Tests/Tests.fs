module Donald.Tests

open System
open System.Data
open System.Data.Common
open System.Data.SQLite
open System.IO
open Xunit
open Donald
open FsUnit.Xunit

let connectionString = "Data Source=:memory:;Version=3;New=true;"
let connectionFactory : DbConnectionFactory = fun _ -> new SQLiteConnection(connectionString) :> IDbConnection
let conn = createConn connectionFactory

type Author = 
    {
        AuthorId : int
        FullName : string
    }
    static member fromReader (rd : IDataReader) =
        {
            AuthorId = rd.GetInt32("author_id")
            FullName = rd.GetString("full_name")
        }

type DbFixture () =
    do 
        use fs = IO.File.OpenRead("schema.sql")
        use sr = new StreamReader(fs)
        let sql = sr.ReadToEnd()

        exec sql [] conn
                  
    interface IDisposable with
        member __.Dispose() = 
            conn.Dispose()
            ()

[<CollectionDefinition("Db")>]
type DbCollection () =
    interface ICollectionFixture<DbFixture>

module UnitTests =
    module Param =
            [<Fact>]
            let ``Should create valid DbParam`` () =
                let v = (SqlType.Int 1)
                let p = newParam "test" v
                p.Name  |> should equal "test"
                p.Value |> should equal v
                
module IntegrationTests =
    
    [<Collection("Db")>]
    type Core() =
        [<Fact>]
        member __.``Can create connection`` () =
            use conn = createConn connectionFactory
            
            conn.State            |> should equal ConnectionState.Closed
            conn.ConnectionString |> should equal connectionString

        [<Fact>]
        member __.``Can begin transaction`` () =
            use tran = beginTran conn

            conn.State      |> should equal ConnectionState.Open
            tran.Connection |> should equal conn            
            
        [<Fact>]
        member __.``Should not throw on duplicate commit transaction`` () =            
            use tran = beginTran conn

            commitTran tran 
            commitTran tran |> should not' (throw typeof<System.Exception>)
            
        [<Fact>]
        member __.``Should not throw on duplicate rollback transaction`` () =            
            use tran = beginTran conn

            rollbackTran tran
            rollbackTran tran |> should not' (throw typeof<System.Exception>)
            
    [<Collection("Db")>]
    type Command() =         
         
        [<Fact>]
        member __.``Should create command with no params`` () =            
            use tran = beginTran conn
            let c = newCommand "SELECT 1" [] tran
            
            c.Connection.ConnectionString |> should equal connectionString
            c.CommandText                 |> should equal "SELECT 1"
            c.Parameters.Count            |> should equal 0
            
        [<Fact>]
        member __.``Should create command with params`` () =            
            use tran = beginTran conn
            let c = newCommand "SELECT @n" [ newParam "n" (SqlType.Int 2) ] tran

            c.Connection.ConnectionString |> should equal connectionString
            c.CommandText                 |> should equal "SELECT @n"
            c.Parameters.Count            |> should equal 1

            (c.Parameters.[0] :?> DbParameter).Value 
            |> System.Convert.ToInt32
            |> should equal 2

    [<Collection("Db")>]
    type Statements() =         
        [<Fact>]
        member __.``SELECT records`` () =            
            let authors =
                query 
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
                     []
                     Author.fromReader
                     conn
            
            authors.Length |> should equal 2

        [<Fact>]
        member __.``try SELECT records`` () =            
            let authorsResult =
                tryQuery 
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
                     []
                     Author.fromReader
                     conn
            
            authorsResult |> should be instanceOfType<DbResult<Author list>>
            
            match authorsResult with
            | DbResult authors -> authors.Length |> should equal 2
            | _ -> "DbResult should not be DbError" |> should equal false

        [<Fact>]
        member __.``SELECT records should fail and create DbError`` () =
            let authorsResult =
                tryQuery 
                    "SELECT author_id, full_name
                     FROM   fake_author"
                     []
                     Author.fromReader
                     conn
            
            authorsResult |> should be instanceOfType<DbResult<Author list>>
            
            match authorsResult with
            | DbError ex -> ex |> should be instanceOfType<Exception>
            | _ -> "DbResult should not be Ok" |> should equal false

        [<Fact>]
        member __.``SELECT single record`` () =            
            let author =
                querySingle
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id = 1"
                     []
                     Author.fromReader
                     conn
            
            author.IsSome         |> should equal true
            author.Value.AuthorId |> should equal 1

        [<Fact>]
        member __.``try SELECT single record`` () =            
            let authorResult =
                tryQuerySingle
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id = 1"
                     []
                     Author.fromReader
                     conn
                        
            authorResult |> should be instanceOfType<DbResult<Author option>>
            
            match authorResult with
            | DbResult author ->                
                match author with 
                | Some author -> 
                    author.AuthorId |> should equal 1
                | None ->  "Author should not be None" |> should equal false
            | _ -> "DbResult should not be DbError" |> should equal false

        [<Fact>]
        member __.``SELECT single record should fail and create DbError`` () =            
            let authorResult =
                tryQuerySingle
                    "SELECT author_id, full_name
                     FROM   fake_author
                     WHERE  AND author_id = 1"
                     []
                     Author.fromReader
                     conn

            authorResult |> should be instanceOfType<DbResult<Author option>>
            
            match authorResult with
            | DbError ex -> ex |> should be instanceOfType<Exception>
            | _ -> "DbResult should not be Ok" |> should equal false
            
        [<Fact>]
        member __.``INSERT author then retrieve to verify`` () =
            let fullName = "Jane Doe"
            let authorId = 
                 scalar
                    "INSERT INTO author (full_name) VALUES (@full_name);
                     SELECT LAST_INSERT_ROWID();"
                    [ newParam "full_name" (SqlType.String fullName)]
                    Convert.ToInt32
                    conn 

            let author = 
                querySingle
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id = @author_id"
                     [ newParam "author_id" (SqlType.Int authorId) ]
                     Author.fromReader       
                     conn

            author.IsSome |> should equal true

            match author with
            | Some author ->
                author.FullName |> should equal fullName
            | None -> 
                ()

        [<Fact>]
        member __.``INSERT author should fail and create DbError`` () =
            let fullName = "Jane Doe"
            let authorInsertResult = 
                 tryExec
                    "INSERT INTO fake_author (full_name) VALUES (@full_nameaaaa);"
                    [ newParam "full_name" (SqlType.String fullName)]                    
                    conn 

            authorInsertResult |> should be instanceOfType<DbResult<unit>>
            
            match authorInsertResult with
            | DbError ex -> ex |> should be instanceOfType<Exception>
            | _ -> "DbResult should not be Ok" |> should equal false

        [<Fact>]
        member __.``INSERT MANY authors then count to verify`` () =
            let initialCount = scalar "SELECT COUNT(author_id) FROM author" [] Convert.ToInt32 conn
           
            let authorParams = 
                [
                    [ newParam "full_name" (SqlType.String "Bugs Bunny") ]
                    [ newParam "full_name" (SqlType.String "Donald Duck") ]
                ]                
            execMany
                "INSERT INTO author (full_name) VALUES (@full_name);"                
                authorParams
                conn 
            
            let afterCount = scalar "SELECT COUNT(author_id) FROM author" [] Convert.ToInt32 conn
            
            initialCount + authorParams.Length |> should equal afterCount

        [<Fact>]
        member __.``INSERT MANY should fail and create DbError`` () =
            let authorParams = 
                [
                    [ newParam "full_name" (SqlType.String "Bugs Bunny") ]
                    [ newParam "full_name" (SqlType.String "Donald Duck") ]
                ]   
                
            let authorsResult =
                tryExecMany
                    "INSERT INTO fake_author (full_name) VALUES (@full_nadsame);"                
                    authorParams
                    conn 

            authorsResult |> should be instanceOfType<DbResult<unit>>

            match authorsResult with
            | DbError ex -> ex |> should be instanceOfType<Exception>
            | _ -> "DbResult should not be Ok" |> should equal false

        [<Fact>]
        member __.``UPDATE author then retrieve to verify`` () =
            let authorId = 1
            let fullName = "Jim Brouwers"
            exec
                "UPDATE author SET full_name = @full_name WHERE author_id = @author_id"
                [ 
                    newParam "author_id" (SqlType.Int authorId)
                    newParam "full_name" (SqlType.String fullName)
                ]
                conn
                
            let author = 
                querySingle
                    "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id = @author_id"
                     [ newParam "author_id" (SqlType.Int authorId) ]
                     Author.fromReader            
                     conn 

            author.IsSome |> should equal true

            match author with
            | Some author ->
                author.FullName |> should equal fullName
            | None -> 
                ()
        
        [<Fact>]
        member __.``UPDATE should fail and create DbError`` () =
            let authorId = 1
            let fullName = "Jim Brouwers"
            let authorResult =
                tryExec
                    "UPDATE fake_author SET full_name = @full_name WHERE aauthor_id = @author_idda"
                    [ 
                        newParam "author_id" (SqlType.Int authorId)
                        newParam "full_name" (SqlType.String fullName)
                    ]
                    conn

            authorResult |> should be instanceOfType<DbResult<unit>>
                
            match authorResult with
            | DbError ex -> ex |> should be instanceOfType<Exception>
            | _ -> "DbResult should not be Ok" |> should equal false

        [<Fact>]
        member __.``INSERT+SELECT binary should work`` () =
            let testString = "A sample of bytes"
            let bytes = Text.Encoding.UTF8.GetBytes(testString)
            let fileId = 
                scalar
                    "INSERT INTO file (data) VALUES (@data); SELECT LAST_INSERT_ROWID();" 
                    [ newParam "data" (SqlType.Bytes bytes) ]
                    Convert.ToInt32
                    conn

            let retrievedBytes =
                querySingle
                    "SELECT data FROM file WHERE file_id = @file_id"
                    [ newParam "file_id" (SqlType.Int fileId) ]
                    (fun rd -> rd.GetBytes("data"))
                    conn

            match retrievedBytes with
            | Some b -> 
                let str = Text.Encoding.UTF8.GetString(b)
                b |> should equal bytes
                str |> should equal testString
            | None   -> true |> should equal "Invalid bytes returned"            