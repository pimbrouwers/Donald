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
let conn = new SQLiteConnection(connectionString)

let shouldNotBeError pred (result : DbResult<'a>) =    
    match result with
    | Ok result' -> pred result'
    | _ -> "DbResult should not be Error" |> should equal false

let shouldNotBeOk (result : DbResult<'a>) =    
    match result with
    | Error ex -> ex |> should be instanceOfType<DbExecutionError>
    | _ -> "DbResult should not be Ok" |> should equal false

type Author = 
    {
        AuthorId : int
        FullName : string
    }
    static member FromReader (rd : IDataReader) =
        {
            AuthorId = rd.GetInt32OrDefault "author_id" -1
            FullName = rd.GetStringOrDefault "full_name" ""
        }

type DbFixture () =
    do 
        use fs = IO.File.OpenRead("schema.sql")
        use sr = new StreamReader(fs)
        let sql = sr.ReadToEnd()

        dbCommand conn { 
            cmdText sql
        }        
        |> DbConn.exec 
        |> ignore
                  
    interface IDisposable with
        member __.Dispose() = 
            conn.Dispose()
            ()

[<CollectionDefinition("Db")>]
type DbCollection () =
    interface ICollectionFixture<DbFixture>

[<Collection("Db")>]
type Statements() = 
    [<Fact>]
    member __.``SELECT all sql types`` () =            
        let sql = 
            "SELECT  @p_null AS p_null
            , @p_string AS p_string
            , @p_ansi_string AS p_ansi_string
            , @p_boolean AS p_boolean
            , @p_byte AS p_byte
            , @p_char AS p_char
            , @p_ansi_char AS p_ansi_char
            , @p_decimal AS p_decimal
            , @p_double AS p_double
            , @p_float AS p_float
            , @p_guid AS p_guid
            , @p_int16 AS p_int16
            , @p_int32 AS p_int32
            , @p_int64 AS p_int64
            , @p_date_time AS p_date_time
            , @p_date_time_offset AS p_date_time_offset"

        let param = 
            DbParams.create [
                "p_null", SqlType.Null
                "p_string", SqlType.String "p_string"
                "p_ansi_string", SqlType.AnsiString "p_ansi_string"
                "p_boolean", SqlType.Boolean false
                "p_byte", SqlType.Byte Byte.MinValue
                "p_char", SqlType.Char 'a'
                "p_ansi_char", SqlType.AnsiChar Char.MinValue
                "p_decimal", SqlType.Decimal 0.0M
                "p_double", SqlType.Double 0.0
                "p_float", SqlType.Float 0.0
                "p_guid", SqlType.Guid (Guid.NewGuid())
                "p_int16", SqlType.Int16 0s
                "p_int32", SqlType.Int32 0
                "p_int64", SqlType.Int64 0L
                "p_date_time", SqlType.DateTime DateTime.Now
                "p_date_time_offset", SqlType.DateTimeOffset DateTimeOffset.Now
            ]

        let map (rd : IDataReader) = 
            {|
                p_null = rd.GetStringOption "p_null"
                p_string = rd.GetStringOption "p_string"
                p_ansi_string = rd.GetStringOption "p_ansi_string"
                p_boolean = rd.GetBooleanOption "p_boolean"
                p_byte = rd.GetByteOption "p_byte"
                p_char = rd.GetCharOption "p_char"
                p_ansi_char = rd.GetCharOption "p_ansi_char"
                p_decimal = rd.GetDecimalOption "p_decimal"
                p_double = rd.GetDoubleOption "p_double"
                p_float = rd.GetFloatOption "p_float"
                p_guid = rd.GetGuidOption "p_guid"
                p_int16 = rd.GetInt16Option "p_int16"
                p_int32 = rd.GetInt32Option "p_int32"
                p_int64 = rd.GetInt64Option "p_int64"
                p_date_time = rd.GetDateTimeOption "p_date_time" 
            |}

        dbCommand conn {
            cmdText sql
            cmdParam param            
        }
        |> DbConn.querySingle map
        |> shouldNotBeError (fun result -> result.IsSome |> should equal true)
                           
    [<Fact>]
    member __.``SELECT records`` () =                    
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
        } 
        |> DbConn.query Author.FromReader
        |> shouldNotBeError (fun result -> result.Length |> should equal 2)

    [<Fact>]
    member __.``SELECT records async`` () =            
 
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
        }
        |> DbConn.Async.query Author.FromReader
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> result.Length |> should equal 2)

    [<Fact>]
    member __.``SELECT records should fail and create DbError`` () =        
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   fake_author"
        }
        |> DbConn.query Author.FromReader
        |> shouldNotBeOk

    //[<Fact>]
    //member __.``SELECT single record`` () =            
    //    let author =
    //        DbConn.querySingle
    //            Author.FromReader
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = 1"
    //            []
    //            conn
            
    //    author.IsSome         |> should equal true
    //    author.Value.AuthorId |> should equal 1

    //[<Fact>]
    //member __.``SELECT NULL`` () =            
    //    let nullableAuthor =
    //        DbConn.querySingle 
    //            (fun rd -> 
    //                {| 
    //                    FullName = rd.GetStringOrDefault "full_name" null
    //                    Age = rd.GetNullableInt32 "age"
    //                |})
    //            "SELECT NULL AS full_name, NULL AS age"
    //            []
    //            conn
            
    //    nullableAuthor.IsSome         |> should equal true
    //    nullableAuthor.Value.FullName |> should equal null
    //    nullableAuthor.Value.Age      |> should equal null
      
    //[<Fact>]
    //member __.``SELECT single record async`` () =            
    //    let author =
    //        DbConn.Async.querySingle
    //            Author.FromReader
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = 1"
    //            []
    //            conn
    //        |> Async.AwaitTask
    //        |> Async.RunSynchronously
            
    //    author.IsSome         |> should equal true
    //    author.Value.AuthorId |> should equal 1
            
    //[<Fact>]
    //member __.``INSERT author then retrieve to verify`` () =
    //    let fullName = "Jane Doe"
    //    let authorId = 
    //        DbConn.scalar                
    //            "INSERT INTO author (full_name) VALUES (@full_name);
    //                SELECT LAST_INSERT_ROWID();"
    //            [ DbParam.create "full_name" (SqlType.String fullName)]
    //            conn 
    //        |> Convert.ToInt32

    //    let author = 
    //        DbConn.querySingle
    //            Author.FromReader   
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = @author_id"
    //            [ DbParam.create "author_id" (SqlType.Int authorId) ]
    //            conn

    //    author.IsSome |> should equal true

    //    match author with
    //    | Some author ->
    //        author.FullName |> should equal fullName
    //    | None -> 
    //        ()

    //[<Fact>]
    //member __.``INSERT author with NULL birth_date`` () =
    //    let fullName = "Jim Doe"
    //    let birthDate : DateTime option = None

    //    let result = 
    //            tryExec
    //            "INSERT INTO author (full_name, birth_date) VALUES (@full_name, @birth_date);"
    //            [ 
    //                DbParam.create "full_name" (SqlType.String fullName) 
    //                DbParam.create "birth_date" (match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null) 
    //            ]
    //            conn 

    //    match result with 
    //    | Ok _ -> ()
    //    | Error ex -> ex.Message |> should equal false

    //[<Fact>]
    //member __.``INSERT author should fail and create DbError`` () =
    //    let fullName = "Jane Doe"
    //    let authorInsertResult = 
    //            tryExec
    //            "INSERT INTO fake_author (full_name) VALUES (@full_nameaaaa);"
    //            [ DbParam.create "full_name" (SqlType.String fullName)]                    
    //            conn 

    //    authorInsertResult |> should be instanceOfType<DbResult<unit>>
            
    //    match authorInsertResult with
    //    | Error ex -> ex |> should be instanceOfType<Exception>
    //    | _ -> "Ok should not be Ok" |> should equal false

    //[<Fact>]
    //member __.``INSERT async author then retrieve to verify`` () =
    //    let fullName = "Janet Doe"
    //    let authorId = 
    //            scalarAsync
    //            "INSERT INTO author (full_name) VALUES (@full_name);
    //                SELECT LAST_INSERT_ROWID();"
    //            [ DbParam.create "full_name" (SqlType.String fullName)]
    //            Convert.ToInt32
    //            conn 
    //        |> Async.AwaitTask
    //        |> Async.RunSynchronously

    //    let author = 
    //        querySingleAsync
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = @author_id"
    //                [ DbParam.create "author_id" (SqlType.Int authorId) ]
    //                Author.FromReader       
    //                conn
    //        |> Async.AwaitTask
    //        |> Async.RunSynchronously

    //    author.IsSome |> should equal true

    //    match author with
    //    | Some author ->
    //        author.FullName |> should equal fullName
    //    | None -> 
    //        ()

    //[<Fact>]
    //member __.``INSERT MANY authors then count to verify`` () =
    //    let initialCount = scalar "SELECT COUNT(author_id) FROM author" [] Convert.ToInt32 conn
           
    //    let authorParams = 
    //        [
    //            [ DbParam.create "full_name" (SqlType.String "Bugs Bunny") ]
    //            [ DbParam.create "full_name" (SqlType.String "Donald Duck") ]
    //        ]                
    //    execMany
    //        "INSERT INTO author (full_name) VALUES (@full_name);"                
    //        authorParams
    //        conn 
            
    //    let afterCount = scalar "SELECT COUNT(author_id) FROM author" [] Convert.ToInt32 conn
            
    //    initialCount + authorParams.Length |> should equal afterCount

    //[<Fact>]
    //member __.``INSERT MANY should fail and create DbError`` () =
    //    let authorParams = 
    //        [
    //            [ DbParam.create "full_name" (SqlType.String "Bugs Bunny") ]
    //            [ DbParam.create "full_name" (SqlType.String "Donald Duck") ]
    //        ]   
                
    //    let authorsResult =
    //        tryExecMany
    //            "INSERT INTO fake_author (full_name) VALUES (@full_nadsame);"                
    //            authorParams
    //            conn 

    //    authorsResult |> should be instanceOfType<DbResult<unit>>

    //    match authorsResult with
    //    | Error ex -> ex |> should be instanceOfType<Exception>
    //    | _ -> "Ok should not be Ok" |> should equal false

    //[<Fact>]
    //member __.``UPDATE author then retrieve to verify`` () =
    //    let authorId = 1
    //    let fullName = "Jim Brouwers"
    //    exec
    //        "UPDATE author SET full_name = @full_name WHERE author_id = @author_id"
    //        [ 
    //            DbParam.create "author_id" (SqlType.Int authorId)
    //            DbParam.create "full_name" (SqlType.String fullName)
    //        ]
    //        conn
                
    //    let author = 
    //        querySingle
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = @author_id"
    //                [ DbParam.create "author_id" (SqlType.Int authorId) ]
    //                Author.FromReader            
    //                conn 

    //    author.IsSome |> should equal true

    //    match author with
    //    | Some author ->
    //        author.FullName |> should equal fullName
    //    | None -> 
    //        ()
        
    //[<Fact>]
    //member __.``UPDATE should fail and create DbError`` () =
    //    let authorId = 1
    //    let fullName = "Jim Brouwers"
    //    let authorResult =
    //        tryExec
    //            "UPDATE fake_author SET full_name = @full_name WHERE aauthor_id = @author_idda"
    //            [ 
    //                DbParam.create "author_id" (SqlType.Int authorId)
    //                DbParam.create "full_name" (SqlType.String fullName)
    //            ]
    //            conn

    //    authorResult |> should be instanceOfType<DbResult<unit>>
                
    //    match authorResult with
    //    | Error ex -> ex |> should be instanceOfType<Exception>
    //    | _ -> "Ok should not be Ok" |> should equal false

    //[<Fact>]
    //member __.``UPDATE async author then retrieve to verify`` () =
    //    let authorId = 1
    //    let fullName = "Jim Brouwers"
    //    execAsync
    //        "UPDATE author SET full_name = @full_name WHERE author_id = @author_id"
    //        [ 
    //            DbParam.create "author_id" (SqlType.Int authorId)
    //            DbParam.create "full_name" (SqlType.String fullName)
    //        ]
    //        conn
    //    |> Async.AwaitTask
    //    |> Async.RunSynchronously
                
    //    let author = 
    //        querySingleAsync
    //            "SELECT author_id, full_name
    //                FROM   author
    //                WHERE  author_id = @author_id"
    //                [ DbParam.create "author_id" (SqlType.Int authorId) ]
    //                Author.FromReader            
    //                conn 
    //        |> Async.AwaitTask
    //        |> Async.RunSynchronously

    //    author.IsSome |> should equal true

    //    match author with
    //    | Some author ->
    //        author.FullName |> should equal fullName
    //    | None -> 
    //        ()
        
    //[<Fact>]
    //member __.``INSERT+SELECT binary should work`` () =
    //    let testString = "A sample of bytes"
    //    let bytes = Text.Encoding.UTF8.GetBytes(testString)
    //    let fileId = 
    //        scalar
    //            "INSERT INTO file (data) VALUES (@data); SELECT LAST_INSERT_ROWID();" 
    //            [ DbParam.create "data" (SqlType.Bytes bytes) ]
    //            Convert.ToInt32
    //            conn

    //    let retrievedBytes =
    //        querySingle
    //            "SELECT data FROM file WHERE file_id = @file_id"
    //            [ DbParam.create "file_id" (SqlType.Int fileId) ]
    //            (fun rd -> rd.GetBytes("data"))
    //            conn

    //    match retrievedBytes with
    //    | Some b -> 
    //        let str = Text.Encoding.UTF8.GetString(b)
    //        b |> should equal bytes
    //        str |> should equal testString
    //    | None   -> true |> should equal "Invalid bytes returned"            