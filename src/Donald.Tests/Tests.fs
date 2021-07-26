module Donald.Tests

open System
open System.Data
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
    | Error e -> sprintf "DbResult should not be Error: %s" e.Error.Message |> should equal false

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
            AuthorId = rd.ReadInt32 "author_id"
            FullName = rd.ReadString "full_name"
        }

type DbFixture () =
    do 
        use fs = IO.File.OpenRead("schema.sql")
        use sr = new StreamReader(fs)
        let sql = sr.ReadToEnd()

        conn
        |> Db.newCommand sql
        |> Db.setTimeout 30
        |> Db.setCommandType CommandType.Text        
        |> Db.exec 
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
            [
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
                p_null = rd.ReadStringOption "p_null"
                p_string = rd.ReadStringOption "p_string"
                p_ansi_string = rd.ReadStringOption "p_ansi_string"
                p_boolean = rd.ReadBooleanOption "p_boolean"
                p_byte = rd.ReadByteOption "p_byte"
                p_char = rd.ReadCharOption "p_char"
                p_ansi_char = rd.ReadCharOption "p_ansi_char"
                p_decimal = rd.ReadDecimalOption "p_decimal"
                p_double = rd.ReadDoubleOption "p_double"
                p_float = rd.ReadFloatOption "p_float"
                p_guid = rd.ReadGuidOption "p_guid"
                p_int16 = rd.ReadInt16Option "p_int16"
                p_int32 = rd.ReadInt32Option "p_int32"
                p_int64 = rd.ReadInt64Option "p_int64"
                p_date_time = rd.ReadDateTimeOption "p_date_time" 
            |}

        //dbCommand conn {
        //    cmdText sql
        //    cmdParam param            
        //}
        conn 
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.querySingle map
        |> shouldNotBeError (fun result -> result.IsSome |> should equal true)
                           
    [<Fact>]
    member __.``SELECT records`` () =                    
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
        } 
        |> Db.query Author.FromReader
        |> shouldNotBeError (fun result -> result.Length |> should equal 2)

    [<Fact>]
    member __.``SELECT records async`` () =            
 
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id IN (1,2)"
        }
        |> Db.Async.query Author.FromReader
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> result.Length |> should equal 2)

    [<Fact>]
    member __.``SELECT records should fail and create DbError`` () =        
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   fake_author"
        }
        |> Db.query Author.FromReader
        |> shouldNotBeOk

    [<Fact>]
    member __.``SELECT NULL`` () =            
        dbCommand conn {
            cmdText "SELECT NULL AS full_name, NULL AS age"
        }
        |> Db.querySingle (fun rd -> 
            {| 
                FullName = rd.ReadStringOption "full_name" |> Option.defaultValue null
                Age = rd.ReadInt32Option "age" |> Option.asNullable
            |})
        |> shouldNotBeError (fun result ->
            result.IsSome         |> should equal true
            result.Value.FullName |> should equal null
            result.Value.Age      |> should equal null)
      
    [<Fact>]
    member __.``SELECT scalar value`` () =            
        dbCommand conn {
            cmdText "SELECT 1"
        }
        |> Db.scalar Convert.ToInt32
        |> shouldNotBeError (fun result ->            
            result |> should equal 1)
    
    [<Fact>]
    member __.``SELECT scalar value async`` () =            
        dbCommand conn {
            cmdText "SELECT 1"
        }
        |> Db.Async.scalar Convert.ToInt32
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result ->            
            result |> should equal 1)

    [<Fact>]
    member __.``SELECT single record`` () =            
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                     FROM   author
                     WHERE  author_id = 1"
        }
        |> Db.querySingle Author.FromReader
        |> shouldNotBeError (fun result ->
            result.IsSome         |> should equal true
            result.Value.AuthorId |> should equal 1)

    [<Fact>]
    member __.``SELECT single record async`` () =                    
        dbCommand conn {
            cmdText "SELECT author_id, full_name
                        FROM   author
                        WHERE  author_id = 1"
        }
        |> Db.Async.querySingle Author.FromReader
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result ->            
            result.IsSome         |> should equal true
            result.Value.AuthorId |> should equal 1)
            
    [<Fact>]
    member __.``INSERT author then retrieve to verify`` () =
        let fullName = "Jane Doe"
        
        dbCommand conn {
            cmdText  "INSERT INTO author (full_name) VALUES (@full_name);

                      SELECT author_id, full_name
                      FROM   author
                      WHERE  author_id = LAST_INSERT_ROWID();"
            cmdParam [ "full_name", SqlType.String fullName ]
        }                 
        |> Db.querySingle Author.FromReader               
        |> shouldNotBeError (fun result ->  
            result.IsSome |> should equal true

            match result with
            | Some author ->
                author.FullName |> should equal fullName
            | None -> 
                ())   

    [<Fact>]
    member __.``INSERT author with NULL birth_date`` () =
        let fullName = "Jim Doe"
        let birthDate : DateTime option = None

        dbCommand conn {
            cmdText  "INSERT INTO author (full_name, birth_date) VALUES (@full_name, @birth_date);"
            cmdParam [ 
                         "full_name", SqlType.String fullName
                         "birth_date", match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null
                     ]
        }
        |> Db.exec
        |> shouldNotBeError (fun result -> ())

    [<Fact>]
    member __.``INSERT author should fail and create DbError`` () =
        let fullName = "Jane Doe"
        dbCommand conn {
            cmdText  "INSERT INTO author (full_name, birth_date) VALUES (@full_name, @birth_date);"
            cmdParam [ "full_name", SqlType.String fullName ]
        }
        |> Db.exec
        |> shouldNotBeOk

    [<Fact>]
    member __.``INSERT MANY authors then count to verify`` () =
        dbCommand conn {    
            cmdText "INSERT INTO author (full_name) VALUES (@full_name);"
        }
        |> Db.execMany [
                               [ "full_name", SqlType.String "Bugs Bunny" ]
                               [ "full_name", SqlType.String "Donald Duck" ]
                           ]
        |> ignore

        dbCommand conn {
            cmdText "SELECT author_id, full_name FROM author WHERE full_name IN ('Bugs Bunny', 'Donald Duck')"
        }
        |> Db.query Author.FromReader
        |> shouldNotBeError (fun result ->
            result |> List.length |> should equal 2)
        
    [<Fact>]
    member __.``INSERT MANY should fail and create DbError`` () =
        dbCommand conn {    
            cmdText "INSERT INTO fake_author (full_name) VALUES (@full_name);"
        }
        |> Db.execMany [
                               [ "full_name", SqlType.String "Bugs Bunny" ]
                               [ "full_name", SqlType.String "Donald Duck" ]
                           ]
        |> shouldNotBeOk
 
    [<Fact>]
    member __.``INSERT+SELECT binary should work`` () =
        let testString = "A sample of bytes"
        let bytes = Text.Encoding.UTF8.GetBytes(testString)
        
        dbCommand conn {
            cmdText "INSERT INTO file (data) VALUES (@data);
                     SELECT data FROM file WHERE file_id = LAST_INSERT_ROWID();"
            cmdParam [ "data", SqlType.Bytes bytes ]
        }
        |> Db.querySingle (fun rd -> rd.ReadBytes "data")
        |> shouldNotBeError (fun result ->
            match result with
            | Some b -> 
                let str = Text.Encoding.UTF8.GetString(b)
                b |> should equal bytes
                str |> should equal testString
            | None   -> true |> should equal "Invalid bytes returned")

    [<Fact>]
    member __.``INSERT TRAN author then retrieve to verify`` () =
        let fullName = "Janet Doe"
        let param = [ "full_name", SqlType.String fullName ]
        use tran = conn.TryBeginTransaction()

        dbCommand conn {
            cmdText  "INSERT INTO author (full_name) VALUES (@full_name);"
            cmdParam param
            cmdTran  tran
        }                 
        |> Db.exec
        |> ignore

        tran.TryCommit()        

        dbCommand conn {
            cmdText  "SELECT author_id, full_name
                      FROM   author
                      WHERE  full_name = @full_name;"
            cmdParam param
        }                 
        |> Db.querySingle Author.FromReader               
        |> shouldNotBeError (fun result ->  
            result.IsSome |> should equal true)

    [<Fact>]
    member __.``dbResult {...} INSERT author with NULL birth_date`` () =
        let fullName = "Jimmy Doe"
        let birthDate : DateTime option = None

        let cmd = dbCommand conn {
            cmdText  "INSERT INTO author (full_name, birth_date) VALUES (@full_name, @birth_date);"
            cmdParam [ 
                         "full_name", SqlType.String fullName
                         "birth_date", match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null
                     ]
        }

        dbResult {
            do! cmd |> Db.exec
        }        
        |> shouldNotBeError (fun result -> ())

    [<Fact>]
    member __.``dbResult {...} INSERT MANY authors then count to verify`` () =
        let insertCmd = dbCommand conn {    
            cmdText "INSERT INTO author (full_name) VALUES (@full_name);"
        }

        let selectCmd = dbCommand conn {
            cmdText "SELECT author_id, full_name FROM author WHERE full_name IN ('Jeremiah Doe', 'Jimmington Doe')"
        }
                
        let result = dbResult {
            do! insertCmd |> Db.execMany [
                [ "full_name", SqlType.String "Jeremiah Doe" ]
                [ "full_name", SqlType.String "Jimmington Doe" ]
            ]

            return! selectCmd |> Db.query Author.FromReader
        }
        
        result
        |> shouldNotBeError (fun result -> result |> List.length |> should equal 2)

    [<Fact>]
    member __.``dbResult {...} INSERT TRAN author then retrieve to verify`` () =
        let fullName = "Jackie Doe"
        let param = [ "full_name", SqlType.String fullName ]

        use tran = conn.TryBeginTransaction()

        let insertCmd = 
            let sql = "INSERT INTO author (full_name) VALUES (@full_name);"
            conn
            |> Db.newCommand sql
            |> Db.setParams param
            |> Db.setTransaction tran

        let selectCmd = 
            let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  full_name = @full_name;"

            conn 
            |> Db.newCommand sql
            |> Db.setParams param
                
        let result = dbResult {
            let! _ = insertCmd |> Db.exec

            let! author = selectCmd |> Db.querySingle Author.FromReader

            return author
        }

        tran.TryCommit()        

        result             
        |> shouldNotBeError (fun result -> result.IsSome |> should equal true)

    [<Fact>]
    member __.``dbResult {...} for...do loop`` () =
        let cmds = 
            [1..10]
            |> Seq.map (fun n -> dbCommand conn {
                cmdText (sprintf "SELECT %i" n)
            })

        dbResult {
            for cmd in cmds do
                cmd |> Db.scalar Convert.ToInt32 |> ignore
        }        
        |> shouldNotBeError (fun result -> ())

    [<Fact>]
    member __.``dbResult {...} for...do loop with return`` () =
        let cmds = 
            [1..10]
            |> Seq.map (fun n -> dbCommand conn {
                cmdText (sprintf "SELECT %i" n)
            })

        dbResult {
            for cmd in cmds do
                cmd |> Db.scalar Convert.ToInt32 |> ignore

            return cmds |> Seq.head |> Db.scalar Convert.ToInt32
        }
        |> shouldNotBeError ignore

    [<Fact>]
    member __.``dbResultTask {...} INSERT author with NULL birth_date`` () =
        let fullName = "Jameson Doe"
        let birthDate : DateTime option = None

        let cmd = dbCommand conn {
            cmdText  "INSERT INTO author (full_name, birth_date) VALUES (@full_name, @birth_date);"
            cmdParam [ 
                         "full_name", SqlType.String fullName
                         "birth_date", match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null
                     ]
        }

        dbResultTask {
            do! cmd |> Db.Async.exec
        }        
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> ())

    [<Fact>]
    member __.``dbResultTask {...} INSERT MANY authors then count to verify`` () =
        let insertCmd = dbCommand conn {    
            cmdText "INSERT INTO author (full_name) VALUES (@full_name);"
        }

        let selectCmd = dbCommand conn {
            cmdText "SELECT author_id, full_name FROM author WHERE full_name IN ('Jimmer Doe', 'Jer Doe')"
        }
                
        let result = dbResultTask {
            do! insertCmd |> Db.Async.execMany [
                [ "full_name", SqlType.String "Jimmer Doe" ]
                [ "full_name", SqlType.String "Jer Doe" ]
            ]

            return! selectCmd |> Db.Async.query Author.FromReader
        }
        
        result
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> result |> List.length |> should equal 2)

    [<Fact>]
    member __.``dbResultTask {...} INSERT TRAN author then retrieve to verify`` () =
        let fullName = "Jolene Doe"
        let param = [ "full_name", SqlType.String fullName ]

        use tran = conn.TryBeginTransaction()

        let insertCmd = dbCommand conn {
            cmdText  "INSERT INTO author (full_name) VALUES (@full_name);"
            cmdParam param
            cmdTran  tran
        }

        let selectCmd = dbCommand conn {
            cmdText  "SELECT author_id, full_name
                      FROM   author
                      WHERE  full_name = @full_name;"
            cmdParam param
        }
        
        let result = dbResultTask {
            let! _ = insertCmd |> Db.Async.exec

            let! author = selectCmd |> Db.Async.querySingle Author.FromReader

            return author
        }

        tran.TryCommit()        

        result             
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> result.IsSome |> should equal true)

    [<Fact>]
    member __.``dbResultTask {...} for...do loop`` () =
        let cmds = 
            [1..10]
            |> Seq.map (fun n -> dbCommand conn {
                cmdText (sprintf "SELECT %i" n)
            })

        dbResultTask {
            for cmd in cmds do
                cmd |> Db.Async.scalar Convert.ToInt32 |> ignore
        }      
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError (fun result -> ())

    [<Fact>]
    member __.``dbResultTask {...} for...do loop with return`` () =
        let cmds = 
            [1..10]
            |> Seq.map (fun n -> dbCommand conn {
                cmdText (sprintf "SELECT %i" n)
            })

        dbResultTask {
            for cmd in cmds do
                cmd |> Db.Async.scalar Convert.ToInt32 |> ignore

            return cmds |> Seq.head |> Db.Async.scalar Convert.ToInt32
        }
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> shouldNotBeError ignore