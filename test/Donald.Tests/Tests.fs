module Donald.Tests

open System
open System.Data
open System.Data.SQLite
open System.IO
open Xunit
open Donald
open FsUnit.Xunit
open System.Threading

let conn = new SQLiteConnection("Data Source=:memory:;Version=3;New=true;")

// let shouldNotBeError pred (result : Result<'a, DbError>) =
//     match result with
//     | Ok result' -> pred result'
//     | Error e -> sprintf "DbResult should not be Error: %A" e |> should equal false

// let shouldNotBeOk (result : Result<'a, DbError>) =
//     match result with
//     | Error ex -> ex |> should be instanceOfType<DbError>
//     | _ -> "DbResult should not be Ok" |> should equal false

type Author =
    { AuthorId : int
      FullName : string }

    static member FromReader (rd : IDataReader) =
        { AuthorId = rd.ReadInt32 "author_id"
          FullName = rd.ReadString "full_name" }

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
        member _.Dispose() =
            conn.Dispose()
            ()

[<CollectionDefinition("Db")>]
type DbCollection () =
    interface ICollectionFixture<DbFixture>

[<Collection("Db")>]
type ExecutionTests() =
    [<Fact>]
    member _.``SELECT all sql types`` () =
        let sql = "
            SELECT  @p_null AS p_null
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
            , @p_date_time AS p_date_time"

        let guidParam = Guid.NewGuid ()
        let dateTimeParam = DateTime.Now
        let param =
            [
                "p_null", SqlType.Null
                "p_string", sqlString "p_string"
                "p_ansi_string", SqlType.AnsiString "p_ansi_string"
                "p_boolean", sqlBoolean false
                "p_byte", sqlByte Byte.MinValue
                "p_char", sqlChar 'a'
                "p_ansi_char", SqlType.AnsiChar Char.MinValue
                "p_decimal", sqlDecimal 0.0M
                "p_double", sqlDouble 0.0
                "p_float", sqlFloat 0.0
                "p_guid", sqlGuid guidParam
                "p_int16", sqlInt16 16s
                "p_int32", sqlInt32 32
                "p_int64", sqlInt64 64L
                "p_date_time", sqlDateTime dateTimeParam
            ]

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.querySingle (fun rd ->
            {|
                p_null = rd.ReadString "p_null"
                p_string = rd.ReadString "p_string"
                p_ansi_string = rd.ReadString "p_ansi_string"
                p_boolean = rd.ReadBoolean "p_boolean"
                p_byte = rd.ReadByte "p_byte"
                p_char = rd.ReadChar "p_char"
                p_ansi_char = rd.ReadChar "p_ansi_char"
                p_decimal = rd.ReadDecimal "p_decimal"
                p_double = rd.ReadDouble "p_double"
                p_float = rd.ReadFloat "p_float"
                p_guid = rd.ReadGuid "p_guid"
                p_int16 = rd.ReadInt16 "p_int16"
                p_int32 = rd.ReadInt32 "p_int32"
                p_int64 = rd.ReadInt64 "p_int64"
                p_date_time = rd.ReadDateTime "p_date_time"
            |})
        |> fun result ->
            result.IsSome |> should equal true
            result.Value.p_null |> should equal ""
            result.Value.p_string |> should equal "p_string"
            result.Value.p_ansi_string |> should equal "p_ansi_string"
            result.Value.p_boolean |> should equal false
            result.Value.p_byte |> should equal Byte.MinValue
            result.Value.p_char |> should equal 'a'
            result.Value.p_ansi_char |> should equal Char.MinValue
            result.Value.p_decimal |> should equal 0.0M
            result.Value.p_double |> should equal 0.0
            result.Value.p_float |> should equal 0.0
            result.Value.p_guid |> should equal guidParam
            result.Value.p_int16 |> should equal 16s
            result.Value.p_int32 |> should equal 32
            result.Value.p_int64 |> should equal 64L
            result.Value.p_date_time |> should equal dateTimeParam

    [<Fact>]
    member _.``DbUnit dispose`` () =
        use dbUnit = Db.newCommand "SELECT @p AS p" conn

        dbUnit
        |> Db.setParams [ "p", SqlType.Int32 1 ]
        |> Db.querySingle (fun rd -> rd.ReadInt32 "p")
        |> fun result ->
            result.IsSome |> should equal true
            result.Value |> should equal 1

    [<Fact>]
    member _.``DbUnit.toDetailString`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id IN (1,2)"

        conn
        |> Db.newCommand sql
        |> DbUnit.toDetailString
        |> fun str ->
            str.Length |> should greaterThan 0

    [<Fact>]
    member _.``SELECT records`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id IN (1,2)"

        conn
        |> Db.newCommand sql
        |> Db.query Author.FromReader
        |> fun result ->
            result.Length |> should equal 2
            result[0].FullName |> should equal "Pim Brouwers"
            result[1].FullName |> should equal "John Doe"

    [<Fact>]
    member _.``SELECT records async`` () =

        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id IN (1,2)"
        conn
        |> Db.newCommand sql
        |> Db.Async.query Author.FromReader
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> fun result ->
            result.Length |> should equal 2
            result[0].FullName |> should equal "Pim Brouwers"
            result[1].FullName |> should equal "John Doe"


    [<Fact>]
    member _.``SELECT records should fail`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   fake_author"

        let query () =
            conn
            |> Db.newCommand sql
            |> Db.query Author.FromReader
            |> ignore

        query |> should throw typeof<DbExecutionException>

    [<Fact>]
    member _.``SELECT records with invalid field name should fail`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author"

        let query () =
            conn
            |> Db.newCommand sql
            |> Db.query (fun rd -> rd.ReadString "email")
            |> ignore

        query |> should throw typeof<DbReaderException>


    [<Fact>]
    member _.``SELECT NULL`` () =
        let sql = "SELECT NULL AS full_name, NULL AS age"

        conn
        |> Db.newCommand sql
        |> Db.querySingle (fun rd ->
            {|
                FullName = rd.ReadStringOption "full_name" |> Option.defaultValue null
                Age = rd.ReadInt32Option "age" |> Option.toNullable
            |})
        |> fun result ->
            result.IsSome |> should equal true
            result.Value.FullName |> should equal null
            result.Value.Age |> should equal null

    [<Fact>]
    member _.``SELECT scalar value`` () =
        let sql = "SELECT 1"

        conn
        |> Db.newCommand sql
        |> Db.scalar Convert.ToInt32
        |> should equal 1

    [<Fact>]
    member _.``SELECT scalar value async`` () =
        let sql = "SELECT 1"

        conn
        |> Db.newCommand sql
        |> Db.Async.scalar Convert.ToInt32
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> should equal 1

    [<Fact>]
    member _.``SELECT single record`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id = 1"

        conn
        |> Db.newCommand sql
        |> Db.setCommandBehavior CommandBehavior.Default
        |> Db.querySingle (fun rd ->
            { FullName = rd.ReadString "full_name"
              AuthorId = rd.ReadInt32 "author_id" })
        |> fun result ->
            result.IsSome         |> should equal true
            result.Value.AuthorId |> should equal 1

    [<Fact>]
    member _.``SELECT single record async`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id = 1"

        conn
        |> Db.newCommand sql
        |> Db.setCommandBehavior CommandBehavior.Default
        |> Db.Async.querySingle (fun rd ->
            { FullName = rd.ReadString "full_name"
              AuthorId = rd.ReadInt32 "author_id" })
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> fun result ->
            result.IsSome |> should equal true
            result.Value.AuthorId |> should equal 1

    [<Fact>]
    member _.``INSERT author then retrieve to verify`` () =
        let fullName = "Jane Doe"

        let sql = "
            INSERT INTO author (full_name) VALUES (@full_name);

            SELECT author_id, full_name
            FROM   author
            WHERE  author_id = LAST_INSERT_ROWID();"

        let param = [ "full_name", SqlType.String fullName ]

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.querySingle Author.FromReader
        |> fun result ->
            result.IsSome |> should equal true

            match result with
            | Some author ->
                author.FullName |> should equal fullName
            | None ->
                ()

    [<Fact>]
    member _.``INSERT author with NULL birth_date`` () =
        let fullName = "Jim Doe"
        let birthDate : DateTime option = None

        let sql = "
            INSERT INTO author (full_name, birth_date)
            VALUES (@full_name, @birth_date);"

        let param =
            [ "full_name", SqlType.String fullName
              "birth_date", match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null ]

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.exec
        |> should equal ()

    [<Fact>]
    member _.``INSERT author should fail`` () =
        let fullName = "Jane Doe"

        let sql = "
            INSERT INTO author (full_name, birth_date)
            VALUES (@full_name, @birth_date);"

        let param = [ "full_name", SqlType.String fullName ]

        let query () =
            conn
            |> Db.newCommand sql
            |> Db.setParams param
            |> Db.exec

        query |> should throw typeof<DbExecutionException>

    [<Fact>]
    member _.``INSERT MANY authors then count to verify`` () =
        let sql = "INSERT INTO author (full_name) VALUES (@full_name);"

        conn
        |> Db.newCommand sql
        |> Db.execMany
            [ [ "full_name", SqlType.String "Bugs Bunny" ]
              [ "full_name", SqlType.String "Donald Duck" ] ]
        |> ignore

        let sql = "
            SELECT  author_id
                  , full_name
            FROM    author
            WHERE   full_name IN ('Bugs Bunny', 'Donald Duck')"

        conn
        |> Db.newCommand sql
        |> Db.query Author.FromReader
        |> fun result -> result.Length |> should equal 2

    [<Fact>]
    member _.``INSERT TRAN MANY authors then count to verify async`` () =
        use tran = conn.TryBeginTransaction()

        let sql = "
            INSERT INTO author (full_name)
            VALUES (@full_name);"

        conn
        |> Db.newCommand sql
        |> Db.setTransaction tran
        |> Db.Async.execMany
            [ [ "full_name", SqlType.String "Batman" ]
              [ "full_name", SqlType.String "Superman" ] ]
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> ignore

        tran.TryCommit()

        let sql = "
            SELECT  author_id
                  , full_name
            FROM    author
            WHERE   full_name IN ('Batman', 'Superman')"

        conn
        |> Db.newCommand sql
        |> Db.Async.query Author.FromReader
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> fun result -> result.Length |> should equal 2

    [<Fact>]
    member _.``INSERT MANY should fail`` () =
        let sql = "
            INSERT INTO fake_author (full_name)
            VALUES (@full_name);"

        let query () =
            conn
            |> Db.newCommand sql
            |> Db.execMany
                [ [ "full_name", SqlType.String "Bugs Bunny" ]
                  [ "full_name", SqlType.String "Donald Duck" ] ]

        query |> should throw typeof<DbExecutionException>

    [<Fact>]
    member _.``INSERT+SELECT binary should work`` () =
        let testString = "A sample of bytes"
        let bytes = Text.Encoding.UTF8.GetBytes(testString)

        let sql = "
            INSERT INTO file (data) VALUES (@data);
            SELECT data FROM file WHERE file_id = LAST_INSERT_ROWID();"

        let param = [ "data", SqlType.Bytes bytes ]

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.querySingle (fun rd -> rd.ReadBytes "data")
        |> fun result ->
            match result with
            | Some b ->
                let str = Text.Encoding.UTF8.GetString(b)
                b |> should equal bytes
                str |> should equal testString
            | None -> true |> should equal "Invalid bytes returned"

    [<Fact>]
    member _.``INSERT TRAN author then retrieve to verify`` () =
        let fullName = "Janet Doe"
        let param = [ "full_name", SqlType.String fullName ]
        use tran = conn.TryBeginTransaction()
        let type' = CommandType.Text

        let sql = "INSERT INTO author (full_name) VALUES (@full_name);"

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.setTransaction tran
        |> Db.setTimeout 10
        |> Db.setCommandType type'
        |> Db.exec
        |> ignore

        tran.TryCommit()

        let sql = "SELECT author_id, full_name
                   FROM   author
                   WHERE  full_name = @full_name;"

        conn
        |> Db.newCommand sql
        |> Db.setParams param
        |> Db.querySingle Author.FromReader
        |> fun result ->
            result.IsSome |> should equal true
            result.Value.FullName |> should equal "Janet Doe"

    [<Fact>]
    member _.``IDataReader via read`` () =
        let sql = "
        SELECT author_id, full_name
        FROM   author
        WHERE  author_id IN (1,2)"

        conn
        |> Db.newCommand sql
        |> Db.read (fun rd ->
            [ while rd.Read() do Author.FromReader rd ])
        |> fun result ->
            result.Length |> should equal 2
            result[0].FullName |> should equal "Pim Brouwers"
            result[1].FullName |> should equal "John Doe"

    [<Fact>]
    member _.``Returning Task<IDataReader> via async read`` () =
        let sql = "
        SELECT author_id, full_name
        FROM   author
        WHERE  author_id IN (1,2)"

        conn
        |> Db.newCommand sql
        |> Db.Async.read (fun rd ->
            [ while rd.Read() do Author.FromReader rd ])
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> fun result ->
            result.Length |> should equal 2
            result[0].FullName |> should equal "Pim Brouwers"
            result[1].FullName |> should equal "John Doe"

    [<Fact>]
    member _.``SELECT scalar Canceled request should be canceled`` () =
        let sql = "SELECT 1"

        use cts = new CancellationTokenSource()
        cts.Cancel()
        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setCancellationToken cts.Token
            |> Db.Async.scalar Convert.ToInt32
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> ignore
        action |> should throw typeof<Tasks.TaskCanceledException>

    [<Fact>]
    member _.``SELECT querySingle Canceled request should be canceled`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id = 1"

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setCommandBehavior CommandBehavior.Default
            |> Db.setCancellationToken cts.Token
            |> Db.Async.querySingle (fun rd ->
                { FullName = rd.ReadString "full_name"
                  AuthorId = rd.ReadInt32 "author_id" })
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> ignore

        action |> should throw typeof<Tasks.TaskCanceledException>


    [<Fact>]
    member _.``SELECT query Canceled request should be canceled`` () =
        let sql = "
            SELECT author_id, full_name
            FROM   author
            WHERE  author_id = 1"

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setCommandBehavior CommandBehavior.Default
            |> Db.setCancellationToken cts.Token
            |> Db.Async.query (fun rd ->
                { FullName = rd.ReadString "full_name"
                  AuthorId = rd.ReadInt32 "author_id" })
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> ignore

        action |> should throw typeof<Tasks.TaskCanceledException>

    [<Fact>]
    member _.``SELECT read Canceled request should be canceled`` () =
        let sql = "
        SELECT author_id, full_name
        FROM   author
        WHERE  author_id IN (1,2)"

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setCancellationToken cts.Token
            |> Db.Async.read (fun _ -> ())
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> should equal ()

        action |> should throw typeof<Tasks.TaskCanceledException>

    [<Fact>]
    member _.``INSERT exec Canceled request should be canceled`` () =
        let fullName = "Jim Doe2"
        let birthDate : DateTime option = None
        use cts = new CancellationTokenSource()
        cts.Cancel()

        let sql = "
            INSERT INTO author (full_name, birth_date)
            VALUES (@full_name, @birth_date);"

        let param =
            [ "full_name", SqlType.String fullName
              "birth_date", match birthDate with Some b -> SqlType.DateTime b | None -> SqlType.Null ]

        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setParams param
            |> Db.setCancellationToken cts.Token
            |> Db.Async.exec
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> ignore

        action |> should throw typeof<Tasks.TaskCanceledException>

    [<Fact>]
    member _.``INSERT execMany Canceled request should be canceled``  () =
        let sql = "INSERT INTO author (full_name) VALUES (@full_name);"

        use cts = new CancellationTokenSource()
        cts.Cancel()

        let action () =
            conn
            |> Db.newCommand sql
            |> Db.setCancellationToken cts.Token
            |> Db.Async.execMany
                [ [ "full_name", SqlType.String "Bugs Bunny2" ]
                  [ "full_name", SqlType.String "Donald Duck2" ] ]
            |> Async.AwaitTask
            |> Async.RunSynchronously
            |> ignore

        action |> should throw typeof<Tasks.TaskCanceledException>
