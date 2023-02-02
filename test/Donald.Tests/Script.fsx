#r "nuget: System.Data.SQLite"

open System
open System.Data

type DbReaderException =
    inherit Exception
    val FieldName : string option
    new() = { inherit Exception(); FieldName = None }
    new(message : string) = { inherit Exception(message); FieldName = None }
    new(message : string, inner : Exception) = { inherit Exception(message, inner); FieldName = None }
    new(fieldName : string, inner : IndexOutOfRangeException) = { inherit Exception($"Failed to read database field: '{fieldName}'", inner); FieldName = Some fieldName }
    new(fieldName : string, inner : InvalidCastException) = { inherit Exception($"Failed to read database field: '{fieldName}'", inner); FieldName = Some fieldName }

type IDataReader with
    member private x.GetOrdinalOption (name : string) =
        try
            let i = x.GetOrdinal(name)

            if i < 0 then raise (IndexOutOfRangeException(name + " is not a valid field name"))

            match x.IsDBNull(i) with
            | true  -> None
            | false -> Some(i)
        with
        | :? IndexOutOfRangeException as ex -> raise (DbReaderException(name, ex))

open System.Data.SQLite

let conn = new SQLiteConnection("Data Source=:memory:;Version=3;New=true;")
conn.Open ()

let sql = "
    WITH author AS (
        SELECT 1 AS author_id, 'pim brouwers' AS full_name
    )
    SELECT author_id, full_name
    FROM   author
    WHERE  1 = 2"

let cmd = conn.CreateCommand ()
cmd.CommandText <- sql

let rd = cmd.ExecuteReader ()
[ while rd.Read () do rd.GetOrdinalOption "email" ]
|> printfn "%A"