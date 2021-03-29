[<AutoOpen>]
module Donald.Command 

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

type IDbCommand with
    member internal this.SetDbParams(dbParams : DbParams) =
        let setParamValue (p : IDbDataParameter) (v : obj) =
            if v = null then p.Value <- DBNull.Value
            else p.Value <- v

        this.Parameters.Clear() // clear to ensure a clean working set

        for param in dbParams do
            let p = this.CreateParameter()
            p.ParameterName <- param.Name
            
            match param.Value with
            | Null -> 
                p.Value <- DBNull.Value

            | String v -> 
                p.DbType <- DbType.String
                setParamValue p v

            | AnsiString v ->
                p.DbType <- DbType.AnsiString
                setParamValue p v

            | Boolean v -> 
                p.DbType <- DbType.Boolean
                setParamValue p v

            | Byte v -> 
                p.DbType <- DbType.Byte
                setParamValue p v

            | Char v -> 
                p.DbType <- DbType.AnsiString
                setParamValue p v

            | AnsiChar v ->
                p.DbType <- DbType.String
                setParamValue p v

            | Decimal v -> 
                p.DbType <- DbType.Decimal
                setParamValue p v

            | Double v
            | Float v ->
                p.DbType <- DbType.Double
                setParamValue p v 

            | Int16 v -> 
                p.DbType <- DbType.Int16
                setParamValue p v

            | Int32 v 
            | Int v -> 
                p.DbType <- DbType.Int32
                setParamValue p v

            | Int64 v -> 
                p.DbType <- DbType.Int64
                setParamValue p v
                
            | Guid v -> 
                p.DbType <- DbType.Guid
                setParamValue p v

            | DateTime v -> 
                p.DbType <- DbType.DateTime
                setParamValue p v

            | DateTimeOffset v ->
                p.DbType <- DbType.DateTimeOffset
                setParamValue p v

            | Bytes v -> 
                p.DbType <- DbType.Binary
                setParamValue p v

            this.Parameters.Add(p)
            |> ignore
        this

    member private this.TryDo (fn : IDbCommand -> 'a) : 'a =        
        try 
            fn this
        with
        | :? DbException as ex -> raise (FailedExecutionError ({ Statement = this.CommandText; Error = ex }))

    member internal this.Exec () =
        this.TryDo (fun this -> this.ExecuteNonQuery() |> ignore)

    member internal this.ExecReader (?cmdBehaviour : CommandBehavior) =        
        this.TryDo (fun this -> this.ExecuteReader(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

type DbCommand with
    member private this.TryDoAsync (fn : DbCommand -> Task<'a>) : Task<'a> = task {
        try 
            return! fn this             
        with
        | :? DbException as ex -> 
            return raise (FailedExecutionError ({ Statement = this.CommandText; Error = ex }))
    }

    member internal this.SetDbParams(param : DbParams) =
        (this :> IDbCommand).SetDbParams(param) :?> DbCommand
            
    member internal this.ExecAsync() =
        this.TryDoAsync (fun this -> this.ExecuteNonQueryAsync())

    member internal this.ExecReaderAsync(?cmdBehaviour : CommandBehavior) =
        this.TryDoAsync (fun this -> this.ExecuteReaderAsync(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

