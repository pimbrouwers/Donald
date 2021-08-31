[<AutoOpen>]
module Donald.Command 

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks

type IDbCommand with
    member internal this.SetDbParams(dbParams : DbParams) =
        let setParamValue (p : IDbDataParameter) (v : obj) =
            if isNull v then p.Value <- DBNull.Value
            else p.Value <- v

        this.Parameters.Clear() // clear to ensure a clean working set

        for param in dbParams do
            let p = this.CreateParameter()
            p.ParameterName <- param.Name
            
            match param.Value with
            | SqlType.Null -> 
                p.Value <- DBNull.Value

            | SqlType.String v -> 
                p.DbType <- DbType.String
                setParamValue p v

            | SqlType.AnsiString v ->
                p.DbType <- DbType.AnsiString
                setParamValue p v

            | SqlType.Boolean v -> 
                p.DbType <- DbType.Boolean
                setParamValue p v

            | SqlType.Byte v -> 
                p.DbType <- DbType.Byte
                setParamValue p v

            | SqlType.Char v -> 
                p.DbType <- DbType.AnsiString
                setParamValue p v

            | SqlType.AnsiChar v ->
                p.DbType <- DbType.String
                setParamValue p v

            | SqlType.Decimal v -> 
                p.DbType <- DbType.Decimal
                setParamValue p v

            | SqlType.Double v
            | SqlType.Float v ->
                p.DbType <- DbType.Double
                setParamValue p v 

            | SqlType.Int16 v -> 
                p.DbType <- DbType.Int16
                setParamValue p v

            | SqlType.Int32 v 
            | SqlType.Int v -> 
                p.DbType <- DbType.Int32
                setParamValue p v

            | SqlType.Int64 v -> 
                p.DbType <- DbType.Int64
                setParamValue p v
                
            | SqlType.Guid v -> 
                p.DbType <- DbType.Guid
                setParamValue p v

            | SqlType.DateTime v -> 
                p.DbType <- DbType.DateTime
                setParamValue p v

            | SqlType.DateTimeOffset v ->
                p.DbType <- DbType.DateTimeOffset
                setParamValue p v

            | SqlType.Bytes v -> 
                p.DbType <- DbType.Binary
                setParamValue p v

            this.Parameters.Add(p)
            |> ignore
        this

    member private this.TryDo (fn : IDbCommand -> 'a) : 'a =        
        try 
            fn this
        with
        | :? DbException as ex -> raise (FailedExecutionException ({ Statement = this.CommandText; Error = ex }))

    member internal this.Exec () =
        this.TryDo (fun this -> this.ExecuteNonQuery() |> ignore)

    member internal this.ExecReader (?cmdBehaviour : CommandBehavior) =        
        this.TryDo (fun this -> this.ExecuteReader(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

type DbCommand with
    member private this.TryDoAsync (fn : DbCommand -> Task<'a>) : Task<'a> = 
        try 
            fn this             
        with
        | :? DbException as ex -> raise (FailedExecutionException ({ Statement = this.CommandText; Error = ex }))    

    member internal this.SetDbParams(param : DbParams) =
        (this :> IDbCommand).SetDbParams(param) :?> DbCommand
            
    member internal this.ExecAsync() =
        this.TryDoAsync (fun this -> this.ExecuteNonQueryAsync())

    member internal this.ExecReaderAsync(?cmdBehaviour : CommandBehavior) =
        this.TryDoAsync (fun this -> this.ExecuteReaderAsync(cmdBehaviour |> Option.defaultValue CommandBehavior.SequentialAccess))

