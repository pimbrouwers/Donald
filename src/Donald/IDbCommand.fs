namespace Donald

open System
open System.Data
open System.Data.Common
open System.Threading

[<AutoOpen>]
module IDbCommandExtensions =
    type IDbCommand with
        member internal x.ToDetailString() =
            let cmd  = x :?> DbCommand
            let param =
                [ for i in 0 .. cmd.Parameters.Count - 1 ->
                    let p = cmd.Parameters.[i]
                    let pName = p.ParameterName
                    let pValue = if isNull p.Value || p.Value = DBNull.Value then "NULL" else string p.Value
                    String.Concat("@", pName, " = ", pValue) ]
                |> fun str -> String.Join(", ", str)
                |> fun str -> if (String.IsNullOrWhiteSpace(str)) then "--" else str

            String.Join("\n\n", param, cmd.CommandText)

        member internal x.Exec() =
            try
                x.ExecuteNonQuery() |> ignore
            with
            | :? DbException as ex -> raise (DbExecutionException(x, ex))

        member internal x.ExecReader(cmdBehavior : CommandBehavior) =
            try
                x.ExecuteReader(cmdBehavior)
            with
            | :? DbException as ex -> raise (DbExecutionException(x, ex))

        member internal x.ExecScalar() =
            try
                x.ExecuteScalar()
            with
            | :? DbException as ex -> raise (DbExecutionException(x, ex))

        member internal x.SetParamsRaw(rawDbParams : (string * obj) list) =
            x.Parameters.Clear()
            for (name, value) in rawDbParams do
                let p = x.CreateParameter()
                p.ParameterName <- name
                match isNull value with
                | true -> p.Value <- DBNull.Value
                | false -> p.Value <- value
                x.Parameters.Add(p) |> ignore
            x

        member internal x.SetParams(dbParams : DbParams) =
            let setParamValue (dbType : DbType) (p : IDbDataParameter) (v : obj) =
                p.DbType <- dbType
                if isNull v then p.Value <- DBNull.Value
                else p.Value <- v

            x.Parameters.Clear() // clear to ensure a clean working set

            for param in dbParams do
                let p = x.CreateParameter()
                p.ParameterName <- param.Name

                match param.Value with
                | SqlType.Null -> p.Value <- DBNull.Value
                | SqlType.String v -> setParamValue DbType.String p v
                | SqlType.AnsiString v -> setParamValue DbType.AnsiString p v
                | SqlType.Boolean v -> setParamValue DbType.Boolean p v
                | SqlType.Byte v -> setParamValue DbType.Byte p v
                | SqlType.Char v
                | SqlType.AnsiChar v -> setParamValue DbType.Object p v
                | SqlType.Decimal v -> setParamValue DbType.Decimal p v
                | SqlType.Double v
                | SqlType.Float v -> setParamValue DbType.Double p v
                | SqlType.Int16 v -> setParamValue DbType.Int16 p v
                | SqlType.Int32 v
                | SqlType.Int v -> setParamValue DbType.Int32 p v
                | SqlType.Int64 v -> setParamValue DbType.Int64 p v
                | SqlType.Guid v -> setParamValue DbType.Guid p v
                | SqlType.DateTime v -> setParamValue DbType.DateTime p v
                | SqlType.Bytes v -> setParamValue DbType.Binary p v

                x.Parameters.Add(p)
                |> ignore

            x

    type DbCommand with
        member internal x.SetParamsRaw(rawParams : (string * obj) list) =
            (x :> IDbCommand).SetParamsRaw(rawParams) :?> DbCommand

        member internal x.SetParams(param : DbParams) =
            (x :> IDbCommand).SetParams(param) :?> DbCommand

        member internal x.ExecAsync(?ct: CancellationToken) = task {
            try
                let! _ = x.ExecuteNonQueryAsync(cancellationToken = defaultArg ct CancellationToken.None)
                ()
            with
            | :? DbException as ex -> return raise (DbExecutionException(x, ex))
        }

        member internal x.ExecReaderAsync(cmdBehavior : CommandBehavior, ?ct: CancellationToken) = task {
            try
                return! x.ExecuteReaderAsync(cmdBehavior, cancellationToken = defaultArg ct CancellationToken.None )
            with
            | :? DbException as ex -> return raise (DbExecutionException(x, ex))
        }

        member internal x.ExecScalarAsync(?ct: CancellationToken) = task {
            try
                return! x.ExecuteScalarAsync(cancellationToken = defaultArg ct CancellationToken.None )
            with
            | :? DbException as ex -> return raise (DbExecutionException(x, ex))
        }
