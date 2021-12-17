namespace Donald

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks

[<RequireQualifiedAccess>]
module Db =
    /// Create a new DbUnit instance using the provided IDbConnection.
    let newCommand (commandText : string) (conn : IDbConnection) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- commandText
        DbUnit(cmd)

    /// Configure the CommandBehavior for the provided DbUnit
    let setCommandBehavior (commandBehavior : CommandBehavior) (dbUnit : DbUnit) =
        dbUnit.CommandBehavior <- commandBehavior
        dbUnit

    /// Configure the CommandType for the provided DbUnit
    let setCommandType (commandType : CommandType) (dbUnit : DbUnit) =
        dbUnit.Command.CommandType <- commandType
        dbUnit

    /// Configure the command parameters for the provided DbUnit
    let setParams (param : RawDbParams) (dbUnit : DbUnit) =
        dbUnit.Command.SetDbParams(DbParams.create param) |> ignore
        dbUnit

    /// Configure the timeout for the provided DbUnit
    let setTimeout (commandTimeout : int) (dbUnit : DbUnit) =
        dbUnit.Command.CommandTimeout <- commandTimeout
        dbUnit

    /// Configure the transaction for the provided DbUnit
    let setTransaction (tran : IDbTransaction) (dbUnit : DbUnit) =
        dbUnit.Command.Transaction <- tran
        dbUnit

    let private tryDo (fn : IDbCommand -> 'a) (cmd : IDbCommand) : Result<'a, DbError> =
        try
            cmd.Connection.TryOpenConnection() |> ignore
            let result = fn cmd
            cmd.Dispose()
            Ok result
        with
        | DbFailureException e -> Error e

    /// Execute parameterized query with no results.
    let exec (dbUnit : DbUnit) : Result<unit, DbError> =
        tryDo (fun dbUnit -> dbUnit.Exec()) dbUnit.Command

    /// Execute parameterized query many times with no results.
    let execMany (param : RawDbParams list) (dbUnit : DbUnit) : Result<unit, DbError> =
        try
            for p in param do
                let dbParams = DbParams.create p
                dbUnit.Command.SetDbParams(dbParams).Exec() |> ignore

            Ok ()
        with
        | DbFailureException e -> Error e

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : Result<'a, DbError> =
        tryDo (fun cmd ->
            let value = cmd.ExecuteScalar()
            convert value)
            dbUnit.Command

    /// Execute parameterized query, enumerate all records and apply mapping.
    let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Result<'a list, DbError> =
        tryDo (fun cmd ->
            use rd = cmd.ExecReader(dbUnit.CommandBehavior) :?> 'reader
            [ while rd.Read() do yield map rd ])
            dbUnit.Command

    /// Execute paramterized query, read only first record and apply mapping.
    let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Result<'a option, DbError> =
        tryDo (fun cmd ->
            use rd = cmd.ExecReader(dbUnit.CommandBehavior) :?> 'reader
            if rd.Read() then Some(map rd) else None)
            dbUnit.Command

    /// Execute paramterized query and return IDataReader
    let read (dbUnit : DbUnit) : IDataReader =
        dbUnit.Command.ExecReader(dbUnit.CommandBehavior)

    module Async =
        let private tryDoAsync (fn : DbCommand -> Task<'a>) (cmd : IDbCommand) : Task<Result<'a, DbError>> =
            task {                
                try
                    cmd.Connection.TryOpenConnection() |> ignore
                    let! result = fn (cmd :?> DbCommand)
                    return (Ok result)
                with
                | DbFailureException e -> return Error e
            }

        /// Asynchronously execute parameterized query with no results.
        let exec (dbUnit : DbUnit) : Task<Result<unit, DbError>> =
            let inner = fun (cmd : DbCommand) -> task {
                let! _ = cmd.ExecAsync()
                return ()
            }
            tryDoAsync inner dbUnit.Command

        /// Asynchronously execute parameterized query many times with no results
        let execMany (param : RawDbParams list) (dbUnit : DbUnit) : Task<Result<unit, DbError>> =
            let inner = fun (cmd : DbCommand) -> task {
                for p in param do
                    let dbParams = DbParams.create p
                    let! _ = cmd.SetDbParams(dbParams).ExecAsync()
                    ()

                return ()
            }
            tryDoAsync inner dbUnit.Command

        /// Execute scalar query and box the result.
        let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : Task<Result<'a, DbError>> =
            let inner = fun (cmd : DbCommand) -> task {
                let! value = cmd.ExecuteScalarAsync()
                return convert value
            }
            tryDoAsync inner dbUnit.Command

        /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
        let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<Result<'a list, DbError>> =
            let inner = fun (cmd : IDbCommand) -> task {
                use! rd = (cmd :?> DbCommand).ExecReaderAsync(dbUnit.CommandBehavior)
                let rd' = rd :?> 'reader
                return [ while rd.Read() do map rd' ]
            }
            tryDoAsync inner dbUnit.Command

        /// Asynchronously execute paramterized query, read only first record and apply mapping.
        let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<Result<'a option, DbError>> =
            let inner = fun (cmd : DbCommand) -> task {
                use! rd = cmd.ExecReaderAsync(dbUnit.CommandBehavior)
                let rd' = rd :?> 'reader
                return if rd.Read() then Some(map rd') else None
            }
            tryDoAsync inner dbUnit.Command

        /// Asynchronously execute paramterized query and return IDataReader
        let read (dbUnit : DbUnit) : Task<IDataReader> =
            let cmd' = dbUnit.Command :?> DbCommand

            task {
                let! rd = cmd'.ExecReaderAsync(dbUnit.CommandBehavior)
                let result = rd :> IDataReader
                return result
            }