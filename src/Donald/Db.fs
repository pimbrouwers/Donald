namespace Donald

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open System.Threading

[<RequireQualifiedAccess>]
module Db =
    /// Create a new DbUnit instance using the provided IDbConnection.
    let newCommand (commandText : string) (conn : IDbConnection) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- commandText
        new DbUnit(cmd)

    /// Configure the CancellationToken for the provided DbUnit
    let setCancellationToken (cancellationToken : CancellationToken) (dbunit : DbUnit) =
        dbunit.CancellationToken <- cancellationToken
        dbunit

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

    //
    // Execution model

    let private tryDo (dbUnit : DbUnit) (fn : IDbCommand -> 'a) : Result<'a, DbError> =
        try
            dbUnit.Command.Connection.TryOpenConnection ()
            let result = fn dbUnit.Command
            (dbUnit :> IDisposable).Dispose ()
            Ok result
        with
        | DbFailureException e -> Error e

    /// Execute parameterized query with no results.
    let exec (dbUnit : DbUnit) : Result<unit, DbError> =
        tryDo dbUnit (fun cmd ->
            cmd.Exec ())

    /// Execute parameterized query many times with no results.
    let execMany (param : RawDbParams list) (dbUnit : DbUnit) : Result<unit, DbError> =
        try
            dbUnit.Command.Connection.TryOpenConnection ()
            for p in param do
                let dbParams = DbParams.create p
                dbUnit.Command.SetDbParams(dbParams).Exec () |> ignore

            Ok ()
        with
        | DbFailureException e -> Error e

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : Result<'a, DbError> =
        tryDo dbUnit (fun cmd ->
            let value = cmd.ExecScalar ()
            convert value)

    /// Execute paramterized query and return IDataReader
    let read (fn : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Result<'a, DbError> =
        tryDo dbUnit (fun cmd ->
            use rd = cmd.ExecReader (dbUnit.CommandBehavior) :?> 'reader
            fn rd)

    /// Execute parameterized query, enumerate all records and apply mapping.
    let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Result<'a list, DbError> =
        read (fun rd -> [ while rd.Read () do yield map rd ]) dbUnit


    /// Execute paramterized query, read only first record and apply mapping.
    let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Result<'a option, DbError> =
        read (fun rd -> if rd.Read () then Some(map rd) else None) dbUnit

    module Async =
        let private tryDoAsync (dbUnit : DbUnit) (fn : DbCommand -> Task<'a>) : Task<Result<'a, DbError>> =
            task {
                try
                    do! dbUnit.Command.Connection.TryOpenConnectionAsync(dbUnit.CancellationToken)
                    let! result = fn (dbUnit.Command :?> DbCommand)
                    (dbUnit :> IDisposable).Dispose ()
                    return (Ok result)
                with
                | DbFailureException e -> return Error e
            }

        /// Asynchronously execute parameterized query with no results.
        let exec (dbUnit : DbUnit) : Task<Result<unit, DbError>> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                let! _ = cmd.ExecAsync(dbUnit.CancellationToken)
                return ()
            })

        /// Asynchronously execute parameterized query many times with no results
        let execMany (param : RawDbParams list) (dbUnit : DbUnit) : Task<Result<unit, DbError>> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                for p in param do
                    let dbParams = DbParams.create p
                    let! _ = cmd.SetDbParams(dbParams).ExecAsync(dbUnit.CancellationToken)
                    ()

                return ()
            })

        /// Execute scalar query and box the result.
        let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : Task<Result<'a, DbError>> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                let! value = cmd.ExecScalarAsync (dbUnit.CancellationToken)
                return convert value
            })

        /// Asynchronously execute paramterized query and return IDataReader
        let read (fn : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<Result<'a, DbError>> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                use! rd = cmd.ExecReaderAsync(dbUnit.CommandBehavior, dbUnit.CancellationToken)
                return fn (rd :?> 'reader)
            })

        /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
        let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<Result<'a list, DbError>> =
            read (fun rd -> [ while rd.Read () do map rd ]) dbUnit

        /// Asynchronously execute paramterized query, read only first record and apply mapping.
        let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<Result<'a option, DbError>> =
            read (fun rd -> if rd.Read () then Some(map rd) else None) dbUnit