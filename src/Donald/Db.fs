namespace Donald

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open System.Threading

[<RequireQualifiedAccess>]
module Db =
    /// Create a new DbUnit instance using the provided IDbConnection.
    let newCommand (commandText : string) (conn : IDbConnection) : DbUnit =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- commandText
        new DbUnit(cmd)

    /// Configure the CancellationToken for the provided DbUnit
    let setCancellationToken (cancellationToken : CancellationToken) (dbunit : DbUnit) : DbUnit =
        dbunit.CancellationToken <- cancellationToken
        dbunit

    /// Configure the CommandBehavior for the provided DbUnit
    let setCommandBehavior (commandBehavior : CommandBehavior) (dbUnit : DbUnit) : DbUnit =
        dbUnit.CommandBehavior <- commandBehavior
        dbUnit

    /// Configure the CommandType for the provided DbUnit
    let setCommandType (commandType : CommandType) (dbUnit : DbUnit) : DbUnit =
        dbUnit.Command.CommandType <- commandType
        dbUnit

    /// Configure the command parameters for the provided DbUnit
    let setParams (param : RawDbParams) (dbUnit : DbUnit) : DbUnit =
        dbUnit.Command.SetDbParams(DbParams.create param) |> ignore
        dbUnit

    /// Configure the timeout for the provided DbUnit
    let setTimeout (commandTimeout : int) (dbUnit : DbUnit) : DbUnit =
        dbUnit.Command.CommandTimeout <- commandTimeout
        dbUnit

    /// Configure the transaction for the provided DbUnit
    let setTransaction (tran : IDbTransaction) (dbUnit : DbUnit) : DbUnit =
        dbUnit.Command.Transaction <- tran
        dbUnit

    //
    // Execution model

    let private tryDo (dbUnit : DbUnit) (fn : IDbCommand -> 'a) : 'a =
        dbUnit.Command.Connection.TryOpenConnection()
        let result = fn dbUnit.Command
        (dbUnit :> IDisposable).Dispose()
        result

    /// Execute parameterized query with no results.
    let exec (dbUnit : DbUnit) : unit =
        tryDo dbUnit (fun cmd -> cmd.Exec())

    /// Execute parameterized query many times with no results.
    let execMany (param : RawDbParams list) (dbUnit : DbUnit) : unit =
        dbUnit.Command.Connection.TryOpenConnection()
        for p in param do
            let dbParams = DbParams.create p
            dbUnit.Command.SetDbParams(dbParams).Exec()

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : 'a =
        tryDo dbUnit (fun cmd ->
            let value = cmd.ExecScalar()
            convert value)

    /// Execute paramterized query and return IDataReader
    let read (fn : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : 'a =
        tryDo dbUnit (fun cmd ->
            use rd = cmd.ExecReader(dbUnit.CommandBehavior) :?> 'reader
            fn rd)

    /// Execute parameterized query, enumerate all records and apply mapping.
    let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : 'a list =
        read (fun rd -> [ while rd.Read() do yield map rd ]) dbUnit

    /// Execute paramterized query, read only first record and apply mapping.
    let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : 'a option =
        read (fun rd -> if rd.Read() then Some(map rd) else None) dbUnit

    module Async =
        let private tryDoAsync (dbUnit : DbUnit) (fn : DbCommand -> Task<'a>) : Task<'a> =
            task {
                do! dbUnit.Command.Connection.TryOpenConnectionAsync(dbUnit.CancellationToken)
                let! result = fn (dbUnit.Command :?> DbCommand)
                (dbUnit :> IDisposable).Dispose()
                return result }

        /// Asynchronously execute parameterized query with no results.
        let exec (dbUnit : DbUnit) : Task<unit> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                let! _ = cmd.ExecAsync(dbUnit.CancellationToken)
                return () })

        /// Asynchronously execute parameterized query many times with no results
        let execMany (param : RawDbParams list) (dbUnit : DbUnit) : Task<unit> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                for p in param do
                    let dbParams = DbParams.create p
                    let! _ = cmd.SetDbParams(dbParams).ExecAsync(dbUnit.CancellationToken)
                    ()
                return () })

        /// Execute scalar query and box the result.
        let scalar (convert : obj -> 'a) (dbUnit : DbUnit) : Task<'a> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                let! value = cmd.ExecScalarAsync(dbUnit.CancellationToken)
                return convert value })

        /// Asynchronously execute paramterized query and return IDataReader
        let read (fn : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<'a> =
            tryDoAsync dbUnit (fun (cmd : DbCommand) -> task {
                use! rd = cmd.ExecReaderAsync(dbUnit.CommandBehavior, dbUnit.CancellationToken)
                return fn (rd :?> 'reader) })

        /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
        let query (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<'a list> =
            read (fun rd -> [ while rd.Read() do map rd ]) dbUnit

        /// Asynchronously execute paramterized query, read only first record and apply mapping.
        let querySingle (map : 'reader -> 'a when 'reader :> IDataReader) (dbUnit : DbUnit) : Task<'a option> =
            read (fun rd -> if rd.Read() then Some(map rd) else None) dbUnit
