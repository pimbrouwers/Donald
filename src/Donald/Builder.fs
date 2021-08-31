[<AutoOpen>]
module Donald.DbCommandBuilder

open System
open System.Data
open System.Threading.Tasks
open FSharp.Control.Tasks

// dbResult {...}
// ------------

/// Computation expression for Result<_, DbExecutionError>.
type DbResultBuilder() =
    member _.Return (value) : Result<'a, DbExecutionError> = Ok value

    member _.ReturnFrom (result) : Result<'a, DbExecutionError> = result

    member _.Delay(fn) : unit -> Result<'a, DbExecutionError> = fn

    member _.Run(fn) : Result<'a, DbExecutionError> = fn ()

    member _.Bind (result, binder) = Result.bind binder result

    member x.Zero () = x.Return ()

    member x.TryWith (result, exceptionHandler) =
        try x.ReturnFrom (result)
        with ex -> exceptionHandler ex

    member x.TryFinally (result, fn) =
        try x.ReturnFrom (result)
        finally fn ()

    member x.Using (disposable : #IDisposable, fn) =
        x.TryFinally(fn disposable, fun _ ->
            match disposable with
            | null -> ()
            | disposable -> disposable.Dispose())

    member x.While (guard,  fn) =
        if not (guard())
            then x.Zero ()
        else
            do fn () |> ignore
            x.While(guard, fn)

    member x.For (items : seq<_>, fn) =
        x.Using(items.GetEnumerator(), fun enum ->
            x.While(enum.MoveNext,
                x.Delay (fun () -> fn enum.Current)))

    member x.Combine (result, fn) =
        x.Bind(result, fun () -> fn ())

/// Computation expression for Result<_, DbExecutionError>.
let dbResult = DbResultBuilder()

// dbResultTask {...}
// ------------

type DbResultTaskBuilder() =
    member _.Return (value) : Task<Result<'a, DbExecutionError>> = value |> Ok |> Task.FromResult

    member _.ReturnFrom (result) : Task<Result<'a, DbExecutionError>> = result

    member _.Delay(fn) : unit -> Task<Result<'a, DbExecutionError>> = fn

    member _.Run(fn) : Task<Result<'a, DbExecutionError>> = fn ()

    member _.Bind (result : Task<Result<'a, DbExecutionError>>, binder : 'a -> Task<Result<'b, DbExecutionError>>) = 
        task {
            let! result = result            
            match result with 
            | Error e  -> return Error e            
            | Ok value -> 
                let! bound = binder value
                return bound
        }

    member x.Zero () = x.Return ()

    member x.TryWith (result, exceptionHandler) =
        try x.ReturnFrom (result)
        with ex -> exceptionHandler ex

    member x.TryFinally (result, fn) =
        try x.ReturnFrom (result)
        finally fn ()

    member x.Using (disposable : #IDisposable, fn) =
        x.TryFinally(fn disposable, fun _ ->
            match disposable with
            | null -> ()
            | disposable -> disposable.Dispose())

    member x.While (guard,  fn) =
        if not (guard())
            then x.Zero ()
        else
            do fn () |> ignore
            x.While(guard, fn)

    member x.For (items : seq<_>, fn) =
        x.Using(items.GetEnumerator(), fun enum ->
            x.While(enum.MoveNext,
                x.Delay (fun () -> fn enum.Current)))

    member x.Combine (result, fn) =
        x.Bind(result, fun () -> fn ())


/// Computation expression for Task<Result<_, DbExecutionError>>.
let dbResultTask = DbResultTaskBuilder()

// dbCommand {...}
// ------------

type CommandSpec<'a> =
    {
        Connection     : IDbConnection
        Transaction    : IDbTransaction option
        CommandType    : CommandType
        CommandTimeout : int option
        Statement      : string
        Param          : RawDbParams
    }
    static member Create (conn : IDbConnection) =
        {
            Connection     = conn
            Transaction    = None
            CommandType    = CommandType.Text
            CommandTimeout = None
            Statement      = ""
            Param          = []
        }

/// Computation expression for generating IDbCommand instances.
type DbCommandBuilder<'a>(conn : IDbConnection) =
    member _.Yield(_) = CommandSpec<'a>.Create (conn)

    member _.Run(spec : CommandSpec<'a>) =
        let cmd =
            spec.Connection
            |> Db.newCommand spec.Statement
            |> Db.setCommandType spec.CommandType
            |> Db.setParams spec.Param

        match spec.Transaction, spec.CommandTimeout with
        | Some tran, Some timeout -> cmd |> Db.setTimeout timeout |> Db.setTransaction tran
        | Some tran, None         -> Db.setTransaction tran cmd
        | None, Some timeout      -> Db.setTimeout timeout cmd
        | None, None              -> cmd

    [<CustomOperation("cmdParam")>]
    /// Add DbParams.
    member _.DbParams (spec : CommandSpec<'a>, param : RawDbParams) =
        { spec with Param = param }

    [<CustomOperation("cmdText")>]
    /// Set statement text.
    member _.Statement (spec : CommandSpec<'a>, statement : string) =
        { spec with Statement = statement }

    [<CustomOperation("cmdTran")>]
    /// Set transaction.
    member _.Transaction (spec : CommandSpec<'a>, tran : IDbTransaction) =
        { spec with Transaction = Some tran }

    [<CustomOperation("cmdType")>]
    /// Set command type (default: CommandType.Text).
    member _.CommandType (spec : CommandSpec<'a>, commandType : CommandType) =
        { spec with CommandType = commandType }

    [<CustomOperation("cmdTimeout")>]
    /// Set command timeout.
    member _.CommandTimeout (spec : CommandSpec<'a>, timeout : TimeSpan) =
        { spec with CommandTimeout = Some <| int timeout.TotalSeconds }

/// Computation expression for generating IDbCommand instances.
let dbCommand<'a> (conn : IDbConnection) = DbCommandBuilder<'a>(conn)