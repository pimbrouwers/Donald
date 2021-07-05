[<AutoOpen>]
module Donald.DbCommandBuilder

open System
open System.Data

/// Computation expression for DbResult<_>.
type DbResultBuilder() =
    member _.Return (value) : DbResult<'a> = Ok value

    member _.ReturnFrom (result) : DbResult<'a> = result

    member _.Zero () : DbResult<unit> = Ok ()

    member _.Delay(fn) : unit -> DbResult<'a> = fn

    member _.Run(fn) : DbResult<'a> = fn ()

    member _.Bind (result, binder) = DbResult.bind binder result

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

/// Computation expression for DbResult<_>.
let dbResult = DbResultBuilder()

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
        let param = DbParams.create spec.Param
        match spec.Transaction with 
        | Some tran -> 
            tran.NewCommand(spec.CommandType, spec.Statement, spec.CommandTimeout)
                .SetDbParams(param)
        
        | None ->
            spec.Connection
                .NewCommand(spec.CommandType, spec.Statement, spec.CommandTimeout)
                .SetDbParams(param)

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