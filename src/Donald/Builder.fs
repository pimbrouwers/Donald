namespace Donald

open System
open System.Data
open System.Threading

type CommandSpec<'a> = 
    {
        Connection : IDbConnection
        Transaction : IDbTransaction option
        CommandType : CommandType
        CommandTimeout : int
        CommandBehavior : CommandBehavior
        Statement : string 
        Param : RawDbParams
        CancellationToken : CancellationToken
    }
    static member Default (conn : IDbConnection) = 
        {
            Connection = conn
            Transaction = None
            CommandType = CommandType.Text
            CommandTimeout = 30
            CommandBehavior = CommandBehavior.SequentialAccess
            Statement = ""
            Param = []
            CancellationToken = CancellationToken.None
        }

/// Computation expression for generating IDbCommand instances.
type DbCommandBuilder<'a> (conn : IDbConnection) =
    member _.Yield(_) = CommandSpec<'a>.Default (conn)

    member _.Run(spec : CommandSpec<'a>) =         
        let cmd = 
            spec.Connection
            |> Db.newCommand spec.Statement
            |> Db.setCommandType spec.CommandType
            |> Db.setCommandBehavior spec.CommandBehavior
            |> Db.setParams spec.Param
            |> Db.setTimeout spec.CommandTimeout
            |> Db.setCancellationToken spec.CancellationToken
            
        match spec.Transaction with         
        | Some tran -> Db.setTransaction tran cmd        
        | None -> cmd
        
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
        { spec with CommandTimeout = int timeout.TotalSeconds }

    [<CustomOperation("cmdBehavior")>]
    /// Set command behavior.
    member _.CommandBehavior (spec : CommandSpec<'a>, commandBehavior : CommandBehavior) =
        { spec with CommandBehavior = commandBehavior }

    [<CustomOperation("cmdCancel")>]
    /// Set CancellationToken.
    member _.CancellationToken (spec : CommandSpec<'a>, cancellationToken : CancellationToken) =
        { spec with CancellationToken = cancellationToken }

[<AutoOpen>]
module DbCommandBuilder =
    /// Computation expression for generating IDbCommand instances.
    let dbCommand<'a> (conn : IDbConnection) = DbCommandBuilder<'a>(conn)