[<AutoOpen>]
module Donald.CommandBuilder

open System.Data

type CommandSpec<'a> = 
    {
        Connection  : IDbConnection
        Transaction : IDbTransaction option
        CommandType : CommandType
        Statement   : string 
        Param       : DbParams
    }
    static member Create (conn : IDbConnection) = 
        {
            Connection  = conn
            Transaction = None
            CommandType = CommandType.Text
            Statement   = ""
            Param      = []
        }

type CommandBuilder<'a>(conn : IDbConnection) =
    member _.Yield(_) = CommandSpec<'a>.Create (conn)

    member _.Run(spec : CommandSpec<'a>) = 
        match spec.Transaction with 
        | Some tran -> 
            tran.NewCommand(spec.CommandType, spec.Statement)
                .SetDbParams(spec.Param)
        
        | None ->
            spec.Connection
                   .NewCommand(spec.CommandType, spec.Statement)
                   .SetDbParams(spec.Param)

    [<CustomOperation("cmdParam")>]
    member _.DbParams (spec : CommandSpec<'a>, param : DbParams ) =
        { spec with Param = param }

    [<CustomOperation("cmdText")>]
    member _.Statement (spec : CommandSpec<'a>, statement : string) =
        { spec with Statement = statement }

    [<CustomOperation("cmdTran")>]
    member _.Transaction (spec : CommandSpec<'a>, tran : IDbTransaction) =
        { spec with Transaction = Some tran }
    
    [<CustomOperation("cmdType")>]
    member _.CommandType (spec : CommandSpec<'a>, commandType : CommandType) =
        { spec with CommandType = commandType }

let dbCommand<'a> (conn : IDbConnection) = CommandBuilder<'a>(conn)