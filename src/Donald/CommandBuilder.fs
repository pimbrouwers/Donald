[<AutoOpen>]
module Donald.CommandBuilder

open System.Data

type CommandSpec<'a> = 
    {
        Connection  : IDbConnection
        Transaction : IDbTransaction option
        CommandType : CommandType
        Statement   : string 
        Param       : RawDbParams
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
        let param = DbParams.create spec.Param
        match spec.Transaction with 
        | Some tran -> 
            tran.NewCommand(spec.CommandType, spec.Statement)
                .SetDbParams(param)
        
        | None ->
            spec.Connection
                .NewCommand(spec.CommandType, spec.Statement)
                .SetDbParams(param)

    [<CustomOperation("cmdParam")>]
    member _.DbParams (spec : CommandSpec<'a>, param : RawDbParams) =
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