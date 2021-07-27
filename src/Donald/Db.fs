[<RequireQualifiedAccess>]
module Donald.Db

open System.Data

/// Create a new IDbCommand instance using the provided IDbConnection.
let newCommand (commandText : string) (conn : IDbConnection) =
    let cmd = conn.CreateCommand()
    cmd.CommandText <- commandText    
    cmd

/// Configure the CommandType for the provided IDbCommand
let setCommandType (commandType : CommandType) (cmd : IDbCommand) = 
    cmd.CommandType <- commandType
    cmd 

/// Configure the command parameters for the provided IDbCommand
let setParams (param : RawDbParams) (cmd : IDbCommand) =    
    cmd.SetDbParams(DbParams.create param)

/// Configure the timeout for the provided IDbCommand
let setTimeout (commandTimeout : int) (cmd : IDbCommand) =
    cmd.CommandTimeout <- commandTimeout
    cmd

/// Configure the transaction for the provided IDbCommand
let setTransaction (tran : IDbTransaction) (cmd : IDbCommand) =
    cmd.Transaction <- tran
    cmd

let private tryDo (fn : IDbCommand -> 'a) (cmd : IDbCommand) : DbResult<'a> =
    try
        cmd.Connection.TryOpenConnection() |> ignore    
        let result = fn cmd
        cmd.Dispose()
        Ok result
    with 
    | FailedExecutionError e -> Error e

/// Execute parameterized query with no results.
let exec (cmd : IDbCommand) : DbResult<unit> =
    tryDo (fun cmd -> cmd.Exec()) cmd

/// Execute parameterized query many times with no results.
let execMany (param : RawDbParams list) (cmd : IDbCommand) : DbResult<unit> =    
    try 
        for p in param do
            let dbParams = DbParams.create p
            cmd.SetDbParams(dbParams).Exec() |> ignore

        Ok ()
    with 
    | FailedExecutionError e -> Error e

/// Execute scalar query and box the result.
let scalar (convert : obj -> 'a) (cmd : IDbCommand) : DbResult<'a> =
    tryDo (fun cmd -> 
        let value = cmd.ExecuteScalar()
        convert value)
        cmd


/// Execute parameterized query, enumerate all records and apply mapping.
let query (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a list> =   
    tryDo (fun cmd ->     
        use rd = cmd.ExecReader()
        let results = [ while rd.Read() do yield map rd ]
        rd.Close() |> ignore
        results) 
        cmd

/// Execute paramterized query, read only first record and apply mapping.
let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a option> =    
    tryDo 
        (fun cmd ->     
            use rd = cmd.ExecReader()
            let result = if rd.Read() then Some(map rd) else None
            rd.Close() |> ignore
            result) 
        cmd    

module Async =
    open System.Data.Common
    open System.Threading.Tasks
    //open FSharp.Control.Tasks.V2.ContextInsensitive

    let private tryDoAsync (fn : DbCommand -> Task<'a>) (cmd : IDbCommand) : DbResultTask<'a> = 
        try
            let continuation (resultTask : Task<'a>) = 
                cmd.Dispose()
                Ok resultTask.Result

            cmd.Connection.TryOpenConnection() |> ignore                
            fn (cmd :?> DbCommand)                    
            |> continueWith continuation
        with 
        | FailedExecutionError e -> Error e |> Task.FromResult
    
    /// Asynchronously execute parameterized query with no results.
    let exec (cmd : IDbCommand) : DbResultTask<unit> = 
        let inner = fun (cmd : DbCommand) -> cmd.ExecAsync() |> continueWith (fun _ -> ())            
        tryDoAsync inner cmd

    /// Asynchronously execute parameterized query many times with no results
    let execMany (param : RawDbParams list) (cmd : IDbCommand) : DbResultTask<unit> =         
        let inner = fun (cmd : DbCommand) -> 
            let tasks = [| for p in param do cmd.SetDbParams(DbParams.create p).ExecAsync() |]
            Task.WhenAll(tasks) 
            |> continueWith (fun _ -> ())
        
        tryDoAsync inner cmd

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (cmd : IDbCommand) : DbResultTask<'a> =
        let inner = fun (cmd : DbCommand) ->      
            cmd.ExecuteScalarAsync() 
            |> continueWith (fun valueResult -> convert valueResult.Result)

        tryDoAsync inner cmd

    /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
    let query (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResultTask<'a list> = 
        let inner = fun (cmd : DbCommand) ->
            let continuation (readerTask : Task<DbDataReader>) = 
                use rd = readerTask.Result
                let results = [ while rd.Read() do map rd ]
                rd.Close() |> ignore
                results
            
            cmd.ExecReaderAsync()            
            |> continueWith continuation 
            
        tryDoAsync inner cmd
    
    /// Asynchronously execute paramterized query, read only first record and apply mapping.
    let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResultTask<'a option> =          
        let inner = fun (cmd : DbCommand) -> 
            let continuation (readerTask : Task<DbDataReader>) =
                use rd = readerTask.Result
                let result = if rd.Read() then Some(map rd) else None
                rd.Close() |> ignore
                result

            cmd.ExecReaderAsync() 
            |> continueWith continuation
        
        tryDoAsync inner cmd            