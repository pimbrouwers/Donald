[<RequireQualifiedAccess>]
module Donald.Db

open System.Data

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
    open FSharp.Control.Tasks.V2.ContextInsensitive

    let private tryDoAsync (fn : DbCommand -> Task<'a>) (cmd : IDbCommand) : Task<DbResult<'a>> = task {
        try
            cmd.Connection.TryOpenConnection() |> ignore                
            let! result = fn (cmd :?> DbCommand)        
            cmd.Dispose()
            return (Ok result)
        with 
        | FailedExecutionError e -> return Error e
    }

    /// Asynchronously execute parameterized query with no results.
    let exec (cmd : IDbCommand) : Task<DbResult<unit>> = 
        let inner = fun (cmd : DbCommand) -> task {        
            let! _ = cmd.ExecAsync()
            return ()
        }
        tryDoAsync inner cmd

    /// Asynchronously execute parameterized query many times with no results
    let execMany (param : RawDbParams list) (cmd : IDbCommand) : Task<DbResult<unit>> =         
        let inner = fun (cmd : DbCommand) -> task {                    
            for p in param do
                let dbParams = DbParams.create p
                let! _ = cmd.SetDbParams(dbParams).ExecAsync()            
                ()

            return ()            
        }
        tryDoAsync inner cmd

    /// Execute scalar query and box the result.
    let scalar (convert : obj -> 'a) (cmd : IDbCommand) : Task<DbResult<'a>> =
        let inner = fun (cmd : DbCommand) -> task {        
            let! value = cmd.ExecuteScalarAsync()
            return convert value
        }
        tryDoAsync inner cmd

    /// Asynchronously execute parameterized query, enumerate all records and apply mapping.
    let query (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a list>> = 
        let inner = fun (cmd : DbCommand) -> task {        
            use! rd = cmd.ExecReaderAsync()            
            let results = [ while rd.Read() do map rd ]
            rd.Close() |> ignore
            return results
        }
        tryDoAsync inner cmd
    
    /// Asynchronously execute paramterized query, read only first record and apply mapping.
    let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a option>> =          
        let inner = fun (cmd : DbCommand) -> task {        
            use! rd = cmd.ExecReaderAsync() 
            let result = if rd.Read() then Some(map rd) else None
            rd.Close() |> ignore
            return result
        }
        tryDoAsync inner cmd            