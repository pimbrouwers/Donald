[<RequireQualifiedAccess>]
module Donald.DbTran

open System.Data

let private tryDo (fn : IDbCommand -> 'a) (cmd : IDbCommand) : DbResult<'a> =
    try
        let result = fn cmd
        Ok result
    with 
    | FailedExecutionError e -> Error e

let exec (cmd : IDbCommand) : DbResult<unit> =
    tryDo (fun cmd -> cmd.Exec()) cmd

let execMany (param : DbParams list) (cmd : IDbCommand) : DbResult<unit> =    
    try 
        for p in param do
            cmd.SetDbParams(p).Exec() |> ignore

        Ok ()
    with 
    | FailedExecutionError e -> Error e
    
let scalar (cmd : IDbCommand) : DbResult<obj> =
    tryDo (fun cmd -> cmd.ExecScalar()) cmd

let query (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a list> =   
    tryDo 
        (fun cmd ->     
            use rd = cmd.ExecReader()
            let results = [ while rd.Read() do yield map rd ]
            rd.Close() |> ignore
            results) 
        cmd

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

    let exec (cmd : DbCommand) : Task<DbResult<unit>> = task {        
        try
            let! _ = cmd.ExecAsync()
            return Ok ()
        with 
        | FailedExecutionError e -> return Error e
    }

    let execMany (param : DbParams list) (cmd : DbCommand) : Task<DbResult<unit>> = task {        
        try 
            for p in param do
                let! _ = cmd.SetDbParams(p).ExecAsync()            
                ()

            return Ok ()
        with 
        | FailedExecutionError e -> return Error e
    }

    let scalar (cmd : DbCommand) : Task<DbResult<obj>> = task {        
        try
            let! result = cmd.ExecScalarAsync()
            return Ok result
        with 
        | FailedExecutionError e -> return Error e
    }

    let query (map : IDataReader -> 'a) (cmd : DbCommand) : Task<DbResult<'a list>> = task {        
        try
            use! rd = cmd.ExecReaderAsync()            
            let results = [ while rd.Read() do map rd ]
            rd.Close() |> ignore
            return Ok results
        with 
        | FailedExecutionError e -> return Error e
    }
    
    let querySingle (map : IDataReader -> 'a) (cmd : DbCommand)  : Task<DbResult<'a option>> = task {          
        try
            use! rd = cmd.ExecReaderAsync() 
            let result = if rd.Read() then Some(map rd) else None
            rd.Close() |> ignore
            return Ok result
        with 
        | FailedExecutionError e -> return Error e
    }
        