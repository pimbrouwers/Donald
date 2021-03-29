[<RequireQualifiedAccess>]
module Donald.DbConn

open System.Data

let private tryDo (fn : IDbCommand -> DbResult<'a>) (cmd : IDbCommand) =     
    cmd.Connection.TryOpenConnection() |> ignore    
    let result = fn cmd    
    cmd.Dispose()
    result

let exec (cmd : IDbCommand) : DbResult<unit> =
    tryDo (DbTran.exec) cmd

let execMany (param : RawDbParams list) (cmd : IDbCommand) : DbResult<unit> =
    tryDo (DbTran.execMany param) cmd

let query (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a list> =    
    tryDo (DbTran.query map) cmd

let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a option> =
    tryDo (DbTran.querySingle map) cmd

module Async =
    open System.Data.Common
    open System.Threading.Tasks
    open FSharp.Control.Tasks.V2.ContextInsensitive
    
    let private tryDoAsync (fn : DbCommand -> Task<DbResult<'a>>) (cmd : IDbCommand) : Task<DbResult<'a>> = task {
        cmd.Connection.TryOpenConnection() |> ignore             
        let! result = fn (cmd :?> DbCommand)        
        cmd.Dispose()
        return result
    }

    let exec (cmd : IDbCommand) : Task<DbResult<unit>> = 
        tryDoAsync (DbTran.Async.exec) cmd

    let execMany (param : RawDbParams list) (cmd : IDbCommand) : Task<DbResult<unit>> =
        tryDoAsync (DbTran.Async.execMany param) cmd

    let query (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a list>> =
        tryDoAsync (DbTran.Async.query map) cmd
        
    let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a option>> =
        tryDoAsync (DbTran.Async.querySingle map) cmd