[<RequireQualifiedAccess>]
module Donald.DbConn

open System.Data

let private tryTransact (fn : IDbCommand -> DbResult<'a>) (cmd : IDbCommand) =     
    use tran = cmd.Connection.TryBeginTransaction()
    cmd.Transaction <- tran
    let result = fn cmd
    tran.TryCommit()
    cmd.Dispose()
    result

let exec (cmd : IDbCommand) : DbResult<unit> =
    tryTransact (DbTran.exec) cmd

let execMany (param : DbParams list) (cmd : IDbCommand) : DbResult<unit> =
    tryTransact (DbTran.execMany param) cmd

let scalar (cmd : IDbCommand) : DbResult<obj> =
    tryTransact (DbTran.scalar) cmd

let query (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a list> =    
    tryTransact (DbTran.query map) cmd

let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : DbResult<'a option> =
    tryTransact (DbTran.querySingle map) cmd

module Async =
    open System.Data.Common
    open System.Threading.Tasks
    open FSharp.Control.Tasks.V2.ContextInsensitive
    
    let private tryTransactAsync (fn : DbCommand -> Task<DbResult<'a>>) (cmd : IDbCommand) : Task<DbResult<'a>> = task {
        use tran = cmd.Connection.TryBeginTransaction()
        cmd.Transaction <- tran :?> DbTransaction        
        let! result = fn (cmd :?> DbCommand)
        tran.TryCommit()
        cmd.Dispose()
        return result
    }

    let exec (cmd : IDbCommand) : Task<DbResult<unit>> = 
        tryTransactAsync (DbTran.Async.exec) cmd

    let execMany (param : DbParams list) (cmd : IDbCommand) : Task<DbResult<unit>> =
        tryTransactAsync (DbTran.Async.execMany param) cmd

    let scalar (cmd : IDbCommand) : Task<DbResult<obj>> = 
        tryTransactAsync (DbTran.Async.scalar) cmd

    let query (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a list>> =
        tryTransactAsync (DbTran.Async.query map) cmd
        
    let querySingle (map : IDataReader -> 'a) (cmd : IDbCommand) : Task<DbResult<'a option>> =
        tryTransactAsync (DbTran.Async.querySingle map) cmd