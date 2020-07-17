[<AutoOpen>]
module Donald.Query

open System.Data
open System.Data.Common
open FSharp.Control.Tasks

/// Query for multiple results within transaction scope
let tranQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    use rd = cmd.ExecuteReader()
    let results = [ while rd.Read() do yield map rd ]
    rd.Close() |> ignore
    results

/// Try to query for multiple results within transaction scope
let tryTranQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    try
        tranQuery sql param map tran
        |> DbResult 
    with ex -> DbError ex

/// Query async for multiple results within transaction scope
let tranQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand
        use! rd = cmd.ExecuteReaderAsync()        

        let rec loopAsync (acc : 'a list) (rd : DbDataReader) =
            task {
                let! canRead = rd.ReadAsync()
                match canRead with
                | false -> return acc
                | true  -> 
                    let result = map rd
                    let results = result :: acc
                    return! loopAsync results rd
            }

        let! results = loopAsync [] rd
        rd.Close() |> ignore
        return results
    }

/// Try to query for multiple results within transaction scope
let tryTranQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (tran : IDbTransaction) =
    task {
        try
            let! result = tranQueryAsync sql param map tran
            return DbResult result
        with ex -> return DbError ex
    }

/// Query for multiple results
let query (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let results = tranQuery sql param map tran
    commitTran tran
    results

/// Query for multiple results
let tryQuery (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =   
    try
        use tran = beginTran conn
        let results = tranQuery sql param map tran
        commitTran tran
        DbResult results
    with ex -> DbError ex
    
/// Query async for multiple results
let queryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        let! results = tranQueryAsync sql param map tran
        commitTran tran
        return results
    }

/// Query async for multiple results
let tryQueryAsync (sql : string) (param : DbParam list) (map : IDataReader -> 'a) (conn : IDbConnection) =   
    task {
        try
            use tran = beginTran conn
            let! results = tranQueryAsync sql param map tran
            commitTran tran
            return DbResult results
        with ex -> return DbError ex
    }
