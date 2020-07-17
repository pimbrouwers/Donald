[<AutoOpen>]
module Donald.Scalar

open System.Data
open System.Data.Common
open FSharp.Control.Tasks

/// Execute query that returns scalar result within transcation scope
let tranScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    use cmd = newCommand sql param tran
    convert (cmd.ExecuteScalar())

/// Try to execute query that returns scalar result within transcation scope
let tryTranScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    try
        tranScalar sql param convert tran
        |> DbResult
    with ex -> DbError ex

/// Execute query async that returns scalar result within transcation scope
let tranScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    task {
        use cmd = newCommand sql param tran :?> DbCommand
        let! result = cmd.ExecuteScalarAsync()
        return convert (result)
    }

/// Try to execute query async that returns scalar result within transcation scope
let tryTranScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (tran : IDbTransaction) =
    task {
        try
            let! result = tranScalarAsync sql param convert tran
            return DbResult result
        with ex -> return DbError ex
    }

/// Execute query with scalar result
let scalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    use tran = beginTran conn
    let v = tranScalar sql param convert tran
    commitTran tran
    v

/// Try to execute query async with scalar result
let tryScalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    task {
        try
            use tran = beginTran conn
            let! result = tranScalarAsync sql param convert tran
            commitTran tran
            return DbResult result
        with ex -> return DbError ex
    }   

/// Execute query async with scalar result
let scalarAsync (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    task {
        use tran = beginTran conn
        let! v = tranScalarAsync sql param convert tran
        commitTran tran
        return v
    }

/// Try to execute query with scalar result
let tryScalar (sql : string) (param : DbParam list) (convert : obj -> 'a) (conn : IDbConnection) =
    try
        use tran = beginTran conn
        let result = tranScalar sql param convert tran
        commitTran tran
        DbResult result
    with ex -> DbError ex    