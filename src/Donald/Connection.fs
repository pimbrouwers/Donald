[<AutoOpen>]
module Donald.Connection

open System.Data

/// Create new instance of IDbConnection using provided DbConnectionFactory
let createConn (createConnection : DbConnectionFactory) =
    createConnection ()    
  
/// Create a new IDbTransaction
let beginTran (conn : IDbConnection) = 
    if conn.State <> ConnectionState.Open then conn.Open()
    conn.BeginTransaction()

/// Rollback IDbTransaction
let rollbackTran (tran : IDbTransaction) =
    try        
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Rollback()
    with            
        | _ -> 
            reraise() 

/// Attempt to commit IDbTransaction, rollback if failed.
let commitTran (tran : IDbTransaction) =
    try
        if not(isNull tran) 
           && not(isNull tran.Connection) then tran.Commit() 
    with
        /// Is supposed to throw System.InvalidOperationException
        /// when commmited or rolled back already, but most
        /// implementations do not. So in all cases try rolling back
        | _ -> 
            rollbackTran tran
            reraise()