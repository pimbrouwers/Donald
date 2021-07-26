[<AutoOpen>]
module Donald.Transaction

open System.Data

type IDbTransaction with
    /// Safely attempt to rollback an IDbTransaction.
    member this.TryRollback() =
        try        
            if not(isNull this) 
               && not(isNull this.Connection) then this.Rollback()
        with ex  -> 
            raise (CouldNotRollbackTransactionError ex) 

    /// Safely attempt to commit an IDbTransaction.
    /// Will rollback in the case of Exception.
    member this.TryCommit() =
        try
            if not(isNull this) 
               && not(isNull this.Connection) then this.Commit() 
        with ex -> 
            /// Is supposed to throw System.InvalidOperationException
            /// when commmited or rolled back already, but most
            /// implementations do not. So in all cases try rolling back
            this.TryRollback()
            raise (CouldNotCommitTransactionError ex)             