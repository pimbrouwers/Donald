namespace Donald

open System.Data
open System.Data.Common
open System.Threading

[<AutoOpen>]
module IDbTransactionExtensions =
    type IDbTransaction with
        /// Safely attempt to rollback an IDbTransaction.
        member x.TryRollback() =
            try
                if not(isNull x) && not(isNull x.Connection) then x.Rollback()
            with ex  ->
                raise (DbTransactionException(TxRollback, ex))

        /// Safely attempt to rollback an IDbTransaction.
        member x.TryRollbackAsync(?cancellationToken : CancellationToken) = task {
            try
                if not(isNull x) && not(isNull x.Connection) then
                    let ct = defaultArg cancellationToken CancellationToken.None
                    match x with
                    | :? DbTransaction as t-> do! t.RollbackAsync(ct)
                    | _ ->
                        ct.ThrowIfCancellationRequested()
                        x.Rollback()
            with ex  ->
                return raise (DbTransactionException(TxRollback, ex))
        }

        /// Safely attempt to commit an IDbTransaction.
        /// Will rollback in the case of Exception.
        member x.TryCommit() =
            try
                if not(isNull x) && not(isNull x.Connection) then x.Commit()
            with ex ->
                // Is supposed to throw System.InvalidOperationException
                // when commmited or rolled back already, but most
                // implementations do not. So in all cases try rolling back
                x.TryRollback()
                raise (DbTransactionException(TxCommit, ex))

        /// Safely attempt to commit an IDbTransaction.
        /// Will rollback in the case of Exception.
        member x.TryCommitAsync(?cancellationToken : CancellationToken) = task {
            let ct = defaultArg cancellationToken CancellationToken.None
            try
                if not(isNull x) && not(isNull x.Connection) then

                    match x with
                    | :? DbTransaction as t -> do! t.CommitAsync(ct)
                    | _ ->
                        ct.ThrowIfCancellationRequested()
                        x.Commit()
            with ex ->
                // Is supposed to throw System.InvalidOperationException
                // when commmited or rolled back already, but most
                // implementations do not. So in all cases try rolling back
                do! x.TryRollbackAsync(ct)
                return raise (DbTransactionException(TxCommit, ex))
        }
