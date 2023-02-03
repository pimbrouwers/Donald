namespace Donald

open System.Data
open System.Data.Common
open System.Threading

[<AutoOpen>]
module IDbConnectionExtensions =
    type IDbConnection with
        /// Safely attempt to open a new IDbTransaction or
        /// return FailedOpenConnectionException.
        member x.TryOpenConnection() =
            try
                if x.State = ConnectionState.Closed then
                    x.Open()
            with ex ->
                raise (DbConnectionException(x, ex))

        /// Safely attempt to open a new IDbTransaction or
        /// return FailedOpenConnectionException.
        member x.TryOpenConnectionAsync(?cancellationToken : CancellationToken)  = task {
            try
                let ct = defaultArg cancellationToken CancellationToken.None
                if x.State = ConnectionState.Closed then
                    match x with
                    | :? DbConnection as c -> do! c.OpenAsync(ct)
                    | _ ->
                        ct.ThrowIfCancellationRequested()
                        x.Open()
            with ex ->
                return raise (DbConnectionException(x, ex))
        }

        /// Safely attempt to create a new IDbTransaction or
        /// return CouldNotBeginTransactionException.
        member x.TryBeginTransaction()  =
            try
                x.TryOpenConnection()
                x.BeginTransaction()
            with ex ->
                raise (DbExecutionException(TxBegin, ex))

        /// Safely attempt to create a new IDbTransaction or
        /// return CouldNotBeginTransactionException.
        member x.TryBeginTransactionAsync(?cancellationToken : CancellationToken)  = task {
            try
                let ct = defaultArg cancellationToken CancellationToken.None
                do! x.TryOpenConnectionAsync(ct)
                match x with
                | :? DbConnection as c ->
                    let! dbTransaction = c.BeginTransactionAsync(ct)
                    return dbTransaction :> IDbTransaction
                | _ ->
                    ct.ThrowIfCancellationRequested()
                    return x.BeginTransaction()
            with ex ->
                return raise (DbExecutionException(TxBegin, ex))
        }
