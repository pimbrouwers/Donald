[<AutoOpen>]
module Donald.Conection

open System.Data 

type IDbConnection with
    /// Safely attempt to open a new IDbTransaction or
    /// return CouldNotOpenConnectionException.
    member this.TryOpenConnection()  =        
        try
            if this.State = ConnectionState.Closed then 
                this.Open()             
        with ex -> 
            let error = 
                { ConnectionString = this.ConnectionString 
                  Error = ex }
            raise (CouldNotOpenConnectionException error) 

    /// Safely attempt to create a new IDbTransaction or
    /// return CouldNotBeginTransactionException.
    member this.TryBeginTransaction()  =        
        try
            this.TryOpenConnection()
            this.BeginTransaction()
        with         
        | ex -> raise (CouldNotBeginTransactionException ex)
