[<AutoOpen>]
module Donald.Conection

open System.Data 

type IDbConnection with
    /// Safely attempt to open a new IDbTransaction or
    /// return CouldNotOpenConnectionError.
    member inline this.TryOpenConnection()  =        
        try
            if this.State = ConnectionState.Closed then 
                this.Open()             
        with ex -> 
            let error = 
                { ConnectionString = this.ConnectionString 
                  Error = ex }
            raise (CouldNotOpenConnectionError error) 

    /// Safely attempt to create a new IDbTransaction or
    /// return CouldNotBeginTransactionError.
    member inline this.TryBeginTransaction()  =        
        try
            this.TryOpenConnection()
            this.BeginTransaction()
        with         
        | ex -> raise (CouldNotBeginTransactionError ex)
