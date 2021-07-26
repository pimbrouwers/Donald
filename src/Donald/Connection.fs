[<AutoOpen>]
module Donald.Conection

open System.Data 

type IDbConnection with
    member internal this.TryOpenConnection()  =        
        try
            if this.State = ConnectionState.Closed then 
                this.Open()             
        with ex -> 
            raise (CouldNotOpenConnectionError ex) 

    /// Safely attempt to create a new IDbTransaction.
    member this.TryBeginTransaction()  =        
        try
            this.TryOpenConnection()
            this.BeginTransaction()
        with         
        | ex -> raise (CouldNotBeginTransactionError ex)
