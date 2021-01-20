[<AutoOpen>]
module Donald.Conection

open System.Data 

type IDbConnection with
    member internal this.NewCommand(commandType : CommandType, sql : string, commandTimeout : int option) =
        let cmd = this.CreateCommand()
        cmd.CommandType <- commandType
        cmd.CommandText <- sql
        commandTimeout |> Option.iter (fun timeout -> cmd.CommandTimeout <- timeout)
        cmd

    member internal this.TryOpenConnection()  =        
        try
            if this.State = ConnectionState.Closed then 
                this.Open()             
        with ex -> 
            raise (CouldNotOpenConnectionError ex) 

    member this.TryBeginTransaction()  =        
        try
            this.TryOpenConnection()
            this.BeginTransaction()
        with         
        | ex -> raise (CouldNotBeginTransactionError ex)
