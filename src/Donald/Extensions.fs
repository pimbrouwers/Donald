namespace Donald

open System
open System.Data
open System.Data.Common
open System.IO
open System.Threading.Tasks
open System.Threading

#if NETSTANDARD2_0 || NETSTANDARD2_1
    open FSharp.Control.Tasks
#endif

[<AutoOpen>]
module Extensions =
    type IDbConnection with
        /// Safely attempt to open a new IDbTransaction or
        /// return FailedOpenConnectionException.
        member x.TryOpenConnection()  =
            try
                if x.State = ConnectionState.Closed then
                    x.Open()
            with ex ->
                let error = DbConnectionError {
                    ConnectionString = x.ConnectionString
                    Error = ex }

                raise (DbFailureException error)

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
                let error = DbConnectionError {
                    ConnectionString = x.ConnectionString
                    Error = ex }

                return raise (DbFailureException error)
        }

        /// Safely attempt to create a new IDbTransaction or
        /// return CouldNotBeginTransactionException.
        member x.TryBeginTransaction()  =
            try
                x.TryOpenConnection()
                x.BeginTransaction()
            with
            | ex ->
                let error = DbTransactionError {
                    Step = TxBegin
                    Error = ex }

                raise (DbFailureException error)


#if !NETSTANDARD2_0
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
            with
            | ex ->
                let error = DbTransactionError {
                    Step = TxBegin
                    Error = ex }

                return raise (DbFailureException error)
        }
#endif

    type IDbTransaction with
        /// Safely attempt to rollback an IDbTransaction.
        member x.TryRollback() =
            try
                if not(isNull x) && not(isNull x.Connection) then x.Rollback()
            with ex  ->
                let error = DbTransactionError {
                    Step = TxRollback
                    Error = ex }

                raise (DbFailureException error)


#if !NETSTANDARD2_0
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
                let error = DbTransactionError {
                    Step = TxRollback
                    Error = ex }

                return raise (DbFailureException error)
        }
#endif
        /// Safely attempt to commit an IDbTransaction.
        /// Will rollback in the case of Exception.
        member x.TryCommit() =
            try
                if not(isNull x) && not(isNull x.Connection) then x.Commit()
            with ex ->
                /// Is supposed to throw System.InvalidOperationException
                /// when commmited or rolled back already, but most
                /// implementations do not. So in all cases try rolling back
                x.TryRollback()

                let error = DbTransactionError {
                    Step = TxCommit
                    Error = ex }

                raise (DbFailureException error)


#if !NETSTANDARD2_0
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
                /// Is supposed to throw System.InvalidOperationException
                /// when commmited or rolled back already, but most
                /// implementations do not. So in all cases try rolling back
                do! x.TryRollbackAsync(ct)

                let error = DbTransactionError {
                    Step = TxCommit
                    Error = ex }

                raise (DbFailureException error)
        }
#endif

    type IDbCommand with
        member internal x.SetDbParams(dbParams : DbParams) =
            let setParamValue (p : IDbDataParameter) (v : obj) =
                if isNull v then p.Value <- DBNull.Value
                else p.Value <- v

            x.Parameters.Clear() // clear to ensure a clean working set

            for param in dbParams do
                let p = x.CreateParameter()
                p.ParameterName <- param.Name

                match param.Value with
                | SqlType.Null ->
                    p.Value <- DBNull.Value

                | SqlType.String v ->
                    p.DbType <- DbType.String
                    setParamValue p v

                | SqlType.AnsiString v ->
                    p.DbType <- DbType.AnsiString
                    setParamValue p v

                | SqlType.Boolean v ->
                    p.DbType <- DbType.Boolean
                    setParamValue p v

                | SqlType.Byte v ->
                    p.DbType <- DbType.Byte
                    setParamValue p v

                | SqlType.Char v ->
                    p.DbType <- DbType.AnsiString
                    setParamValue p v

                | SqlType.AnsiChar v ->
                    p.DbType <- DbType.String
                    setParamValue p v

                | SqlType.Decimal v ->
                    p.DbType <- DbType.Decimal
                    setParamValue p v

                | SqlType.Double v
                | SqlType.Float v ->
                    p.DbType <- DbType.Double
                    setParamValue p v

                | SqlType.Int16 v ->
                    p.DbType <- DbType.Int16
                    setParamValue p v

                | SqlType.Int32 v
                | SqlType.Int v ->
                    p.DbType <- DbType.Int32
                    setParamValue p v

                | SqlType.Int64 v ->
                    p.DbType <- DbType.Int64
                    setParamValue p v

                | SqlType.Guid v ->
                    p.DbType <- DbType.Guid
                    setParamValue p v

                | SqlType.DateTime v ->
                    p.DbType <- DbType.DateTime
                    setParamValue p v

                | SqlType.Bytes v ->
                    p.DbType <- DbType.Binary
                    setParamValue p v

                x.Parameters.Add(p)
                |> ignore

            x

        member internal x.Exec () =
            try
                x.ExecuteNonQuery() |> ignore
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                raise (DbFailureException error)

        member internal x.ExecReader (cmdBehavior : CommandBehavior) =
            try
                x.ExecuteReader(cmdBehavior)
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                raise (DbFailureException error)

        member internal x.ExecScalar () =
            try
                x.ExecuteScalar()
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                raise (DbFailureException error)

    type DbCommand with
        member internal x.SetDbParams(param : DbParams) =
            (x :> IDbCommand).SetDbParams(param) :?> DbCommand

        member internal x.ExecAsync(?ct: CancellationToken) = task {
            try
                return! x.ExecuteNonQueryAsync(cancellationToken = defaultArg ct CancellationToken.None)
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                return raise (DbFailureException error)
        }

        member internal x.ExecReaderAsync(cmdBehavior : CommandBehavior, ?ct: CancellationToken) = task {
            try
                return! x.ExecuteReaderAsync(cmdBehavior, cancellationToken = defaultArg ct CancellationToken.None )
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                return raise (DbFailureException error)
        }

        member internal x.ExecScalarAsync(?ct: CancellationToken) = task {
            try
                return! x.ExecuteScalarAsync(cancellationToken = defaultArg ct CancellationToken.None )
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = x.CommandText
                    Error = ex }

                return raise (DbFailureException error)
        }

    /// IDataReader extensions
    type IDataReader with
        member private x.GetOrdinalOption (name : string) =
            try
                let i = x.GetOrdinal(name)

                match x.IsDBNull(i) with
                | true  -> None
                | false -> Some(i)
            with
            | :? IndexOutOfRangeException as ex ->
                let error = DataReaderOutOfRangeError {
                    FieldName = name
                    Error = ex }

                raise (DbFailureException error)

        member private x.GetOption (map : int -> 'a when 'a : struct) (name : string) =
            let fn v =
                try
                    map v
                with
                | :? InvalidCastException as ex ->
                    let error = DataReaderCastError {
                        FieldName = name
                        Error = ex }

                    raise (DbFailureException error)

            x.GetOrdinalOption(name)
            |> Option.map fn

        /// Safely retrieve String Option
        member x.ReadStringOption (name : string) =
            name |> x.GetOrdinalOption |> Option.map (fun i -> x.GetString(i))

        /// Safely retrieve Boolean Option
        member x.ReadBooleanOption (name : string) =
            name |> x.GetOption (fun i -> x.GetBoolean(i))

        /// Safely retrieve Byte Option
        member x.ReadByteOption (name : string) =
            name |> x.GetOption (fun i -> x.GetByte(i))

        /// Safely retrieve Char Option
        member x.ReadCharOption (name : string) =
            name |> x.GetOption (fun i -> x.GetString(i).[0])

        /// Safely retrieve DateTime Option
        member x.ReadDateTimeOption (name : string) =
            name |> x.GetOption (fun i -> x.GetDateTime(i))

        /// Safely retrieve Decimal Option
        member x.ReadDecimalOption (name : string) =
            name |> x.GetOption (fun i -> x.GetDecimal(i))

        /// Safely retrieve Double Option
        member x.ReadDoubleOption (name : string) =
            name |> x.GetOption (fun i -> x.GetDouble(i))

        /// Safely retrieve Float Option
        member x.ReadFloatOption (name : string) =
            x.ReadDoubleOption name

        /// Safely retrieve Guid Option
        member x.ReadGuidOption (name : string) =
            name |> x.GetOption (fun i -> x.GetGuid(i))

        /// Safely retrieve Int16 Option
        member x.ReadInt16Option (name : string) =
            name |> x.GetOption (fun i -> x.GetInt16(i))

        /// Safely retrieve Int32 Option
        member x.ReadInt32Option (name : string) =
            name |> x.GetOption (fun i -> x.GetInt32(i))

        /// Safely retrieve Int64 Option
        member x.ReadInt64Option (name : string) =
            name |> x.GetOption (fun i -> x.GetInt64(i))

        // ------------
        // Defaults
        // ------------

        /// Safely retrieve String or return provided default
        member x.ReadString (name : string) =
            x.ReadStringOption name |> Option.defaultValue String.Empty

        /// Safely retrieve Boolean or return provided default
        member x.ReadBoolean (name : string) =
            x.ReadBooleanOption name |> Option.defaultValue false

        /// Safely retrieve Byte or return provided default
        member x.ReadByte (name : string) =
            x.ReadByteOption name |> Option.defaultValue Byte.MinValue

        /// Safely retrieve Char or return provided default
        member x.ReadChar (name : string) =
            x.ReadCharOption name |> Option.defaultValue Char.MinValue

        /// Safely retrieve DateTime or return provided default
        member x.ReadDateTime (name : string) =
            x.ReadDateTimeOption name |> Option.defaultValue DateTime.MinValue

        /// Safely retrieve Decimal or return provided default
        member x.ReadDecimal (name : string) =
            x.ReadDecimalOption name |> Option.defaultValue 0.0M

        /// Safely retrieve Double or return provided default
        member x.ReadDouble (name : string) =
            x.ReadDoubleOption name |> Option.defaultValue 0.0

        /// Safely retrieve Float or return provided default
        member x.ReadFloat (name : string) =
            x.ReadFloatOption name |> Option.defaultValue 0.0

        /// Safely retrieve Guid or return provided default
        member x.ReadGuid (name : string) =
            x.ReadGuidOption name |> Option.defaultValue Guid.Empty

        /// Safely retrieve Int16 or return provided default
        member x.ReadInt16 (name : string) =
            x.ReadInt16Option name |> Option.defaultValue 0s

        /// Safely retrieve Int32 or return provided default
        member x.ReadInt32 (name : string) =
            x.ReadInt32Option name |> Option.defaultValue 0

        /// Safely retrieve Int64 or return provided default
        member x.ReadInt64 (name : string) =
            x.ReadInt64Option name |> Option.defaultValue 0L

        /// Safely retrieve byte[]
        member x.ReadBytesOption (name : string) : byte[] option =
            match name |> x.GetOrdinalOption with
            | None   -> None
            | Some i ->
                use ms = new MemoryStream()
                let bufferSize = 1024
                let buffer = Array.zeroCreate bufferSize
                let rec chunkValue (position: int64) (str : Stream) (rd : IDataReader) =
                    match rd.GetBytes(i, position, buffer, 0, buffer.Length) with
                    | 0L   -> ()
                    | read ->
                        ms.Write(buffer, 0, int read)
                        chunkValue (position + read) str rd

                chunkValue 0L ms x |> ignore
                Some (ms.ToArray())

        /// Safely retrieve byte[] or return provided default
        member x.ReadBytes (name : string) : byte[] =
            match x.ReadBytesOption name with
            | None       -> Array.zeroCreate 0
            | Some bytes -> bytes
