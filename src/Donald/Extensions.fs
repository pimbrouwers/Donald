namespace Donald

open System
open System.Data
open System.Data.Common
open System.IO
open System.Threading.Tasks

[<AutoOpen>]
module Extensions =
    type IDbConnection with
        /// Safely attempt to open a new IDbTransaction or
        /// return FailedOpenConnectionException.
        member this.TryOpenConnection()  =
            try
                if this.State = ConnectionState.Closed then
                    this.Open()
            with ex ->
                let error = DbConnectionError {
                    ConnectionString = this.ConnectionString
                    Error = ex
                }
                raise (ManagedException error)

        /// Safely attempt to create a new IDbTransaction or
        /// return CouldNotBeginTransactionException.
        member this.TryBeginTransaction()  =
            try
                this.TryOpenConnection()
                this.BeginTransaction()
            with
            | ex ->
                let error = DbTransactionError {
                    Step = TxBegin
                    Error = ex
                }
                raise (ManagedException error)

    type IDbTransaction with
        /// Safely attempt to rollback an IDbTransaction.
        member this.TryRollback() =
            try
                if not(isNull this)
                   && not(isNull this.Connection) then this.Rollback()
            with ex  ->
                let error = DbTransactionError {
                    Step = TxRollback
                    Error = ex
                }
                raise (ManagedException error)

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

                let error = DbTransactionError {
                    Step = TxCommit
                    Error = ex
                }
                raise (ManagedException error)

    type IDbCommand with
        member internal this.SetDbParams(dbParams : DbParams) =
            let setParamValue (p : IDbDataParameter) (v : obj) =
                if isNull v then p.Value <- DBNull.Value
                else p.Value <- v

            this.Parameters.Clear() // clear to ensure a clean working set

            for param in dbParams do
                let p = this.CreateParameter()
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

                this.Parameters.Add(p)
                |> ignore

            this

        member private this.TryDo (fn : IDbCommand -> 'a) : 'a =
            try
                fn this
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = this.CommandText
                    Error = ex
                }
                raise (ManagedException error)

        member internal this.Exec () =
            this.TryDo (fun this -> this.ExecuteNonQuery() |> ignore)

        member internal this.ExecReader (cmdBehavior : CommandBehavior) =
            this.TryDo (fun this -> this.ExecuteReader(cmdBehavior))

    type DbCommand with
        member private this.TryDoAsync (fn : DbCommand -> Task<'a>) : Task<'a> =
            try
                fn this
            with
            | :? DbException as ex ->
                let error = DbExecutionError {
                    Statement = this.CommandText
                    Error = ex
                }
                raise (ManagedException error)

        member internal this.SetDbParams(param : DbParams) =
            (this :> IDbCommand).SetDbParams(param) :?> DbCommand

        member internal this.ExecAsync() =
            this.TryDoAsync (fun this -> this.ExecuteNonQueryAsync())

        member internal this.ExecReaderAsync(cmdBehavior : CommandBehavior) =
            this.TryDoAsync (fun this -> this.ExecuteReaderAsync(cmdBehavior))

    /// IDataReader extensions
    type IDataReader with
        member private this.GetOrdinalOption (name : string) =
            let getColumnIndex name =
                try
                    this.GetOrdinal(name)
                with
                | :? IndexOutOfRangeException as ex ->
                    let error = DataReaderInvalidColumnError {
                        ColumnName = name
                        Error = ex
                    }
                    raise (ManagedException error)

            let i = getColumnIndex name
            match this.IsDBNull(i) with
            | true  -> None
            | false -> Some(i)

        member private this.GetOption (map : int -> 'a when 'a : struct) (name : string) =
            let fn v =
                try
                    map v
                with
                | :? InvalidCastException as ex ->
                    let error = DataReaderCastError {
                        FieldName = name
                        Error = ex
                    }
                    raise (ManagedException error)

            this.GetOrdinalOption(name)
            |> Option.map fn

        /// Safely retrieve String Option
        member this.ReadStringOption (name : string) =
            name |> this.GetOrdinalOption |> Option.map (fun i -> this.GetString(i))

        /// Safely retrieve Boolean Option
        member this.ReadBooleanOption (name : string) =
            name |> this.GetOption (fun i -> this.GetBoolean(i))

        /// Safely retrieve Byte Option
        member this.ReadByteOption (name : string) =
            name |> this.GetOption (fun i -> this.GetByte(i))

        /// Safely retrieve Char Option
        member this.ReadCharOption (name : string) =
            name |> this.GetOption (fun i -> this.GetString(i).[0])

        /// Safely retrieve DateTime Option
        member this.ReadDateTimeOption (name : string) =
            name |> this.GetOption (fun i -> this.GetDateTime(i))

        /// Safely retrieve Decimal Option
        member this.ReadDecimalOption (name : string) =
            name |> this.GetOption (fun i -> this.GetDecimal(i))

        /// Safely retrieve Double Option
        member this.ReadDoubleOption (name : string) =
            name |> this.GetOption (fun i -> this.GetDouble(i))

        /// Safely retrieve Float Option
        member this.ReadFloatOption (name : string) =
            this.ReadDoubleOption name

        /// Safely retrieve Guid Option
        member this.ReadGuidOption (name : string) =
            name |> this.GetOption (fun i -> this.GetGuid(i))

        /// Safely retrieve Int16 Option
        member this.ReadInt16Option (name : string) =
            name |> this.GetOption (fun i -> this.GetInt16(i))

        /// Safely retrieve Int32 Option
        member this.ReadInt32Option (name : string) =
            name |> this.GetOption (fun i -> this.GetInt32(i))

        /// Safely retrieve Int64 Option
        member this.ReadInt64Option (name : string) =
            name |> this.GetOption (fun i -> this.GetInt64(i))

        // ------------
        // Defaults
        // ------------

        /// Safely retrieve String or return provided default
        member this.ReadString (name : string) =
            this.ReadStringOption name |> Option.defaultValue String.Empty

        /// Safely retrieve Boolean or return provided default
        member this.ReadBoolean (name : string) =
            this.ReadBooleanOption name |> Option.defaultValue false

        /// Safely retrieve Byte or return provided default
        member this.ReadByte (name : string) =
            this.ReadByteOption name |> Option.defaultValue Byte.MinValue

        /// Safely retrieve Char or return provided default
        member this.ReadChar (name : string) =
            this.ReadCharOption name |> Option.defaultValue Char.MinValue

        /// Safely retrieve DateTime or return provided default
        member this.ReadDateTime (name : string) =
            this.ReadDateTimeOption name |> Option.defaultValue DateTime.MinValue

        /// Safely retrieve Decimal or return provided default
        member this.ReadDecimal (name : string) =
            this.ReadDecimalOption name |> Option.defaultValue 0.0M

        /// Safely retrieve Double or return provided default
        member this.ReadDouble (name : string) =
            this.ReadDoubleOption name |> Option.defaultValue 0.0

        /// Safely retrieve Float or return provided default
        member this.ReadFloat (name : string) =
            this.ReadFloatOption name |> Option.defaultValue 0.0

        /// Safely retrieve Guid or return provided default
        member this.ReadGuid (name : string) =
            this.ReadGuidOption name |> Option.defaultValue Guid.Empty

        /// Safely retrieve Int16 or return provided default
        member this.ReadInt16 (name : string) =
            this.ReadInt16Option name |> Option.defaultValue 0s

        /// Safely retrieve Int32 or return provided default
        member this.ReadInt32 (name : string) =
            this.ReadInt32Option name |> Option.defaultValue 0

        /// Safely retrieve Int64 or return provided default
        member this.ReadInt64 (name : string) =
            this.ReadInt64Option name |> Option.defaultValue 0L

        /// Safely retrieve byte[]
        member this.ReadBytesOption (name : string) : byte[] option =
            match name |> this.GetOrdinalOption with
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

                chunkValue 0L ms this |> ignore
                Some (ms.ToArray())

        /// Safely retrieve byte[] or return provided default
        member this.ReadBytes (name : string) : byte[] =
            match this.ReadBytesOption name with
            | None       -> Array.zeroCreate 0
            | Some bytes -> bytes
