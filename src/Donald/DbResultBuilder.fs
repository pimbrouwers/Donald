[<AutoOpen>]
module Donald.DbResultBuilder

open System
open System.Data

type DbResultBuilder() =
    member _.Return (x : 'a) = Ok x

    member _.ReturnFrom (result : DbResult<_>) = result

    member _.Zero () = Ok ()

    member _.Bind (result : DbResult<'a>, f) = Result.bind f result

let dbResult = DbResultBuilder()