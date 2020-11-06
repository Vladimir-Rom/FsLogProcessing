module SequenceUtils

open System.Collections.Generic

type SeqEnum<'T> = 
    | End
    | Ready of IEnumerator<'T>

let isReady seqEnum =
    match seqEnum with
        | Ready _ -> true
        | End -> false

let tryGetItem seqEnum = 
    match seqEnum with
        | Ready item -> Some (item.Current)
        | End -> None

let moveIEnumerator (enumerator: IEnumerator<'T>) = 
    if enumerator.MoveNext() then Ready enumerator else End

let getSeqEnum<'T> (sequence: seq<'T>) = sequence.GetEnumerator() |> moveIEnumerator

let moveSeqEnum seqEnum = 
    match seqEnum with
        | Ready e -> e |> moveIEnumerator
        | _ -> End

