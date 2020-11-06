module LogParsing

#load "Utils\\SequenceUtils.fsx"
#load "Utils\\ArrayUtils.fsx"
#load "Utils\\StreamUtils.fsx"

open System.Collections.Concurrent
open System.IO
open System.Text
open System
open System.Text.RegularExpressions
open System.Threading.Tasks
open SequenceUtils
open ArrayUtils

let bufferedSeq<'T> bufferSize sequence =
    let startBufferFilling (buffer: BlockingCollection<'T>) seq =
        Task.Run(
            fun() -> 
                seq |> Seq.iter (fun item -> buffer.Add(item)) 
                buffer.CompleteAdding()
        ) |> ignore

    seq {
        use buffer = new BlockingCollection<'T>(boundedCapacity=bufferSize)
        startBufferFilling buffer sequence 
        for item in buffer.GetConsumingEnumerable() do yield item
    }    

let private mergeSeq<'T when 'T : comparison> (sequences: seq<seq<'T>>) =
    seq {
        let mutable exit = false
        let mutable readyEnums = sequences |> Seq.map getSeqEnum |> Array.ofSeq |> Array.filter isReady
        while not exit do
            readyEnums <- readyEnums |> Array.filter isReady
            let min = 
                readyEnums 
                    |> Array.map (tryGetItem >> Option.get) 
                    |> getIndexOfMinBy id

            match min with
                | None -> exit <- true
                | Some (index, minValue) -> 
                    yield minValue
                    readyEnums.[index] <- readyEnums.[index] |> moveSeqEnum 
    }

let mergeLines isNew (lines: seq<string>) = 
    seq {
        let mutable rb = StringBuilder()
        for line in lines do
            if isNew line then
                if rb.Length > 0 then yield rb.ToString()
                rb <- StringBuilder(line)
            else
                rb.AppendLine(line) |> ignore
        if rb.Length > 0 then yield rb.ToString()
    }

type Severity = 
    | Error
    | Warning
    | Information
    | Verbose

type ItemRecord<'T> = { 
    date: DateTime; 
    severity: Severity; 
    body: string; 
    fullBody: string; 
    mutable next: ItemRecord<'T> option;
    mutable prev: ItemRecord<'T> option;
    record: 'T;
    fileName: string; 
} 

type PopulateItemRec<'T> = string * ItemRecord<'T> -> ItemRecord<'T>

let processStream<'T> isNewRecord emptyRecord (populateRecord: PopulateItemRec<'T>) stream =
    let parseStream isNewRecord map (stream: Stream) = 
        let strings = seq {
            use reader = new StreamReader(stream)
            while not reader.EndOfStream do yield reader.ReadLine ()
        }

        let stringRecords = mergeLines isNewRecord strings
        stringRecords |> Seq.map map

    let mapRecord str = 
        (
            str, 
            { 
                date = DateTime.MinValue;
                severity = Information;
                body = "";
                fullBody = str;
                next = None;
                prev = None;
                record = emptyRecord;
                fileName = "";
             }
        ) |> populateRecord
    stream |> parseStream isNewRecord mapRecord

let setLinks (records: seq<ItemRecord<'T>>) =
    let result = records |> Array.ofSeq
    result
        |> Array.pairwise
        |> Array.iter (
            fun (prev, next) -> 
                prev.next <- Some next
                next.prev <- Some prev)
    result

let createRegex regexSting = 
    Regex(regexSting, RegexOptions.Compiled ||| RegexOptions.CultureInvariant ||| RegexOptions.Singleline ||| RegexOptions.ExplicitCapture)

let parseString (regexp: Regex) input =
    let matchResult = regexp.Match(input)
    if matchResult.Success 
        then matchResult.Groups 
            |> Seq.cast<Group> 
            |> Seq.map (fun group -> (group.Name, group.Value)) 
            |> Map.ofSeq 
            |> Some
        else None

type LogRecordDescriptor<'T> =
    {
        isNewRecordRegexString: string;
        recordRegexString: string;
        populateRecord: Map<string, string> * ItemRecord<'T> -> ItemRecord<'T>;
        emptyRecord: 'T;
    }

let processStreamByRegex<'T> (recordDescr: LogRecordDescriptor<'T>) stream =

    let isNewRecordRegex = createRegex recordDescr.isNewRecordRegexString
    let recordRegex = createRegex recordDescr.recordRegexString
    let isNewRecord record = isNewRecordRegex.IsMatch(record)
    let populate (recordAsString, initialProps) = 
        match recordAsString |> parseString recordRegex with
            | Some parseResult -> (parseResult, initialProps) |> recordDescr.populateRecord
            | None -> initialProps
    stream |> processStream isNewRecord (recordDescr.emptyRecord) populate 

let processFile recordDescr fileName  =
    seq {
        use file = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
        yield! file |> processStreamByRegex recordDescr |> Seq.map (fun i -> { i with fileName = fileName })
    }

let processFiles 
    recordDescr 
    (filter: seq<ItemRecord<'a>> -> seq<ItemRecord<'a>>)
    fileNames =
    let bufferSize = 500000 / Array.length fileNames
    fileNames 
        |> Array.ofSeq
        |> Array.map (processFile recordDescr >> filter)
        |> Seq.ofArray
        |> Seq.map (bufferedSeq bufferSize)
        |> mergeSeq

let processDir recordDescr dir mask filter =
    Directory.EnumerateFiles(dir, mask) |> Array.ofSeq |> processFiles recordDescr filter

let outLog (writer: TextWriter) maxLines (records: seq<ItemRecord<'T>>) = 
    let getBody item = 
        if maxLines <= 0 
            then item.fullBody 
            else 
                item.fullBody.Split([|'\r'; '\n'|], StringSplitOptions.RemoveEmptyEntries)  
                    |> Seq.truncate maxLines
                    |> String.concat "\r\n"
                    
    records |> Seq.iter (fun record -> record |> getBody |> fprintfn writer "%s\r\n")

let printTrim maxCharacters (records: seq<ItemRecord<'T>>) = 
    records |> outLog Console.Out maxCharacters

let print (records: seq<ItemRecord<'T>>) = 
    records |> printTrim 0

let incl (substring: string) records =
    records |> Seq.filter (fun i -> i.body.Contains(substring))

let inclr (substring: string) records =
    records |> Seq.filter (fun i -> Regex.IsMatch(i.body, substring))

let excl (substring: string) records =
    records |> Seq.filter (fun i -> i.body.Contains(substring) |> not)

let errors records =
    records |> Seq.filter (fun i -> i.severity = Error)

let expandBefore count (records: seq<ItemRecord<'T>>) = 
    let seqEnum = getSeqEnum records
    let firstItem = tryGetItem seqEnum
    let before = 
        Seq.unfold 
            (function None -> None | Some item -> Some (item, item.prev)) 
            firstItem 
            |> Seq.truncate count
    let after = 
        Seq.unfold 
            (function 
                | SeqEnum.End -> None 
                | Ready item as currentSeqEnum-> Some (item.Current, moveSeqEnum currentSeqEnum)) 
            seqEnum

    Seq.append before after

let mapr (regex: string) (mapper: Map<string, string> -> 'a) (records: seq<ItemRecord<'T>>) =
    let reg = createRegex regex
    records 
        |> Seq.map (fun r -> parseString reg r.body)
        |> Seq.choose id
        |> Seq.map mapper 