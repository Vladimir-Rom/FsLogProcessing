module StreamUtils

open System.IO
open System.Text

let getStreamWriter stream = new StreamWriter(stream, Encoding.UTF8, 1024, leaveOpen = true)