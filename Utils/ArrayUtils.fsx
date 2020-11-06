module ArrayUtils

type ArrayEnum<'T> = 
    | Start of 'T[]
    | Single of 'T
    | First of int * 'T[]
    | Middle of int * 'T[] 
    | Last of int * 'T[]
    | End

let moveArrayEnum arrayEnum =
    match arrayEnum with
        | Start a when a.Length = 0 -> End
        | Start a when a.Length = 1 -> Single a.[0]
        | Single _ -> End
        | Start a -> First (0, a)
        | First (_, a) when a.Length = 2 -> Last (1, a)
        | First (_, a) -> Middle (1, a)
        | Middle (index, a) when index = a.Length - 2 -> Last (a.Length - 1, a)
        | Middle (index, a) -> Middle (index + 1, a)
        | Last _ -> End
        | End -> End

let getIndexOfMinBy getValue array =
    let rec findMin index min minIndex =
        if index >= Array.length array then 
            Some (minIndex, min)
        else    
            let current = getValue array.[index]
            if current < min 
                then findMin (index + 1) current index
                else findMin (index + 1) min minIndex
    if Array.isEmpty array then None else findMin 0 (getValue array.[0]) 0
                    
