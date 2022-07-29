# py-evtx-ripper
A command line wrapper for the [python-evtx library.](https://github.com/williballenthin/python-evtx) 
Includes support for the following features: 

* Collection of one or many evtx files from a given path. 
* 'Concurrent' processing of multiple files.
* Export to CSV
* Export to SQLite database.
* Separation of SQLite databases by filename or the writing of all events to a single database.

## Why?
I needed a fairly low-tech solution to enable analysts to quickly parse and 
extract evtx event logs in environments without EventViewer, with the ability 
only pull out events of interest. 

## How to use it: 

```
-c --cores  # for selecting the number of cores/concurrent files to process.
-C --csv    # for selecting the csv parser.
-d --db     # for selecting the db parser.

-s --sep    # for seperating the db parser output.

-i --input  # input path or directory.
-o --output # output path.
```

#### Example: 
```
python evtx-ripper.py -d -i "C:\Users\the-siegfried\Events" -o "C:\Users\the-siegfried\output"
```
