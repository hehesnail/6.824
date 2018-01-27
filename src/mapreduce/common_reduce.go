package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

type kvs []KeyValue

func (kv kvs) Len() int {
	return len(kv)
}

func (kv kvs) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

func (kv kvs) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//First, Read the corresponding files in and decode it to kv slice
	var filenames []string
	var files []*os.File
	var decs []*json.Decoder

	//Create file pointers and decoders
	for m := 0; m < nMap; m++ {
		filenames = append(filenames, reduceName(jobName, m, reduceTask))
		tempf, err := os.Open(filenames[m])
		if err != nil {
			log.Fatal("can not read the intermediate files", err)
		}
		files = append(files, tempf)
		dec := json.NewDecoder(tempf)
		decs = append(decs, dec)
	}

	//Decode kv values from the files
	var kvslice []KeyValue
	for _, dec := range decs {
		for dec.More() {
			var tempkv KeyValue
			err := dec.Decode(&tempkv)
			if err != nil {
				log.Fatal("decode error", err)
			}
			kvslice = append(kvslice, tempkv)
		}
	}

	//Close all file pointers
	for _, tempf := range files {
		tempf.Close()
	}

	//Sort the kvslice by the keys.
	//Define the needed sort.Interface(Len, Swap, Less) first.
	sort.Sort(kvs(kvslice))

	//Group kvslice to (k, list(v)) form
	//Be careful here!
	//Use var kvmap map[string][]string will generate nil map
	//So, here we should use make to generate an empty map
	kvmap := make(map[string][]string)
	for _, kv := range kvslice {
		kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
	}

	//Call the reduce function and get the fnail k/v result
	var finalkv []KeyValue
	for k, v := range kvmap {
		value := reduceF(k, v)
		finalkv = append(finalkv, KeyValue{k, value})
	}

	//Encode the final result to the file
	f, err := os.Create(outFile)
	if err != nil {
		log.Fatal("can not create the output file")
	}

	defer f.Close()
	enc := json.NewEncoder(f)
	for _, kv := range finalkv {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("encode error", err)
		}
	}
}
