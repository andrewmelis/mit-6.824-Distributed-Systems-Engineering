package mapreduce

import (
	"encoding/json"
	_ "fmt"
	"log"
	"os"
	_ "sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	//////////////////

	// get all intermediate files for this reducetask
	// there are nMap files of form:
	// mrtemp-<jobName>-<range nMap>-<reduceTaskNumber>

	decoders := make([]*json.Decoder, 0, nMap) // DISCUSSION: don't forget second argument
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		decoders = append(decoders, decoder)
	}

	// parse all these files from json
	var kv KeyValue // DISCUSSION: right place to init this??
	kvs := make([]KeyValue, 0)
	for _, decoder := range decoders {
		for decoder.More() {
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			kvs = append(kvs, kv)
		}
	}

	// sort all the data
	// sort.Sort(kvs) // TODO: implement sort.Interface

	// group each key, merging all values into a single array
	// (if i don't group, TestSingle should pass, but not TestMany)
	// UPDATE: passed anyways?

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, kv := range kvs {
		enc.Encode(KeyValue{kv.Key, reduceF(kv.Key, []string{kv.Value})})
	}

}

// implement sort.Interface for KeyValues
// type ByKey []KeyValue

// func (a ByKey) Len() int { return len(a) }

// func (
