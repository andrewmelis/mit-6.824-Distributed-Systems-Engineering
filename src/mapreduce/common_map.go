package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// read input file
	// DISCUSSION: which reader to use?
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}

	// calls the user-defined map function (mapF) for that file's contents
	// DISCUSSION: bc used ioutil, have to cast contents to string here
	results := mapF(inFile, string(contents))

	// create + open all intermediate output files ahead of time
	// init a json encoder for each open intermediate file
	// return map map of filename => encoder
	encoders := make(map[string]*json.Encoder)
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTaskNumber, i)
		file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666) // better way to ensure append?
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoders[fileName] = encoder
	}

	// append each result key to the correct file as json
	for _, kv := range results {
		reduceTaskNumber := int(reduceTask(kv.Key, nReduce))
		outputFile := reduceName(jobName, mapTaskNumber, reduceTaskNumber)

		err = encoders[outputFile].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// DISCUSSION: correct to just cast return value here instead of at caller?
func reduceTask(key string, nReduce int) uint32 {
	return ihash(key) % uint32(nReduce)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
