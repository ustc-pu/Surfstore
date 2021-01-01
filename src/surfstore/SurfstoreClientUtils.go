package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {

	indexFilePath := client.BaseDir + "/index.txt"
	_, err := os.Stat(indexFilePath)
	if os.IsNotExist(err) {
		_, err = os.Create(indexFilePath)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	//build oldMap/local fileMetaDataMap from index.txt
	oldMap := readIndex(indexFilePath)
	client.oldMap = make(map[string]FileMetaData)
	for key, value := range oldMap {
		client.oldMap[key] = value
	}
	//scan base dir to build newMap/current fileMetaDataMap
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	var newMap = make(map[string]FileMetaData)
	for _, file := range files {
		blockOfFileHash := []string{}
		buf := make([]byte, client.BlockSize)
		if file.Name() == "index.txt" || file.Name()[0] == '.' ||
			strings.Contains(file.Name(), ",") {
			continue
		}
		file_handler, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err = file_handler.Close(); err != nil {
				log.Panicln(err)
			}
		}()
		for {
			size, err := file_handler.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				break
			}
			h := sha256.New()
			h.Write(buf[:size])
			blockHashValue := hex.EncodeToString(h.Sum(nil))
			blockOfFileHash = append(blockOfFileHash, blockHashValue)
		}
		newMap[file.Name()] = FileMetaData{file.Name(), 1, blockOfFileHash}
	}

	//build remoteMap
	var succ *bool
	succ = new(bool)
	*succ = false
	var remoteMapPt = new(map[string]FileMetaData)
	remoteMap := client.getRemoteIndex(succ, remoteMapPt)

	// 1. Sync
	// 1.1 new file in server and need to download:
	client.ifRemoteFileInLocal(remoteMap, oldMap, newMap)
	//client.ifRemoteFileInLocal(&remoteMap, &newMap)

	// 2 local change to server, either local new, local modification or local deletion
	//2.1 local new files
	client.ifLocalFileInRemote(remoteMap, oldMap)
	client.ifLocalFileInRemote(remoteMap, newMap)

	//2.2 local changed files
	client.commitLocalChangedFiles(newMap, oldMap, remoteMap)

	//2.3 local delete files
	client.deleteLocalFiles(newMap, oldMap, remoteMap)

	//3 write old map to local index.txt
	client.writeIntoIndex(indexFilePath, client.oldMap)
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}

func readIndex(dir string) map[string]FileMetaData {
	f, err := os.Open(dir)
	if err != nil {
		log.Panicln(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Panicln(err)
		}
	}()

	// lines := []string{}
	indexMap := make(map[string]FileMetaData)
	read := bufio.NewReader(f)
	// s := bufio.NewScanner(f)
	// for s.Scan() {
	// 	curline := s.Text()

	// 	splitByComma := strings.Split(curline, ",")
	// 	//fmt.Println(splitByComma)
	// 	fileName := splitByComma[0]
	// 	fileVersion, err := strconv.Atoi(splitByComma[1])
	// 	if err != nil {
	// 		log.Panicln(err)
	// 	}
	// 	hashList := strings.Split(splitByComma[2], " ")
	// 	indexMap[fileName] = FileMetaData{fileName, fileVersion, hashList}
	// }

	for {
		curline, err := read.ReadString('\n')
		if len(curline) >= 1 && curline[len(curline)-1] == '\n' {
			curline = curline[0:(len(curline)-1)]
		}
		if err == io.EOF {
			fmt.Print(curline)
			break
		}
		splitByComma := strings.Split(curline, ",")
		fileName := splitByComma[0]
		fileVersion, err := strconv.Atoi(splitByComma[1])
		if err != nil {
			log.Panicln(err)
		}
		hashList := strings.Split(splitByComma[2], " ")
		indexMap[fileName] = FileMetaData{fileName, fileVersion, hashList}
	}

	return indexMap
}

func (client *RPCClient) getRemoteIndex(succ *bool, serverFileInfoMap *map[string]FileMetaData) map[string]FileMetaData {
	client.GetFileInfoMap(succ, serverFileInfoMap)
	return *serverFileInfoMap
}

func (client *RPCClient) ifLocalFileInRemote(remoteMap, oldMap map[string]FileMetaData) {
	for filename := range oldMap {
		if _, ok := (remoteMap)[filename]; !ok { //file in oldMap but not in remoteMap
			log.Println("find a new file", filename)
			blockOfFileHash := client.uploadFileBlockList(filename)
			upMetaData := FileMetaData{filename, 1, blockOfFileHash}
			latestVersion := 1
			err := client.updateFileMetaData(&upMetaData, &latestVersion)
			if err != nil {
				//conflict, sync with server
				client.syncRemoteChangedFile(remoteMap, oldMap, filename, latestVersion)
			} else {
				client.oldMap[filename] = upMetaData
			}
		}
	}
	//PrintMetaMap(*oldMap)
}

func (client *RPCClient) ifRemoteFileInLocal(remoteMap, oldMap, newMap map[string]FileMetaData) {
	var blockList []Block
	for filename, remoteMetaData := range remoteMap {
		if _, ok := (oldMap)[filename]; !ok {
			if _, ok := (newMap)[filename]; !ok {
				// get all blocks of new file
				log.Println("download file", filename, "from server to local")
				filePath := client.BaseDir + "/" + filename
				oldHashList := (oldMap)[filename].BlockHashList
				remoteHashList := (remoteMap)[filename].BlockHashList
				client.downLoadFileToLocal(oldHashList, remoteHashList, blockList, filePath)
				client.oldMap[filename] = remoteMetaData
			}
		} else if oldMap[filename].BlockHashList[0] == "0" && remoteMetaData.BlockHashList[0] != "0" {
			// other client re-creates a deleted file
			log.Println("a deleted file", filename, "is recreated by other client")
			log.Println("download file", filename, "from server to local")
			filePath := client.BaseDir + "/" + filename
			oldHashList := (oldMap)[filename].BlockHashList
			oldHashList = nil
			remoteHashList := (remoteMap)[filename].BlockHashList
			client.downLoadFileToLocal(oldHashList, remoteHashList, blockList, filePath)
			client.oldMap[filename] = remoteMetaData
		}

	}
}

func (client *RPCClient) writeIntoIndex(dir string, oldMap map[string]FileMetaData) {
	log.Println("write oldMap to index.txt")
	err := os.Truncate(dir, 0)
	if err != nil {
		log.Fatal(err)
	}

	//write new content
	writeString := ""
	for filename, oldMetaData := range oldMap {
		writeString += (filename + "," + strconv.Itoa(oldMetaData.Version) + ",")
		for idx, hvalue := range oldMetaData.BlockHashList {
			writeString += hvalue
			if idx < len(oldMetaData.BlockHashList)-1 {
				writeString += " "
			}

		}
		writeString += ("\n")
	}
	writeIn := []byte(writeString)
	err = ioutil.WriteFile(dir, writeIn, 0644)
	if err != nil {
		fmt.Println(err)
	}
}

func (client *RPCClient) buildBlockListFromFile(fileName string, blockList *[]Block) {
	buf := make([]byte, client.BlockSize)
	fileObj, err := os.Open(client.BaseDir + "/" + fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = fileObj.Close(); err != nil {
			log.Panicln(err)
		}
	}()
	//build local file block list
	for {
		size, err := fileObj.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		*blockList = append(*blockList, Block{buf[:size], size})
	}
}

func (client *RPCClient) downLoadFileToLocal(oldHashList, remoteHashList []string, blockList []Block, filePath string) {
	log.Println("rebuild local file from blocklist", filePath)
	if remoteHashList[0] != "0" {
		for idx := range remoteHashList {
			//log.Println(remoteHashList[idx])
			var block *Block
			block = new(Block)
			if idx >= len(oldHashList) {
				//download remote block to local
				blockList = append(blockList, client.downLoadBlock(remoteHashList[idx], block))
			}
			if idx < len(oldHashList) && remoteHashList[idx] != oldHashList[idx] {
				blockList[idx] = client.downLoadBlock(remoteHashList[idx], block)
			}
		}

		// build local file with blockList, overwrite old file
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
		defer func() {
			if err = f.Close(); err != nil {
				log.Println(err)
			}
		}()
		err = os.Truncate(filePath, 0)
		if err != nil {
			log.Println(err)
		}
		for _, block := range blockList {
			_, err = f.Write(block.BlockData[:block.BlockSize])
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (client *RPCClient) commitLocalChangedFiles(newMap, oldMap, remoteMap map[string]FileMetaData) {
	//file in old map also in new map, but hash list is diff
	for fileName, _ := range oldMap {
		if _, ok := (newMap)[fileName]; ok {
			oldMetaData := (oldMap)[fileName]
			newMetaData := (newMap)[fileName]
			remoteMetaData := (remoteMap)[fileName]
			//log.Println("-----oldMap-----")
			//log.Println(oldMetaData.BlockHashList)
			//log.Println("-----newMap-----")
			//log.Println(newMetaData.BlockHashList)
			if !reflect.DeepEqual(oldMetaData.BlockHashList, newMetaData.BlockHashList) {
				log.Println(fileName, ": oldHashList and newHashList are different")
				if remoteMetaData.Version == oldMetaData.Version {
					//upload file blocks to server
					blockOfFileHash := client.uploadFileBlockList(fileName)
					//upload file metadata map to server
					upMetaData := FileMetaData{fileName, oldMetaData.Version + 1, blockOfFileHash}
					latestVersion := oldMetaData.Version + 1
					err := client.updateFileMetaData(&upMetaData, &latestVersion)
					if err != nil {
						//conflict, sync with server
						client.syncRemoteChangedFile(remoteMap, oldMap, fileName, latestVersion)
					} else {
						client.oldMap[fileName] = upMetaData
					}
				}

				if remoteMetaData.Version > oldMetaData.Version {
					client.syncRemoteChangedFile(remoteMap, oldMap, fileName, remoteMetaData.Version)
				}
			} else {
				if remoteMetaData.Version > oldMetaData.Version {
					client.syncRemoteChangedFile(remoteMap, oldMap, fileName, remoteMetaData.Version)
				}
			}
		}
	}
}

func (client *RPCClient) deleteLocalFiles(newMap, oldMap, remoteMap map[string]FileMetaData) {
	//file in old map but not in new map
	for fileName, _ := range oldMap {
		if _, ok := (newMap)[fileName]; !ok && oldMap[fileName].BlockHashList[0] != "0" {
			log.Println(fileName, "was deleted")
			oldMetaData := (oldMap)[fileName]
			oldMetaData.BlockHashList = nil
			oldMetaData.BlockHashList = append(oldMetaData.BlockHashList, "0")
			oldMetaData.Version++
			var latestVersion int
			latestVersion = oldMetaData.Version
			//log.Println("latesteVersion is:", latestVersion)
			err := client.UpdateFile(&oldMetaData, &latestVersion)
			if err != nil {
				client.syncRemoteChangedFile(remoteMap, oldMap, fileName, latestVersion)
			} else {
				//log.Println("after upload latesteVersion is:", latestVersion)
				oldMetaData.Version = latestVersion
				client.oldMap[fileName] = oldMetaData
				os.Remove(client.BaseDir + "/" + fileName)
			}
		}
	}
}

func (client *RPCClient) syncRemoteChangedFile(remoteMap, oldMap map[string]FileMetaData,
	fileName string, latestVersion int) {
	remoteMetaData := (remoteMap)[fileName]
	oldMetaData := (oldMap)[fileName]
	log.Print(remoteMetaData.Version, " ", oldMetaData.Version, " ")
	filePath := client.BaseDir + "/" + fileName
	if remoteMetaData.Version > oldMetaData.Version && remoteMetaData.BlockHashList[0] != "0" {
		log.Println("remote version is larger and remote hash is not 0")
		var blockList []Block
		//build local file block list
		client.buildBlockListFromFile(fileName, &blockList)
		oldHashList := oldMetaData.BlockHashList
		remoteHashList := remoteMetaData.BlockHashList
		//update local block list, download file to local and overwrite local file
		client.downLoadFileToLocal(oldHashList, remoteHashList, blockList, filePath)
		//update oldMap
		oldMetaData.Version = latestVersion
		oldMetaData.BlockHashList = remoteHashList
		client.oldMap[fileName] = oldMetaData
	}

	if remoteMetaData.Version > oldMetaData.Version && remoteMetaData.BlockHashList[0] == "0" {
		//update local metadata: update version, hash list to 0, Delete local file
		log.Println("remote version is larger and remote hash is 0")
		oldMetaData.Version = latestVersion
		oldMetaData.BlockHashList = nil
		oldMetaData.BlockHashList = append(oldMetaData.BlockHashList, "0")
		//remove file
		e := os.Remove(filePath)
		if e != nil {
			log.Println(e)
		}
	}
	client.oldMap[fileName] = oldMetaData
}

func (client *RPCClient) uploadFileBlockList(fileName string) []string {
	log.Println("upload file blocks list to server:", fileName)
	buf := make([]byte, client.BlockSize)
	filePath := client.BaseDir + "/" + fileName
	log.Println("generate blocklist of file", filePath)
	fileObj, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = fileObj.Close(); err != nil {
			log.Panicln(err)
		}
	}()
	blockOfFileHashList := []string{}
	var upBlock Block
	for {
		size, err := fileObj.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		var succ *bool
		succ = new(bool)
		*succ = false
		upBlock.BlockData = buf[:size]
		upBlock.BlockSize = size
		err = client.upLoadBlock(upBlock, succ)
		if !*succ || err != nil{
			log.Println(err)
		}
		h := sha256.New()
		h.Write(buf[:size])
		blockHashValue := hex.EncodeToString(h.Sum(nil))
		blockOfFileHashList = append(blockOfFileHashList, blockHashValue)
	}
	return blockOfFileHashList
}

func (client *RPCClient) downLoadBlock(blockHash string, blockData *Block) Block {
	client.GetBlock(blockHash, blockData)
	return *blockData
}

func (client *RPCClient) upLoadBlock(block Block, succ *bool) error {
	return client.PutBlock(block, succ)
}

func (client *RPCClient) updateFileMetaData(fileMetaData *FileMetaData, latestVersion *int) error {
	return client.UpdateFile(fileMetaData, latestVersion)
}
