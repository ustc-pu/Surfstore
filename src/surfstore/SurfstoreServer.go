package surfstore

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
}

func (s *Server) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	//panic("todo")
	return s.MetaStore.GetFileInfoMap(succ, serverFileInfoMap)
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	//panic("todo")
	return s.MetaStore.UpdateFile(fileMetaData, latestVersion)
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	//panic("todo")
	return s.BlockStore.GetBlock(blockHash, blockData)
}


func (s *Server) PutBlock(blockData Block, succ *bool) error{
		//panic("todo")
	return s.BlockStore.PutBlock(blockData, succ)
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	//panic("todo")
	return s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}

	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
	}
}

func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {
	//panic("todo")
	err := rpc.Register(&surfstoreServer)

	if err != nil{
		log.Fatal("error surfstoreServer :", err) 
	}

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", hostAddr)

	if err != nil {
		log.Println("TCP listen error: ", err)
	}

	log.Printf("serving rpc on %v", hostAddr)

	err = http.Serve(listener, nil)
	if err != nil{
		log.Fatal("coule not start listening on http: ", err)
	}
	return err
}
