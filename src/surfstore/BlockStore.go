package surfstore

//JH: implement sha256 function & encoing/hex
import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	//"log"
)

type BlockStore struct {
	// TODO: string = sha256 hashvalue
	BlockMap map[string]Block //block hash string -> block chunk
}

//JH: getblock if block exists in map
func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	//log.Println("download block")
	if content, ok := bs.BlockMap[blockHash]; ok {
		*blockData = content

	} else {
		err := errors.New("No related file in hashlist")
		return err
	}
	return nil
}

//JH: store block in k-v map, index by hash value h (apply sha256)
func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	//panic("todo")
	//TODO: block is block
	//TODO: block size condition, if larger, error

	h := sha256.New()
	h.Write(block.BlockData)
	key := hex.EncodeToString(h.Sum(nil))
	bs.BlockMap[key] = block
	*succ = true
	return nil
}

//JH: given list of In, return which in In is in k-v map
func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	//panic("todo")
	for _, v := range blockHashesIn {
		if _, ok := bs.BlockMap[v]; ok {
			*blockHashesOut = append(*blockHashesOut, v)
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
