package surfstore

import (
	"errors"
	"log"
	"strconv"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData //string = filename
}

//JH: return a map of files in cloud including version, filename and hashlist
func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	//panic("todo")
	log.Println("get serverFileInfoMap")
	*serverFileInfoMap = m.FileMetaMap
	return nil

}


func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	//panic("todo")
	log.Println("update fileMetaData to server")
	filename := fileMetaData.Filename
	localVersion := fileMetaData.Version
	if localVersion == 1 {
		m.FileMetaMap[filename] = *fileMetaData
		*latestVersion = 1
	} else {
		serverVersion := m.FileMetaMap[filename].Version
		if  localVersion - serverVersion == 1 {
			m.FileMetaMap[filename] = *fileMetaData
			*latestVersion = serverVersion + 1
		} else {
			err = errors.New("expected version >=" + strconv.Itoa(serverVersion+1))
			*latestVersion = serverVersion
			return err
		}
	}
	return nil;
}

var _ MetaStoreInterface = new(MetaStore)
