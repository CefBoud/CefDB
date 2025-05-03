package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type BlockId struct {
	Filename string
	Blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	return &BlockId{Filename: filename, Blknum: blknum}
}

type FileMgr struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
	sync.Mutex
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// NewFileMgr creates a new FileMgr.
func NewFileMgr(dbDirectory string, blockSize int) (*FileMgr, error) {
	fm := &FileMgr{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		openFiles:   make(map[string]*os.File),
	}

	// Check if the directory exists
	_, err := os.Stat(dbDirectory)
	dirExists := err == nil
	fm.isNew = !dirExists

	// Create the directory if the database is new
	if fm.isNew {
		if err := os.MkdirAll(dbDirectory, 0777); err != nil {
			return nil, fmt.Errorf("creating database directory '%v': %w", dbDirectory, err)
		}
	}

	// Remove any leftover temporary tables
	files, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, fmt.Errorf("reading directory '%v': %w", dbDirectory, err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "temp") {
			tempFilePath := filepath.Join(dbDirectory, file.Name())
			if err := os.Remove(tempFilePath); err != nil {
				fmt.Printf("warning: could not delete temporary file '%v': %v\n", tempFilePath, err)
			}
		}
	}
	return fm, nil
}

// Read reads a block from the specified BlockId into the Page.
func (fm *FileMgr) Read(blk *BlockId, p *Page) error {
	fm.Lock()
	defer fm.Unlock()

	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return fmt.Errorf("getting file '%v': %w", blk.Filename, err)
	}

	offset := int64(blk.Blknum) * int64(fm.blockSize)
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking to offset %d in file '%v': %w", offset, blk.Filename, err)
	}

	_, err = io.ReadFull(file, p.Contents())
	if err != nil {
		return fmt.Errorf("reading block %v from file '%v': %v", blk, blk.Filename, err)
	}

	return nil
}

// Write writes the contents of the Page to the specified BlockId.
func (fm *FileMgr) Write(blk *BlockId, p *Page) error {
	fm.Lock()
	defer fm.Unlock()

	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return fmt.Errorf("getting file '%v': %v", blk.Filename, err)
	}

	offset := int64(blk.Blknum) * int64(fm.blockSize)
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking to offset %d in file '%v': %w", offset, blk.Filename, err)
	}

	_, err = file.Write(p.Contents())
	if err != nil {
		return fmt.Errorf("writing block %v to file '%v': %w", blk, blk.Filename, err)
	}

	// Ensure the write is persisted to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing file '%v': %w", blk.Filename, err)
	}

	return nil
}

// Append appends a new block to the specified file and returns the BlockId of the new block.
func (fm *FileMgr) Append(filename string) (*BlockId, error) {
	fm.Lock()
	defer fm.Unlock()

	file, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("getting file '%v': %w", filename, err)
	}

	lastOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed seeking to end of file '%v': %v", filename, err)
	}

	newBlockId := int(int(lastOffset) / fm.blockSize)
	// Create a buffer of zeros for the new block
	b := make([]byte, fm.blockSize)
	_, err = file.Write(b)
	if err != nil {
		return nil, fmt.Errorf("appending block to file '%v': %v", filename, err)
	}

	// Ensure the write is persisted to disk
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("syncing file '%v': %v", filename, err)
	}

	return &BlockId{Filename: filename, Blknum: newBlockId}, nil
}

// Length returns the number of blocks in the specified file.
func (fm *FileMgr) Length(filename string) (int, error) {
	fm.Lock()
	defer fm.Unlock()

	file, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("getting file '%v': %w", filename, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("getting file info for '%v': %w", filename, err)
	}

	return int(fileInfo.Size() / int64(fm.blockSize)), nil
}

// IsNew returns true if the database directory was newly created.
func (fm *FileMgr) IsNew() bool {
	return fm.isNew
}

// BlockSize returns the block size.
func (fm *FileMgr) BlockSize() int {
	return fm.blockSize
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if file, ok := fm.openFiles[filename]; ok {
		return file, nil
	}

	filePath := filepath.Join(fm.dbDirectory, filename)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("opening file '%v': %w", filePath, err)
	}
	fm.openFiles[filename] = file
	return file, nil
}
