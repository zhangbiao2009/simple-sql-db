package diskbased

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	// PageSize is the size of each page in bytes (4KB)
	PageSize = 4096

	// MaxPageCacheSize is the maximum number of pages to keep in memory
	MaxPageCacheSize = 1000
)

// PageID is a unique identifier for a page
type PageID uint32

// Page represents a fixed-size block of data
type Page struct {
	id       PageID
	data     []byte
	dirty    bool
	pinCount int
	manager  *PageManager
}

// ID returns the page identifier
func (p *Page) ID() PageID {
	return p.id
}

// Data returns the page data for reading/writing
func (p *Page) Data() []byte {
	return p.data
}

// IsDirty returns true if the page has been modified
func (p *Page) IsDirty() bool {
	return p.dirty
}

// MarkDirty marks the page as modified
func (p *Page) MarkDirty() {
	p.dirty = true
}

// Pin increases the pin count for this page
func (p *Page) Pin() {
	p.pinCount++
}

// Unpin decreases the pin count for this page
func (p *Page) Unpin() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// PageManager manages the allocation, reading, writing, and caching of pages
type PageManager struct {
	filename   string
	file       *os.File
	pageSize   int
	numPages   uint32
	freePages  []PageID
	pageCache  map[PageID]*Page
	cacheMutex sync.RWMutex
}

// NewPageManager creates a new page manager
func NewPageManager(filename string) (*PageManager, error) {
	// Open the file for reading and writing, create if it doesn't exist
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// Get file size to determine number of existing pages
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	fileSize := fileInfo.Size()
	numPages := uint32(fileSize / PageSize)

	// Create page manager
	pm := &PageManager{
		filename:  filename,
		file:      file,
		pageSize:  PageSize,
		numPages:  numPages,
		freePages: []PageID{},
		pageCache: make(map[PageID]*Page),
	}

	// If this is a new file, initialize the header page
	if numPages == 0 {
		headerPage, err := pm.createHeaderPage()
		if err != nil {
			file.Close()
			return nil, err
		}

		// Mark page as dirty to ensure it gets written
		headerPage.MarkDirty()
		pm.numPages = 1
	} else {
		// Read the header page to get free page list
		err = pm.loadFreePageList()
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	return pm, nil
}

// createHeaderPage initializes the header page for a new file
func (pm *PageManager) createHeaderPage() (*Page, error) {
	// Allocate page 0 as the header page
	headerPage := &Page{
		id:       0,
		data:     make([]byte, PageSize),
		dirty:    true,
		pinCount: 1,
		manager:  pm,
	}

	// Store this in the cache
	pm.cacheMutex.Lock()
	pm.pageCache[0] = headerPage
	pm.cacheMutex.Unlock()

	// Initialize header page with metadata
	// First 4 bytes: number of pages
	binary.LittleEndian.PutUint32(headerPage.data[0:4], 1)

	// Next 4 bytes: number of free pages
	binary.LittleEndian.PutUint32(headerPage.data[4:8], 0)

	return headerPage, nil
}

// loadFreePageList reads the free page list from the header page
func (pm *PageManager) loadFreePageList() error {
	headerPage, err := pm.GetPage(0)
	if err != nil {
		return err
	}

	// Read number of pages from header
	numPages := binary.LittleEndian.Uint32(headerPage.data[0:4])
	pm.numPages = numPages

	// Read number of free pages
	numFreePages := binary.LittleEndian.Uint32(headerPage.data[4:8])

	// Read free page IDs
	pm.freePages = make([]PageID, numFreePages)
	for i := uint32(0); i < numFreePages; i++ {
		offset := 8 + (i * 4)
		pm.freePages[i] = PageID(binary.LittleEndian.Uint32(headerPage.data[offset : offset+4]))
	}

	return nil
}

// updateFreePageList updates the free page list in the header page
func (pm *PageManager) updateFreePageList() error {
	headerPage, err := pm.GetPage(0)
	if err != nil {
		return err
	}

	// Update number of pages
	binary.LittleEndian.PutUint32(headerPage.data[0:4], pm.numPages)

	// Update number of free pages
	numFreePages := uint32(len(pm.freePages))
	binary.LittleEndian.PutUint32(headerPage.data[4:8], numFreePages)

	// Write free page IDs
	for i, pageID := range pm.freePages {
		offset := 8 + (uint32(i) * 4)
		if offset+4 > PageSize {
			// If free page list overflows the header page, we would need
			// additional pages to store it. For simplicity, we limit the
			// number of free pages tracked.
			break
		}
		binary.LittleEndian.PutUint32(headerPage.data[offset:offset+4], uint32(pageID))
	}

	headerPage.MarkDirty()
	return nil
}

// AllocatePage allocates a new page or reuses a free page
func (pm *PageManager) AllocatePage() (*Page, error) {
	pm.cacheMutex.Lock()
	defer pm.cacheMutex.Unlock()

	var pageID PageID

	// Check if there are any free pages we can reuse
	if len(pm.freePages) > 0 {
		// Reuse a freed page
		pageID = pm.freePages[len(pm.freePages)-1]
		pm.freePages = pm.freePages[:len(pm.freePages)-1]
	} else {
		// Allocate a new page at the end of the file
		pageID = PageID(pm.numPages)
		pm.numPages++

		// Extend the file
		err := pm.extendFile(1)
		if err != nil {
			return nil, err
		}
	}

	// Create page struct and cache it
	page := &Page{
		id:       pageID,
		data:     make([]byte, PageSize),
		dirty:    true,
		pinCount: 1,
		manager:  pm,
	}

	// Zero out the page data
	for i := 0; i < PageSize; i++ {
		page.data[i] = 0
	}

	// Add to cache and pin it
	pm.pageCache[pageID] = page

	// Update the free page list
	err := pm.updateFreePageList()
	if err != nil {
		return nil, err
	}

	return page, nil
}

// GetPage retrieves a page by its ID, either from cache or from disk
func (pm *PageManager) GetPage(pageID PageID) (*Page, error) {
	if pageID >= PageID(pm.numPages) {
		return nil, fmt.Errorf("page ID %d out of range (max: %d)", pageID, pm.numPages-1)
	}

	// First check the cache
	pm.cacheMutex.RLock()
	page, exists := pm.pageCache[pageID]
	pm.cacheMutex.RUnlock()

	if exists {
		page.Pin()
		return page, nil
	}

	// Page not in cache, load from disk
	page, err := pm.loadPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	// Cache the page
	pm.cacheMutex.Lock()
	// Check again if another thread loaded the page while we were waiting
	if cachedPage, exists := pm.pageCache[pageID]; exists {
		// Use the already cached page
		pm.cacheMutex.Unlock()
		cachedPage.Pin()
		return cachedPage, nil
	}

	// Add to cache
	pm.pageCache[pageID] = page
	pm.cacheMutex.Unlock()

	return page, nil
}

// loadPageFromDisk loads a page from disk into memory
func (pm *PageManager) loadPageFromDisk(pageID PageID) (*Page, error) {
	offset := int64(pageID) * int64(PageSize)

	// Create a new page
	page := &Page{
		id:       pageID,
		data:     make([]byte, PageSize),
		dirty:    false,
		pinCount: 1,
		manager:  pm,
	}

	// Read page data from file
	_, err := pm.file.ReadAt(page.data, offset)
	if err != nil {
		// If we're reading past the end of file, that's an error
		if err == io.EOF {
			return nil, fmt.Errorf("page %d does not exist (beyond EOF)", pageID)
		}
		return nil, err
	}

	return page, nil
}

// FreePage marks a page as free for future reuse
func (pm *PageManager) FreePage(pageID PageID) error {
	pm.cacheMutex.Lock()
	defer pm.cacheMutex.Unlock()

	// Check if the page is in cache
	page, exists := pm.pageCache[pageID]
	if exists {
		// Can't free a pinned page
		if page.pinCount > 0 {
			return errors.New("cannot free a pinned page")
		}

		// Remove from cache
		delete(pm.pageCache, pageID)
	}

	// Add to free list
	pm.freePages = append(pm.freePages, pageID)

	// Update the free page list in the header
	return pm.updateFreePageList()
}

// FlushPage writes a page to disk if it's dirty
func (pm *PageManager) FlushPage(pageID PageID) error {
	pm.cacheMutex.RLock()
	page, exists := pm.pageCache[pageID]
	pm.cacheMutex.RUnlock()

	// If page not in cache or not dirty, nothing to do
	if !exists || !page.dirty {
		return nil
	}

	// Write the page to disk
	offset := int64(pageID) * int64(PageSize)
	_, err := pm.file.WriteAt(page.data, offset)
	if err != nil {
		return err
	}

	// Mark as clean
	page.dirty = false
	return nil
}

// FlushAllPages writes all dirty pages to disk
func (pm *PageManager) FlushAllPages() error {
	pm.cacheMutex.RLock()
	pagesToFlush := make([]PageID, 0, len(pm.pageCache))
	for pageID, page := range pm.pageCache {
		if page.dirty {
			pagesToFlush = append(pagesToFlush, pageID)
		}
	}
	pm.cacheMutex.RUnlock()

	// Flush each dirty page
	for _, pageID := range pagesToFlush {
		err := pm.FlushPage(pageID)
		if err != nil {
			return err
		}
	}

	return nil
}

// extendFile extends the file by adding the specified number of pages
func (pm *PageManager) extendFile(numAdditionalPages uint32) error {
	// Calculate new file size
	newFileSize := int64(pm.numPages) * int64(PageSize)

	// Extend the file
	err := pm.file.Truncate(newFileSize)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the page manager, flushing all dirty pages and closing the file
func (pm *PageManager) Close() error {
	// Flush all dirty pages to disk
	err := pm.FlushAllPages()
	if err != nil {
		return err
	}

	// Close the file
	return pm.file.Close()
}
