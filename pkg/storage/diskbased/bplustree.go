package diskbased

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// NodeTypeNonLeaf is the type of internal nodes
	NodeTypeNonLeaf byte = 0
	// NodeTypeLeaf is the type of leaf nodes
	NodeTypeLeaf byte = 1

	// MaxKeysPerNode is the maximum number of keys allowed per B+ tree node
	// In practice, this should be calculated based on page size and key size
	MaxKeysPerNode = 128

	// NodeHeaderSize is the size of the node header in bytes
	NodeHeaderSize = 9 // 1 byte for node type + 4 bytes for number of keys + 4 bytes for next page
)

// BPlusTree is a B+ tree implementation that stores data on disk
type BPlusTree struct {
	pageManager *PageManager
	rootPageID  PageID
	order       int // Maximum number of children per node
}

// NewBPlusTree creates a new B+ tree
func NewBPlusTree(pageManager *PageManager, rootPageID PageID) (*BPlusTree, error) {
	tree := &BPlusTree{
		pageManager: pageManager,
		rootPageID:  rootPageID,
		order:       MaxKeysPerNode,
	}

	// Check if this is a new tree
	rootPage, err := pageManager.GetPage(rootPageID)
	if err != nil {
		return nil, err
	}

	if rootPage.IsDirty() {
		// This is a new tree, initialize root as leaf node
		data := rootPage.Data()
		data[0] = NodeTypeLeaf                      // Node type
		binary.LittleEndian.PutUint32(data[1:5], 0) // Number of keys
		binary.LittleEndian.PutUint32(data[5:9], 0) // Next page
	}

	return tree, nil
}

// CreateNewTree creates a new B+ tree with a new root page
func CreateNewTree(pageManager *PageManager) (*BPlusTree, error) {
	// Allocate a new page for the root
	rootPage, err := pageManager.AllocatePage()
	if err != nil {
		return nil, err
	}

	// Initialize as leaf node
	data := rootPage.Data()
	data[0] = NodeTypeLeaf                      // Node type
	binary.LittleEndian.PutUint32(data[1:5], 0) // Number of keys
	binary.LittleEndian.PutUint32(data[5:9], 0) // Next page

	return NewBPlusTree(pageManager, rootPage.ID())
}

// Insert inserts a key-value pair into the B+ tree
func (t *BPlusTree) Insert(key []byte, value []byte) error {
	return t.insert(t.rootPageID, key, value, nil)
}

// insert recursively inserts a key-value pair into the tree
func (t *BPlusTree) insert(nodeID PageID, key []byte, value []byte, parentID *PageID) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	nodeData := node.Data()
	nodeType := nodeData[0]
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	switch nodeType {
	case NodeTypeLeaf:
		// Check if key already exists
		keys, _, err := t.getLeafNodeEntries(nodeID)
		if err != nil {
			return err
		}

		// Check for duplicate key
		for i, existingKey := range keys {
			if bytes.Equal(existingKey, key) {
				// Update existing key
				index := NodeHeaderSize + (i * (len(existingKey) + 8)) + 4 + len(existingKey)
				binary.LittleEndian.PutUint32(nodeData[index:index+4], uint32(len(value)))
				copy(nodeData[index+4:index+4+len(value)], value)
				node.MarkDirty()
				return nil
			}
		}

		// If node is not full, insert key here
		if numKeys < uint32(t.order-1) {
			err = t.insertIntoLeaf(nodeID, key, value)
			return err
		}

		// Node is full, need to split
		return t.splitLeafNode(nodeID, key, value, parentID)

	case NodeTypeNonLeaf:
		// Find the appropriate child
		childID, err := t.findChildNode(nodeID, key)
		if err != nil {
			return err
		}

		// Recursively insert into the child node
		return t.insert(childID, key, value, &nodeID)
	}

	return fmt.Errorf("unknown node type: %d", nodeType)
}

// Get retrieves a value from the B+ tree by key
func (t *BPlusTree) Get(key []byte) ([]byte, error) {
	// Start search from root
	leafNodeID, err := t.findLeafNode(t.rootPageID, key)
	if err != nil {
		return nil, err
	}

	// Search in the leaf node
	node, err := t.pageManager.GetPage(leafNodeID)
	if err != nil {
		return nil, err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	// Scan through keys
	offset := NodeHeaderSize
	for i := uint32(0); i < numKeys; i++ {
		keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		currentKey := nodeData[offset : offset+int(keyLen)]
		offset += int(keyLen)

		valueLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		if bytes.Equal(currentKey, key) {
			// Found the key
			return nodeData[offset : offset+int(valueLen)], nil
		}

		offset += int(valueLen)
	}

	return nil, errors.New("key not found")
}

// Delete removes a key-value pair from the B+ tree
func (t *BPlusTree) Delete(key []byte) error {
	// Find the leaf node containing the key
	leafNodeID, err := t.findLeafNode(t.rootPageID, key)
	if err != nil {
		return err
	}

	// Delete the key from the leaf node
	return t.deleteFromLeaf(leafNodeID, key)
}

// FindRange finds all values with keys in the specified range
func (t *BPlusTree) FindRange(startKey, endKey []byte) ([][]byte, error) {
	results := [][]byte{}

	// Find the leaf node containing the start key
	leafNodeID, err := t.findLeafNode(t.rootPageID, startKey)
	if err != nil {
		return nil, err
	}

	for leafNodeID != 0 {
		node, err := t.pageManager.GetPage(leafNodeID)
		if err != nil {
			return nil, err
		}

		nodeData := node.Data()
		numKeys := binary.LittleEndian.Uint32(nodeData[1:5])
		nextNodeID := binary.LittleEndian.Uint32(nodeData[5:9])

		// Scan through keys
		offset := NodeHeaderSize
		for i := uint32(0); i < numKeys; i++ {
			keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
			offset += 4

			currentKey := nodeData[offset : offset+int(keyLen)]
			offset += int(keyLen)

			valueLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
			offset += 4

			// Check if key is in range
			if bytes.Compare(currentKey, startKey) >= 0 &&
				(endKey == nil || bytes.Compare(currentKey, endKey) <= 0) {
				results = append(results, nodeData[offset:offset+int(valueLen)])
			}

			// If we've gone past the end of range, we're done
			if endKey != nil && bytes.Compare(currentKey, endKey) > 0 {
				return results, nil
			}

			offset += int(valueLen)
		}

		// Move to the next leaf node
		leafNodeID = PageID(nextNodeID)
	}

	return results, nil
}

// Helper functions

// findLeafNode finds the leaf node that should contain the key
func (t *BPlusTree) findLeafNode(nodeID PageID, key []byte) (PageID, error) {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return 0, err
	}

	nodeData := node.Data()
	nodeType := nodeData[0]

	if nodeType == NodeTypeLeaf {
		return nodeID, nil
	}

	// This is an internal node, find the correct child
	childID, err := t.findChildNode(nodeID, key)
	if err != nil {
		return 0, err
	}

	// Recursively search in the child node
	return t.findLeafNode(childID, key)
}

// findChildNode finds the appropriate child node for a key in a non-leaf node
func (t *BPlusTree) findChildNode(nodeID PageID, key []byte) (PageID, error) {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return 0, err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	// Structure of internal node entry: child_ptr, key, child_ptr, key, ..., child_ptr

	// If key is less than the first key, go to first child
	if numKeys == 0 {
		// Special case for empty root node
		return PageID(binary.LittleEndian.Uint32(nodeData[NodeHeaderSize : NodeHeaderSize+4])), nil
	}

	offset := NodeHeaderSize
	firstChildID := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
	offset += 4

	for i := uint32(0); i < numKeys; i++ {
		keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		currentKey := nodeData[offset : offset+int(keyLen)]
		offset += int(keyLen)

		if bytes.Compare(key, currentKey) < 0 {
			if i == 0 {
				return PageID(firstChildID), nil
			}
			// Go to child before this key
			return PageID(binary.LittleEndian.Uint32(nodeData[offset-int(keyLen)-8 : offset-int(keyLen)-4])), nil
		}

		// After last key, get the last child pointer
		if i == numKeys-1 {
			return PageID(binary.LittleEndian.Uint32(nodeData[offset : offset+4])), nil
		}

		// Skip next child pointer for now
		offset += 4
	}

	// Key is greater than all keys in node, return last child
	return PageID(binary.LittleEndian.Uint32(nodeData[offset-4 : offset])), nil
}

// getLeafNodeEntries gets the keys and values from a leaf node
func (t *BPlusTree) getLeafNodeEntries(nodeID PageID) ([][]byte, [][]byte, error) {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return nil, nil, err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)

	offset := NodeHeaderSize
	for i := uint32(0); i < numKeys; i++ {
		keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		keys[i] = append([]byte{}, nodeData[offset:offset+int(keyLen)]...)
		offset += int(keyLen)

		valueLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		values[i] = append([]byte{}, nodeData[offset:offset+int(valueLen)]...)
		offset += int(valueLen)
	}

	return keys, values, nil
}

// insertIntoLeaf inserts a key-value pair into a leaf node
func (t *BPlusTree) insertIntoLeaf(nodeID PageID, key []byte, value []byte) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	// Find insertion position (keep keys sorted)
	keys, values, err := t.getLeafNodeEntries(nodeID)
	if err != nil {
		return err
	}

	insertPosition := numKeys
	for i := uint32(0); i < numKeys; i++ {
		if bytes.Compare(key, keys[i]) < 0 {
			insertPosition = i
			break
		}
	}

	// Insert the new key-value pair
	newKeys := make([][]byte, numKeys+1)
	newValues := make([][]byte, numKeys+1)

	copy(newKeys[:insertPosition], keys[:insertPosition])
	copy(newValues[:insertPosition], values[:insertPosition])

	newKeys[insertPosition] = key
	newValues[insertPosition] = value

	copy(newKeys[insertPosition+1:], keys[insertPosition:])
	copy(newValues[insertPosition+1:], values[insertPosition:])

	// Write back to page
	node.MarkDirty()
	offset := NodeHeaderSize

	binary.LittleEndian.PutUint32(nodeData[1:5], numKeys+1) // Update number of keys

	for i := uint32(0); i < numKeys+1; i++ {
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])

		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newValues[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newValues[i])], newValues[i])
		offset += len(newValues[i])
	}

	return nil
}

// splitLeafNode splits a leaf node when it becomes full
func (t *BPlusTree) splitLeafNode(nodeID PageID, newKey []byte, newValue []byte, parentID *PageID) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	// Get all keys and values from the node, including the new one
	keys, values, err := t.getLeafNodeEntries(nodeID)
	if err != nil {
		return err
	}

	// Find where to insert the new key
	insertPosition := len(keys)
	for i := 0; i < len(keys); i++ {
		if bytes.Compare(newKey, keys[i]) < 0 {
			insertPosition = i
			break
		}
	}

	// Insert the new key and value
	newKeys := make([][]byte, len(keys)+1)
	newValues := make([][]byte, len(values)+1)

	copy(newKeys[:insertPosition], keys[:insertPosition])
	copy(newValues[:insertPosition], values[:insertPosition])

	newKeys[insertPosition] = newKey
	newValues[insertPosition] = newValue

	copy(newKeys[insertPosition+1:], keys[insertPosition:])
	copy(newValues[insertPosition+1:], values[insertPosition:])

	// Allocate a new page for the second half of the keys
	newNode, err := t.pageManager.AllocatePage()
	if err != nil {
		return err
	}

	newNodeID := newNode.ID()
	newNodeData := newNode.Data()

	// Setup new leaf node
	newNodeData[0] = NodeTypeLeaf

	// Split point - middle of the keys
	splitPoint := len(newKeys) / 2

	// Update the original node to contain only the first half
	nodeData := node.Data()
	binary.LittleEndian.PutUint32(nodeData[1:5], uint32(splitPoint)) // Update number of keys

	// Get the next node pointer from the original node and set it for the new node
	nextNodeID := binary.LittleEndian.Uint32(nodeData[5:9])
	binary.LittleEndian.PutUint32(newNodeData[5:9], nextNodeID)

	// Update the next pointer of the original node to point to the new node
	binary.LittleEndian.PutUint32(nodeData[5:9], uint32(newNodeID))

	// Write first half to original node
	offset := NodeHeaderSize
	for i := 0; i < splitPoint; i++ {
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4
		copy(nodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newValues[i])))
		offset += 4
		copy(nodeData[offset:offset+len(newValues[i])], newValues[i])
		offset += len(newValues[i])
	}

	// Write second half to new node
	binary.LittleEndian.PutUint32(newNodeData[1:5], uint32(len(newKeys)-splitPoint)) // Number of keys
	offset = NodeHeaderSize
	for i := splitPoint; i < len(newKeys); i++ {
		binary.LittleEndian.PutUint32(newNodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4
		copy(newNodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])
		binary.LittleEndian.PutUint32(newNodeData[offset:offset+4], uint32(len(newValues[i])))
		offset += 4
		copy(newNodeData[offset:offset+len(newValues[i])], newValues[i])
		offset += len(newValues[i])
	}

	// Mark both nodes as dirty
	node.MarkDirty()
	newNode.MarkDirty()

	// Update parent node or create new root if necessary
	splitKey := newKeys[splitPoint] // The first key of the second node is the split key

	if parentID == nil {
		// This was the root node, create new root
		return t.createNewRoot(nodeID, newNodeID, splitKey)
	} else {
		// Add split key to parent
		return t.insertIntoParent(*parentID, nodeID, newNodeID, splitKey)
	}
}

// createNewRoot creates a new root node when the current root splits
func (t *BPlusTree) createNewRoot(leftChildID, rightChildID PageID, key []byte) error {
	// Create a new page for the root
	rootPage, err := t.pageManager.AllocatePage()
	if err != nil {
		return err
	}

	// Setup as non-leaf node
	rootData := rootPage.Data()
	rootData[0] = NodeTypeNonLeaf                   // Node type
	binary.LittleEndian.PutUint32(rootData[1:5], 1) // One key

	// Structure: left_child_ptr, key, right_child_ptr
	offset := NodeHeaderSize

	// Write left child pointer
	binary.LittleEndian.PutUint32(rootData[offset:offset+4], uint32(leftChildID))
	offset += 4

	// Write key
	binary.LittleEndian.PutUint32(rootData[offset:offset+4], uint32(len(key)))
	offset += 4
	copy(rootData[offset:offset+len(key)], key)
	offset += len(key)

	// Write right child pointer
	binary.LittleEndian.PutUint32(rootData[offset:offset+4], uint32(rightChildID))

	// Update the tree's root pageID
	t.rootPageID = rootPage.ID()
	rootPage.MarkDirty()

	return nil
}

// insertIntoParent inserts a key and child pointer into a non-leaf node
func (t *BPlusTree) insertIntoParent(parentID, leftChildID, rightChildID PageID, key []byte) error {
	parent, err := t.pageManager.GetPage(parentID)
	if err != nil {
		return err
	}

	parentData := parent.Data()
	numKeys := binary.LittleEndian.Uint32(parentData[1:5])

	// If parent has space, insert the key
	if numKeys < uint32(t.order-1) {
		return t.insertIntoNonLeaf(parentID, rightChildID, key)
	}

	// Parent is full, need to split
	return t.splitNonLeafNode(parentID, rightChildID, key, nil)
}

// insertIntoNonLeaf inserts a key and right child pointer into a non-leaf node
func (t *BPlusTree) insertIntoNonLeaf(nodeID, rightChildID PageID, key []byte) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])
	node.MarkDirty()

	// Extract all keys and child pointers
	keys := make([][]byte, numKeys)
	childPtrs := make([]uint32, numKeys+1)

	offset := NodeHeaderSize
	childPtrs[0] = binary.LittleEndian.Uint32(nodeData[offset : offset+4])
	offset += 4

	for i := uint32(0); i < numKeys; i++ {
		keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		keys[i] = make([]byte, keyLen)
		copy(keys[i], nodeData[offset:offset+int(keyLen)])
		offset += int(keyLen)

		childPtrs[i+1] = binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4
	}

	// Find insertion position for the new key
	insertPos := numKeys
	for i := uint32(0); i < numKeys; i++ {
		if bytes.Compare(key, keys[i]) < 0 {
			insertPos = i
			break
		}
	}

	// Make room for the new key and pointer
	newKeys := make([][]byte, numKeys+1)
	newChildPtrs := make([]uint32, numKeys+2)

	copy(newKeys[:insertPos], keys[:insertPos])
	copy(newChildPtrs[:insertPos+1], childPtrs[:insertPos+1])

	newKeys[insertPos] = key
	newChildPtrs[insertPos+1] = uint32(rightChildID)

	copy(newKeys[insertPos+1:], keys[insertPos:])
	copy(newChildPtrs[insertPos+2:], childPtrs[insertPos+1:])

	// Write back to the page
	binary.LittleEndian.PutUint32(nodeData[1:5], numKeys+1) // Update the number of keys

	offset = NodeHeaderSize
	binary.LittleEndian.PutUint32(nodeData[offset:offset+4], newChildPtrs[0])
	offset += 4

	for i := uint32(0); i < numKeys+1; i++ {
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])

		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], newChildPtrs[i+1])
		offset += 4
	}

	return nil
}

// splitNonLeafNode splits a non-leaf node when it becomes full
func (t *BPlusTree) splitNonLeafNode(nodeID, newChildID PageID, newKey []byte, parentID *PageID) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	// Extract all keys and child pointers
	keys := make([][]byte, numKeys)
	childPtrs := make([]uint32, numKeys+1)

	offset := NodeHeaderSize
	childPtrs[0] = binary.LittleEndian.Uint32(nodeData[offset : offset+4])
	offset += 4

	for i := uint32(0); i < numKeys; i++ {
		keyLen := binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4

		keys[i] = make([]byte, keyLen)
		copy(keys[i], nodeData[offset:offset+int(keyLen)])
		offset += int(keyLen)

		childPtrs[i+1] = binary.LittleEndian.Uint32(nodeData[offset : offset+4])
		offset += 4
	}

	// Find insertion position for the new key
	insertPos := numKeys
	for i := uint32(0); i < numKeys; i++ {
		if bytes.Compare(newKey, keys[i]) < 0 {
			insertPos = i
			break
		}
	}

	// Insert new key and child pointer
	newKeys := make([][]byte, numKeys+1)
	newChildPtrs := make([]uint32, numKeys+2)

	copy(newKeys[:insertPos], keys[:insertPos])
	copy(newChildPtrs[:insertPos+1], childPtrs[:insertPos+1])

	newKeys[insertPos] = newKey
	newChildPtrs[insertPos+1] = uint32(newChildID)

	copy(newKeys[insertPos+1:], keys[insertPos:])
	copy(newChildPtrs[insertPos+2:], childPtrs[insertPos+1:])

	// Create new node for the split
	newNode, err := t.pageManager.AllocatePage()
	if err != nil {
		return err
	}
	newNodeData := newNode.Data()
	newNode.MarkDirty()

	// Set up new non-leaf node
	newNodeData[0] = NodeTypeNonLeaf

	// Split point - middle of the keys
	splitPoint := (len(newKeys)) / 2

	// The middle key will go up to the parent
	middleKey := newKeys[splitPoint]

	// Update the original node to contain only the first half
	binary.LittleEndian.PutUint32(nodeData[1:5], uint32(splitPoint))

	// Write first half to original node
	offset = NodeHeaderSize
	binary.LittleEndian.PutUint32(nodeData[offset:offset+4], newChildPtrs[0])
	offset += 4

	for i := 0; i < splitPoint; i++ {
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])

		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], newChildPtrs[i+1])
		offset += 4
	}

	// Write second half to new node (skip the middle key that goes up)
	binary.LittleEndian.PutUint32(newNodeData[1:5], uint32(len(newKeys)-(splitPoint+1)))

	offset = NodeHeaderSize
	binary.LittleEndian.PutUint32(newNodeData[offset:offset+4], newChildPtrs[splitPoint+1])
	offset += 4

	for i := splitPoint + 1; i < len(newKeys); i++ {
		binary.LittleEndian.PutUint32(newNodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4

		copy(newNodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])

		binary.LittleEndian.PutUint32(newNodeData[offset:offset+4], newChildPtrs[i+1])
		offset += 4
	}

	node.MarkDirty()

	// Update parent or create new root
	if parentID == nil {
		// This was the root node, create a new root
		return t.createNewRoot(nodeID, newNode.ID(), middleKey)
	} else {
		// Add middle key to parent
		return t.insertIntoParent(*parentID, nodeID, newNode.ID(), middleKey)
	}
}

// deleteFromLeaf deletes a key from a leaf node
func (t *BPlusTree) deleteFromLeaf(nodeID PageID, key []byte) error {
	node, err := t.pageManager.GetPage(nodeID)
	if err != nil {
		return err
	}

	nodeData := node.Data()
	numKeys := binary.LittleEndian.Uint32(nodeData[1:5])

	// Get all keys and values
	keys, values, err := t.getLeafNodeEntries(nodeID)
	if err != nil {
		return err
	}

	// Find the key position
	keyPosition := -1
	for i, existingKey := range keys {
		if bytes.Equal(existingKey, key) {
			keyPosition = i
			break
		}
	}

	if keyPosition == -1 {
		return errors.New("key not found")
	}

	// Remove the key-value pair
	newKeys := make([][]byte, numKeys-1)
	newValues := make([][]byte, numKeys-1)

	copy(newKeys[:keyPosition], keys[:keyPosition])
	copy(newValues[:keyPosition], values[:keyPosition])

	copy(newKeys[keyPosition:], keys[keyPosition+1:])
	copy(newValues[keyPosition:], values[keyPosition+1:])

	// Write back to the page
	binary.LittleEndian.PutUint32(nodeData[1:5], numKeys-1) // Update number of keys

	offset := NodeHeaderSize
	for i := uint32(0); i < numKeys-1; i++ {
		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newKeys[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newKeys[i])], newKeys[i])
		offset += len(newKeys[i])

		binary.LittleEndian.PutUint32(nodeData[offset:offset+4], uint32(len(newValues[i])))
		offset += 4

		copy(nodeData[offset:offset+len(newValues[i])], newValues[i])
		offset += len(newValues[i])
	}

	node.MarkDirty()
	return nil
}

// Close flushes all dirty pages to disk
func (t *BPlusTree) Close() error {
	return t.pageManager.FlushAllPages()
}
