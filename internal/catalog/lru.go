package catalog

type lruCache struct {
	data    map[string]*cacheEntry
	maxSize int
}

type cacheEntry struct {
	value    interface{}
	lastUsed int64
}

func newLRUCache(maxSize int) *lruCache {
	return &lruCache{
		data:    make(map[string]*cacheEntry),
		maxSize: maxSize,
	}
}

func (c *lruCache) Get(key string) (interface{}, bool) {

	entry, exists := c.data[key]
	if !exists {
		return nil, false
	}

	entry.lastUsed++
	return entry.value, true
}

func (c *lruCache) Put(key string, value interface{}) {

	if len(c.data) >= c.maxSize {
		if _, exists := c.data[key]; !exists {
			c.evictOldest()
		}
	}

	c.data[key] = &cacheEntry{
		value:    value,
		lastUsed: 0,
	}
}

func (c *lruCache) evictOldest() {
	var oldestKey string
	var oldestTime int64 = -1

	for key, entry := range c.data {
		if oldestTime == -1 || entry.lastUsed < oldestTime {
			oldestTime = entry.lastUsed
			oldestKey = key
		}
	}

	if oldestKey != "" {
		delete(c.data, oldestKey)
	}
}

func (c *lruCache) Delete(key string) {

	delete(c.data, key)
}

func (c *lruCache) Clear() {

	c.data = make(map[string]*cacheEntry)
}

func (c *lruCache) Keys() []string {

	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}
