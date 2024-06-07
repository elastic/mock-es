package api

import (
	"maps"
	"sync"
)

// UserAgentTracker is a simple async hashmap that
// tracks the reported User-Agent strings
type UserAgentTracker struct {
	lock           sync.RWMutex
	userAgentsSeen UserAgentMaps
}

// UserAgentMaps track seen user agent strings across different API endpoints
// the hashmap takes the form of User-Agent:seen_count
type UserAgentMaps struct {
	root    map[string]int
	license map[string]int
	bulk    map[string]int
}

// NewUserAgentTracker returns a new tracker
func NewUserAgentTracker() *UserAgentTracker {
	return &UserAgentTracker{
		lock: sync.RWMutex{},
		userAgentsSeen: UserAgentMaps{
			root:    make(map[string]int),
			license: make(map[string]int),
			bulk:    make(map[string]int),
		},
	}
}

// RootSeen increments a counter for the / endpoint
func (uat *UserAgentTracker) RootSeen(agent string) {
	uat.lock.Lock()
	defer uat.lock.Unlock()
	uat.userAgentsSeen.root[agent]++
}

// BulkSeen increments a counter for the /_bulk endpoint
func (uat *UserAgentTracker) BulkSeen(agent string) {
	uat.lock.Lock()
	defer uat.lock.Unlock()
	uat.userAgentsSeen.bulk[agent]++
}

// BulkSeen increments a counter for the /license endpoint
func (uat *UserAgentTracker) LicenseSeen(agent string) {
	uat.lock.Lock()
	defer uat.lock.Unlock()
	uat.userAgentsSeen.license[agent]++
}

// Get returns all metrics for the User Agent
func (uat *UserAgentTracker) Get() UserAgentMaps {
	uat.lock.RLock()
	defer uat.lock.RUnlock()

	new := UserAgentMaps{
		license: maps.Clone(uat.userAgentsSeen.license),
		root:    maps.Clone(uat.userAgentsSeen.root),
		bulk:    maps.Clone(uat.userAgentsSeen.bulk),
	}
	return new
}
