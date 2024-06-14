/*
 *	!!!!		WARNING		!		WARNING	!		WARNING	!
 *
 * 	!	DO NOT USE THIS MMAP ! USE ANYTHING ELSE !
 *
 *	!	THIS MMAP PLUGIN HAS ALL SYNC.MUTEX DISABLED !
 *
 * !	WE USE IT AS: 'MAPREGION PER SINGLE WRITER' FASHION !
 *
 * !	WE DONT EVEN KNOW IF IT REALLY WORKS ... SAFELY ! TRIAL + ERROR !
 *
 * ! WORK IN PROGRESS !
 *
 * 	THIS MMAP IS MIX OF SEVERAL MMAP IMPLEMENTATIONS OUT THERE.
 *
 * 	CREDITS TO : github.com/edsrzf/mmap-go
 */

// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file defines the common package interface and contains a little bit of
// factored out logic.

// Package mmap allows mapping files into memory. It tries to provide a simple, reasonably portable interface,
// but doesn't go out of its way to abstract away every little platform detail.
// This specifically means:
//   - forked processes may or may not inherit mappings
//   - a file's timestamp may or may not be updated by writes through mappings
//   - specifying a size larger than the file's actual size can increase the file's size
//   - If the mapped file is being modified by another process while your program's running, don't expect consistent results between platforms
package mmap

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	//"sync"
	"golang.org/x/sys/unix"
	"reflect"
	"unsafe"
)

const (
	// RDONLY maps the memory read-only.
	// Attempts to write to the MMap object will result in undefined behavior.
	RDONLY = 0
	// RDWR maps the memory as read-write. Writes to the MMap object will update the
	// underlying file.
	RDWR = 1 << iota
	// COPY maps the memory as copy-on-write. Writes to the MMap object will affect
	// memory, but the underlying file will remain unchanged.
	COPY
	// If EXEC is set, the mapped memory is marked as executable.
	EXEC
)

const (
	// If the ANON flag is set, the mapped memory will not be backed by a file.
	ANON = 1 << iota
)

// mmap implements Map.
type mmap struct {
	//mux              sync.RWMutex
	fd               uintptr
	flags, prot, len int
	offset           int64
	anon             bool
	data             []byte
	ptr              int
	write            bool
}

// Map represents a mapped file in memory and implements the io.ReadWriteCloser/io.Seeker/io.ReaderAt interfaces.
// Note that any change to the []byte returned by various methods is changing the underlying memory representation
// for all users of this mmap data.  For safety reasons or when using concurrent access, use the built in methods
// to read and write the data
type Map interface {
	io.ReadWriteCloser
	io.Seeker
	io.ReaderAt

	// Bytes returns the bytes in the map. Modifying this slice modifies the inmemory representation.
	// This data should only be used as read-only and instead you should use the Write() method that offers
	// better protections.  Write() will protect you from faults like writing to a read-only mmap or other
	// errors that cannot be caught via defer/recover. It also protects you from writing data that cannot
	// be sync'd to disk because the underlying file does not have the capactiy (regardless to what you
	// set the mmap length to).
	Bytes() []byte

	// Len returns the size of the file, which can be larger than the memory mapped area.
	Len() int

	// Pos returns the current index of the file pointer.
	Pos() int

	// Lock prevents the physical memory from being swapped out to disk.
	Lock() error

	// Unlock allows the physical memory to be swapped out to disk. If the memory is not locked, nothing happens.
	Unlock() error

	Flush() error

	Unmap() error

	Close() error
}

// String provides methods for working with the mmaped file as a UTF-8 text file and retrieving data as a string.
type String interface {
	// Embedded Map gives access to all Map methods and satisfies io.ReadWriteCloser/io.Seeker/io.ReaderAt.
	Map

	// Readline returns each line of text, stripped of any trailing end-of-line marker. The returned line may be empty.
	// The end-of-line marker is one optional carriage return followed by one mandatory newline.
	// In regular expression notation, it is `\r?\n`. The last non-empty line of input will be returned even if it has no newline.
	ReadLine() (string, error)

	// Write writes the string to the internal data starting at the current offset.  It moves the internal offset pointer.
	// If the data would go over the file length, no data is written and an error is returned.
	WriteString(string) (int, error)

	// String() returns the entire mmap'd data as a string.
	String() string
}

// NewMap creates a new Map object that provides methods for interacting with the mmap'd file.
func NewMap(f *os.File, length int, prot int, flags int, offset int64) (*mmap, error) {
	return mapRegion(f, length, prot, flags, offset)
}

// MapRegion maps part of a file into memory.
// The offset parameter must be a multiple of the system's page size.
// If length < 0, the entire file will be mapped.
// If ANON is set in flags, f is ignored.
func mapRegion(f *os.File, length int, prot int, flags int, offset int64) (*mmap, error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}
	m := &mmap{
		flags:  flags,
		prot:   prot,
		len:    length,
		offset: offset,
	}
	var fd uintptr
	if flags&ANON == 0 {
		fd = uintptr(f.Fd())
		if length < 0 {
			fi, err := f.Stat()
			if err != nil {
				return nil, err
			}
			length = int(fi.Size())
		}
	} else {
		if length <= 0 {
			return nil, errors.New("anonymous mapping requires non-zero length")
		}
		fd = ^uintptr(0)
	}
	var err error
	m.data, err = mmapMount(length, uintptr(prot), uintptr(flags), fd, offset)
	if err != nil {
		return nil, fmt.Errorf("problem with mmap system call: %q", err)
	}
	m.fd = fd
	return m, nil
}

func mmapMount(len int, inprot, inflags, fd uintptr, off int64) ([]byte, error) {
	flags := unix.MAP_SHARED
	prot := unix.PROT_READ
	switch {
	case inprot&COPY != 0:
		prot |= unix.PROT_WRITE
		flags = unix.MAP_PRIVATE
	case inprot&RDWR != 0:
		prot |= unix.PROT_WRITE
	}
	if inprot&EXEC != 0 {
		prot |= unix.PROT_EXEC
	}
	if inflags&ANON != 0 {
		flags |= unix.MAP_ANON
	}

	b, err := unix.Mmap(int(fd), off, len, prot, flags)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Bytes implements Map.Bytes().
func (m *mmap) Bytes() []byte {
	//m.mux.RLock()
	//defer m.mux.RUnlock()
	return m.data
}

// Len returns the size of the file, which can be larger than the memory mapped area.
func (m *mmap) Len() int {
	return m.len
}

// Read implements io.Reader.Read().
func (m *mmap) Read(p []byte) (int, error) {
	//m.mux.RLock()
	//defer m.mux.RUnlock()

	if m.ptr >= m.len {
		return 0, io.EOF
	}

	//log.Printf("INFO mmap.Read before len(p)=%d m.ptr=%d", len(p), m.ptr)
	n := copy(p, m.data[m.ptr:])
	m.ptr += n
	//log.Printf("INFO mmap.Read copied len(p)=%d m.ptr=%d n=%d", len(p), m.ptr, n)

	if n == m.len-m.ptr {
		//log.Printf("INFO mmap.Read reached end m.ptr=%d n=%d m.len=%d", m.ptr, n, m.len)
		return n, nil
	}
	return n, nil
}

// ReadAt implements ReaderAt.ReadAt().
func (m *mmap) ReadAt(p []byte, off int64) (n int, err error) {
	log.Printf("mmap.ReadAt p=%d offset=%d", len(p), off)
	//m.mux.RLock()
	//defer m.mux.RUnlock()

	if int(off) >= m.len {
		return 0, fmt.Errorf("offset is larger than the mmap []byte")
	}

	n = copy(p, m.data[off:])
	if n < len(p) {
		return n, fmt.Errorf("len(p) was greater than mmap[off:]")
	}
	return n, nil
}

// Write implements io.Writer.Write().
func (m *mmap) Write(p []byte) (n int, err error) {
	//m.mux.Lock()
	//defer m.mux.Unlock()

	if len(p) > m.len-m.ptr {
		log.Printf("ERROR mmap.Write len(p)=%d > m.len=%d - m.ptr=%d", len(p), m.len, m.ptr)
		return 0, fmt.Errorf("attempting to write past the end of the mmap'd file")
	}

	//log.Printf("INFO mmap.Write before len(p)=%d m.ptr=%d", len(p), m.ptr)
	n = copy(m.data[m.ptr:], p)
	//log.Printf("INFO mmap.Write copied len(p)=%d m.ptr=%d n=%d", len(p), m.ptr, n)
	m.ptr += n
	//log.Printf("INFO mmap.Write setptr len(p)=%d m.ptr=%d n=%d", len(p), m.ptr, n)
	return n, nil
}

// Seek implements io.Seeker.Seek().
func (m *mmap) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		log.Printf("ERROR Seek")
		return 0, fmt.Errorf("cannot seek to a negative offset")
	}

	//m.mux.Lock()
	//defer m.mux.Unlock()

	switch whence {
	case 0:
		// jump to offset
		if offset < int64(m.len) {
			m.ptr = int(offset)
			//log.Printf("Seek jump to m.ptr=%d offset=%d", m.ptr, offset)
			return int64(m.ptr), nil
		}
		return 0, fmt.Errorf("offset not in data size")
	case 1:
		// incr ptr by offset
		if m.ptr+int(offset) < m.len {
			m.ptr += int(offset)
			//log.Printf("Seek incr to m.ptr=%d offset=%d", m.ptr, offset)
			return int64(m.ptr), nil
		}
		return 0, fmt.Errorf("offset goes beyond the data size")
	case 2:
		// decr ptr by offset
		if m.ptr-int(offset) > -1 {
			m.ptr -= int(offset)
			//log.Printf("Seek decr to m.ptr=%d offset=%d", m.ptr, offset)
			return int64(m.ptr), nil
		}
		return 0, fmt.Errorf("error negative offset reached")
	}
	return 0, fmt.Errorf("whence arg was not set to a valid value")
}

// Pos implements Map.Pos().
func (m *mmap) Pos() int {
	//m.mux.RLock()
	pos := m.ptr
	//m.mux.RUnlock()
	return pos
}

// Unmap implements Map.Unmap().
// Unmap deletes the memory mapped region, flushes any remaining changes, and sets
// m to nil.
// Trying to read or write any remaining references to m after Unmap is called will
// result in undefined behavior.
// Unmap should only be called on the slice value that was originally returned from
// a call to Map. Calling Unmap on a derived slice may cause errors.
func (m *mmap) Unmap() error {
	//m.mux.Lock()
	err := unix.Munmap(m.data)
	var clear mmap
	*m = clear
	//m.mux.Unlock()
	return err
}

// Lock implements Map.Lock().
// Lock keeps the mapped region in physical memory, ensuring that it will not be
// swapped out.
func (m *mmap) Lock() error {
	//m.mux.Lock()
	mlock := unix.Mlock(m.data)
	//m.mux.Unlock()
	return mlock
}

// Unlock implements Map.Unlock().
// Unlock reverses the effect of Lock, allowing the mapped region to potentially
// be swapped out.
// If m is already unlocked, aan error will result.
func (m *mmap) Unlock() error {
	//m.mux.Lock()
	munlock := unix.Munlock(m.data)
	//m.mux.Unlock()
	return munlock
}

// Close implements Map.Close().
func (m *mmap) Close() error {
	//log.Printf("mmap.Close()")
	//m.mux.Lock()
	err := unix.Close(int(m.fd))
	var clear mmap
	*m = clear
	//m.mux.Unlock()
	return err
}

// Flush implements Map.Flush().
// Flush synchronizes the mapping's contents to the file's contents on disk.
func (m *mmap) Flush() error {
	//m.mux.Lock()
	retsync := unix.Msync(m.data, unix.MS_SYNC)
	//m.mux.Unlock()
	return retsync
}


func (m *mmap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

func (m *mmap) addrLen() (uintptr, uintptr) {
	header := m.header()
	return header.Data, uintptr(header.Len)
}




