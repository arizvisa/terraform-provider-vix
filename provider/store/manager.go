package store

import (
    "os"
    "log"
    "fmt"
    "context"
    "io"
    "io/ioutil"
    "path/filepath"
    "errors"
)

type ManagerFileCloser = context.CancelFunc

type Manager interface {
    New(name string) (io.ReadWriteSeeker, ManagerFileCloser, error)
    Path() string
    Close() error
}

type FileManager struct {
    handle *os.File

    handles map[string]*os.File
}

func NewFileManager(f *os.File) *FileManager {
    return &FileManager{handle: f}
}

func (M *FileManager) __newFileCloser(name string) ManagerFileCloser {

    closer := func() {
        if f, ok := M.handles[name]; ok {
            delete(M.handles, name)
            f.Close()
        } else {
            log.Printf("[WARN] Unable to close already closed file: %s", name)
        }
    }

    return closer
}

func (M *FileManager) New(name string) (io.ReadWriteSeeker, ManagerFileCloser, error) {
    if f, ok := M.handles[name]; ok {
        return f, M.__newFileCloser(name), nil
    }

    path := filepath.Join(M.handle.Name(), name)
    if fi, err := os.Stat(path); !os.IsNotExist(err) {
        return nil, nil, fmt.Errorf("File %s already exists", fi.Name())
    }

    if f, err := os.Create(path); err != nil {
        return nil, nil, err

    } else {
        M.handles[name] = f
    }

    return M.handles[name], M.__newFileCloser(name), nil
}

func (M *FileManager) Open(name string) (io.ReadCloser, ManagerFileCloser, error) {
    if f, ok := M.handles[name]; ok {
        return f, M.__newFileCloser(name), nil
    }

    path := filepath.Join(M.handle.Name(), name)
    if fi, err := os.Stat(path); os.IsNotExist(err) {
        return nil, nil, fmt.Errorf("File %s does not exist", fi.Name())
    }

    if f, err := os.Open(path); err != nil {
        return nil, nil, err

    } else {
        M.handles[name] = f
    }

    return M.handles[name], M.__newFileCloser(name), nil
}

func (M *FileManager) Path() string {
    return M.handle.Name()
}

func (M *FileManager) Close() error {
    for name, f := range M.handles {
        if err := f.Close(); err != nil {
            log.Printf("[WARN] Unable to close handle for file %s: %v", name, err)
        }
    }
    return M.handle.Close()
}

func (M *FileManager) Add(f *os.File, name string) (io.ReadCloser, ManagerFileCloser, error) {
    if _, ok := M.handles[name]; ok {
        return nil, nil, errors.New("Unable to manage file with duplicate name")
    }

    if fi, err := f.Stat(); err != nil {
        return nil, nil, err

    } else if fi.IsDir() {
        return nil, nil, errors.New("Unable to manage a directory.")
    }

    path := filepath.Join(M.handle.Name(), name)

    if err := os.Link(f.Name(), path); err != nil {
        return nil, nil, fmt.Errorf("Unable to link %s into manager: %w", f.Name(), err)
    }

    if f, err := os.Open(path); err != nil {
        if err := os.Remove(path); err != nil {
            log.Printf("[WARN] Unable to remove linked file %s during error: %s", path, err)
        }
        return nil, nil, fmt.Errorf("Unable to open linked file: %w", err)

    } else {
        M.handles[name] = f
    }

    return M.handles[name], M.__newFileCloser(name), nil
}

type TemporaryFileManager struct {
    handle *os.File

    handles map[int]*os.File
}

func NewTemporaryFileManager(f *os.File) *TemporaryFileManager {
    return &TemporaryFileManager{handle: f}
}

func (M *TemporaryFileManager) New(name string) (io.ReadWriteSeeker, ManagerFileCloser, error) {
    f, err := ioutil.TempFile(M.handle.Name(), name)
    if err != nil {
        return nil, nil, err

    }

    index := 1 + len(M.handles)

    Closer := func() {
        if fh, ok := M.handles[index]; ok {
            delete(M.handles, index)
            path := fh.Name()

            if err := fh.Close(); err != nil {
                log.Printf("[WARN] Error closing temporary file #%d (%s): %v", index, path, err)
            }

            if err := os.Remove(path); err != nil {
                log.Printf("[ERROR] Error removing temporary file #%d (%s): %v", index, path, err)
            }
        } else {
            log.Printf("[WARN] Unable to locate temporary file #%d: %v", index, f.Name())
        }
    }

    M.handles[index] = f
    return f, Closer, nil
}

func (M *TemporaryFileManager) Path() string {
    return M.handle.Name()
}

func (M *TemporaryFileManager) Close() error {
    for _, f := range M.handles {
        path := f.Name()
        if err := f.Close(); err != nil {
            log.Printf("[WARN] Error closing temporary file %s: %v", path, err)
        }

        if err := os.Remove(path); err != nil {
            log.Printf("[ERROR] Error removing temporary file %s: %v", path, err)
        }
    }

    return M.handle.Close()
}

type CacheFile struct {
    path string

    handle *os.File
}

func NewCacheFile(path string) (*CacheFile, error) {
    if _, err := os.Stat(path); !os.IsNotExist(err) {
        return nil, errors.New("Cache file already exists")
    }
    return &CacheFile{path: path, handle: nil}, nil
}

func (cf *CacheFile) Unload() error {
    if cf.handle != nil {
        name := cf.handle.Name()
        if err := cf.handle.Close(); err != nil {
            log.Printf("[WARN] Unable to close handle for %s during refresh", name)
        }
    }
    cf.handle = nil
    return nil
}

func (cf *CacheFile) Load() error {
    if cf.handle != nil {
        return errors.New("Cache file is already loaded")
    }

    if f, err := os.Open(cf.path); os.IsNotExist(err) {
        return errors.New("Cache file is allocated but not initialized")

    } else if err != nil {
        return fmt.Errorf("Error opening cache file: %w", err)

    } else {
        if fi, err := f.Stat(); err != nil {
            return fmt.Errorf("Unable to get stats for file %s: %w", f.Name(), err)
        } else if fi.IsDir() {
            return errors.New("Cache file has been initialized with a directory, not a file")
        } else {
            cf.handle = f
        }
    }
    return nil
}

func (cf *CacheFile) Name() string {
    if cf.handle == nil {
        return cf.path
    }
    return cf.handle.Name()
}

func (cf *CacheFile) Use(f *os.File) error {
    if err := os.Link(f.Name(), cf.path); err != nil {
        return fmt.Errorf("Unable to link %s into cache at %s: %w", f.Name(), cf.path, err)
    }
    return cf.Load()
}

func (cf *CacheFile) Read(p []byte) (n int, err error) {
    if cf.handle == nil {
        return 0, errors.New("Cache file has not been loaded")
    }
    return cf.handle.Read(p)
}

func (cf *CacheFile) Write(p []byte) (n int, err error) {
    if cf.handle == nil {
        return 0, errors.New("Cache file has not been loaded")
    }
    return cf.handle.Write(p)
}

func (cf *CacheFile) Seek(offset int64, whence int) (int64, error) {
    if cf.handle == nil {
        return 0, errors.New("Cache file has not been loaded")
    }
    return cf.handle.Seek(offset, whence)
}

func (cf *CacheFile) Close() (err error) {
    var fi os.FileInfo

    if cf.handle != nil {
        fi, err = cf.handle.Stat()
        if cerr := cf.handle.Close(); cerr != nil {
            log.Printf("[WARN] Error closing cache file %s: %v", fi.Name(), err)
        }

    } else {
        fi, err = os.Stat(cf.path)
    }

    if os.IsNotExist(err) {
        return nil
    }

    if fi.IsDir() {
        return errors.New("Cache file is a directory and not a file")
    }

    return os.Remove(fi.Name())
}

type CacheFileManager struct {
    handle *os.File

    allocated map[string]*CacheFile
}

func NewCacheFileManager(f *os.File) *CacheFileManager {
    return &CacheFileManager{handle: f}
}

func (M *CacheFileManager) New(name string) (io.ReadWriteSeeker, ManagerFileCloser, error) {
    if f, ok := M.allocated[name]; ok {
        return f, M.__newFileCloser(name), nil
    }

    path := filepath.Join(M.handle.Name(), name)
    f, err := NewCacheFile(path)
    if err != nil {
        return nil, nil, err
    }
    M.allocated[name] = f

    return f, M.__newFileCloser(name), nil
}

func (M *CacheFileManager) Open(name string) (io.ReadCloser, ManagerFileCloser, error) {
    if f, ok := M.allocated[name]; ok {
        return f, M.__newFileCloser(name), nil
    }

    path := filepath.Join(M.handle.Name(), name)
    f, err := NewCacheFile(path)
    if err != nil {
        return nil, nil, err
    }
    M.allocated[name] = f

    return f, M.__newFileCloser(name), nil
}

func (M *CacheFileManager) Path() string {
    return M.handle.Name()
}

func (M *CacheFileManager) Close() error {
    for name, f := range M.allocated {
        if err := f.Close(); err != nil {
            log.Printf("[WARN] Unable to close cache file %s: %v", name, err)
        }
    }
    return M.handle.Close()
}

func (M *CacheFileManager) __newFileCloser(name string) ManagerFileCloser {
    return func() {
        if f, ok := M.allocated[name]; ok {
            delete(M.allocated, name)
            f.Close()
        } else {
            log.Printf("[WARN] Unable to close cache file: %s", name)
        }
    }
}
