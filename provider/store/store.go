package store

import (
    "log"
    "os"
    "fmt"
    "path/filepath"
    "io/ioutil"
    "errors"
)

type ExitScopeFunc func() error

type Store struct {
    base string

    handle *os.File
    topics map[string]*os.File
}

func NewStore(workingDirectory string) (*Store, error) {
    if fi, err := os.Stat(workingDirectory); !os.IsNotExist(err) {
        return nil, err

    } else if os.IsNotExist(err) {
        if err := os.MkdirAll(workingDirectory, 0775); err != nil {
            return nil, fmt.Errorf("Unable to create work directory: %w", err)
        }

    } else if !fi.IsDir() {
        return nil, errors.New("Specified path already exists and is not a directory")
    }

    // Create our store object that the user asked for, and open up the directory
    // so we can retain a handle to it.
    store := Store{base: workingDirectory}

    if fh, err := os.Open(store.base); os.IsNotExist(err) {
        return nil, fmt.Errorf("Unable to lock work directory: %w", err)

    } else {
        store.handle = fh
    }

    // Open the default topics
    if err := store.__openTopics(); err != nil {
        store.Close()
        return nil, err
    }

    // Should be ready to use.
    return &store, nil
}

func (store *Store) Close() error {
    for topic, handle := range store.topics {
        if err := handle.Close(); err != nil {
            log.Printf("[WARN] Error closing storage topic %s: %w", topic, err)
        }
    }

    if err := store.handle.Close(); err != nil {
        log.Printf("[WARN] There was an issue trying to close the local storage: %v", err)
        return err

    } else {
        return nil
    }
}

func (store *Store) __openTopics() error {
    var results []error

    var defaults = []string{
        "cache",
        "images",
        "gold",
        "tmp",
    }

    for _, topic := range defaults {
        path := filepath.Join(store.base, topic)

        if fi, err := os.Stat(path); os.IsNotExist(err) {
            if err := os.Mkdir(filepath.Join(store.base, topic), 0); err != nil {
                results = append(results, fmt.Errorf("Unable to initialize store topic %s: %w", topic, err))
                continue
            }

        } else if err != nil {
            results = append(results, fmt.Errorf("Unable to initialize store topic %s: %w", topic, err))
            continue

        } else if !fi.IsDir() {
            results = append(results, fmt.Errorf("Unable to initialize store topic %s: %w", topic, err))
            continue
        }

        if err := os.Chmod(path, 0775); err != nil {
            results = append(results, fmt.Errorf("Unable to set permissions on store topic %s: %w", topic, err))
            continue

        }

        if fh, err := os.Open(path); err != nil {
            results = append(results, fmt.Errorf("Unable to open store topic %s: %w", topic, err))
        } else {
            store.topics[topic] = fh
        }
    }

    // If there were no errors, then we can return cleanly
    if len(results) == 0 {
        return nil
    }

    // Otherwise, we need to collect all the errors so we can log them
    for _, E := range results {
        log.Printf("[ERROR] %v", E)
    }

    // Figure out which topics we were unable to open
    failed := []string{}
    for _, topic := range defaults {
        if _, ok := store.topics[topic]; !ok {
            failed = append(failed, topic)
        }
    }

    // Return them to the caller
    one_error_to_rule_them_all := fmt.Errorf("Error opening the following topics: %v", failed)
    return one_error_to_rule_them_all
}

func (store *Store) TemporaryDir(prefix string) (mgr Manager, err error, closer ExitScopeFunc) {
    var temporaryPath string
    DummyExitScope := func() error {
        return nil
    }

    base := filepath.Join(store.base, store.topics["tmp"].Name())
    temporaryPath, err = ioutil.TempDir(base, prefix)
    if err != nil {
        return nil, err, DummyExitScope
    }

    if f, err := os.Open(temporaryPath); err != nil {
        return nil, err, DummyExitScope
    } else {
        mgr = NewTemporaryFileManager(f)
    }

    closer = func() error {
        if err := mgr.Close(); err != nil {
            log.Printf("[ERROR] Unable to close file manager: %v", err)
        }
        return os.RemoveAll(temporaryPath)
    }

    return mgr, nil, closer
}
