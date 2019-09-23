package kafkalock

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/afero"
)

// ErrLockIsFlapping is returned by FlapGuard if it determined that application is flapping.
var ErrLockIsFlapping = errors.New("app is flapping; locking KafkaLocker not allowed")

// FlapGuard is a utility class that protects the KafkaLocker from one fatal drawback: if there isn't an instance of
// KafkaLocker that is currently holding a lock and one of the other instances starts flapping for some reason
// (any unexpected issue that's causing the application to call KafkaLocker.Lock() too often), then none of the other
// KafkaLocker instances will be able to get a lock (because Kafka will trigger rebalancing process too often).
//
// Check out NewFlapGuard to see how FlapGuard prevents this issue. Default implementation uses filesystem as storage
// and is created via NewFileFlapGuard. flapDurationMax is slightly larger than Kafka's configuration option
// Consumer.Group.Session.Timeout (duration after which one of the KafkaLocker instances can safely acquire the lock).
//
// For testing purposes, or if you can guarantee that flapping will not happen (e.g. via supervisor config), there is
// a no-op FlapGuard, which can be created via NewNopFlapGuard. If you secure your application via e.g. supervisor but
// still want to be protected against the above issue during the runtime then you can use no-op storage created
// via NewNopFlapsStorage. FlapGuard will remember all the events during the runtime but won't persist them
// in between multiple application runs.
type FlapGuard interface {
	Check(flapDurationMax time.Duration) error
	io.Closer
}

type pastNFlapGuard struct {
	logger       *Logger
	storage      FlapsStorage
	allowedFlaps int
	wait         bool
	store        int
	times        []time.Time
}

func (f *pastNFlapGuard) Check(flapDurationMax time.Duration) error {
	var err error
	if f.times == nil {
		f.times, err = f.storage.Read()
		if err != nil {
			return err
		}

		f.logger.Debugw("loaded past KafkaLocker lock request times", "times", f.times)
	}

	f.add()

	err = f.storage.Store(f.times)
	if err != nil {
		return err
	}

	f.logger.Debugw("saved KafkaLocker lock request times", "times", f.times)

	err = f.doCheck(flapDurationMax)
	if err != nil && f.wait {
		waitTime := flapDurationMax*2 + time.Duration(rand.Intn(int(flapDurationMax*2)))
		f.logger.Warnw("FlapGuard detected that application is flapping", "waitTime", waitTime)
		<-clock.After(waitTime)

		return nil
	}

	return err
}

func (f *pastNFlapGuard) Close() error {
	return f.storage.Close()
}

func (f *pastNFlapGuard) add() {
	f.times = append(f.times, clock.Now().UTC())
	if len(f.times) > f.store {
		times := make([]time.Time, f.store)
		copy(times, f.times[len(f.times)-f.store:])
		f.times = times
	}
}

func (f *pastNFlapGuard) doCheck(flapDurationMax time.Duration) error {
	flaps := 0
	for i, t := range f.times {
		if len(f.times) == i+1 {
			break
		}

		if f.times[i+1].Sub(t) <= flapDurationMax {
			flaps++
		}
	}

	f.logger.Debugw("checking if application is flapping", "allowedFlaps", f.allowedFlaps, "flaps", flaps)

	if flaps > f.allowedFlaps {
		return ErrLockIsFlapping
	}

	return nil
}

var _ FlapGuard = (*pastNFlapGuard)(nil)

// NewFlapGuard creates new FlapGuard with a custom storage.
//
// FlapGuard will remember time of every Check() call and will check if it's not called too often (application
// is "flapping"). Call to Check() is considered as "flap" if it is made in duration less then
// flapDurationMax from the last call.
//
// allowedFlaps is the maximum number of flaps that will be tolerated. If allowedFlaps is exceeded then depending
// on waitInsteadError the FlapGuard will either return the ErrLockIsFlapping error immediately or wait until
// flapDurationMax duration passed since last flap and then return nil error.
//
// Check out NewFileFlapGuard for default/simple storage implementation.
func NewFlapGuard(logger *Logger, storage FlapsStorage, allowedFlaps int, waitInsteadError bool) (FlapGuard, error) {
	if allowedFlaps < 0 {
		return nil, errors.New("allowedFlaps must be greater or equal to 0")
	}

	return &pastNFlapGuard{
		logger:       logger,
		storage:      storage,
		allowedFlaps: allowedFlaps,
		wait:         waitInsteadError,
		store:        allowedFlaps + 2,
	}, nil
}

// NewFileFlapGuard creates new FlapGuard with a storage that uses filesystem to store the data. This is just a shortcut for:
//  kafkalock.NewFlapGuard(kafkalock.NewFileFlapsStorage(file), allowedFlaps, waitInsteadError)
func NewFileFlapGuard(logger *Logger, path string, allowedFlaps int, waitInsteadError bool) (FlapGuard, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return NewFlapGuard(logger, NewFileFlapsStorage(file), allowedFlaps, waitInsteadError)
}

type nopFlapGuard struct{}

func (nopFlapGuard) Check(time.Duration) error {
	return nil
}

func (nopFlapGuard) Close() error {
	return nil
}

var _ FlapGuard = (*nopFlapGuard)(nil)

// NewNopFlapGuard creates new no-op FlapGuard. Use at your own risk: lock instances might get stuck.
// For more information check FlapGuard documentation.
func NewNopFlapGuard() FlapGuard {
	return &nopFlapGuard{}
}

// FlapsStorage is an interface for storage adapters that provide information about application flaps.
// You can either provide your own or use one of the defaults: file-based or no-op one.
//
// File-based storage can be created via NewFileFlapsStorage; it uses *os.File to store the information.
//
// No-op storage is created by NewNopFlapsStorage and doesn't really persist anything: use at your own risk (see
// FlapGuard documentation for more information).
type FlapsStorage interface {
	Read() ([]time.Time, error)
	Store([]time.Time) error
	io.Closer
}

type fileFlapsStorage struct {
	file afero.File
}

func (f *fileFlapsStorage) Read() ([]time.Time, error) {
	stat, err := f.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to Stat() file: %w", err)
	}

	times := make([]time.Time, 0)
	if stat.Size() == 0 {
		return times, nil
	}

	err = gob.NewDecoder(f.file).Decode(&times)
	if err != nil {
		return nil, fmt.Errorf("failed to decode data: %w", err)
	}

	return times, nil
}

func (f *fileFlapsStorage) Store(times []time.Time) error {
	err := f.file.Truncate(0)
	if err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	_, err = f.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	err = gob.NewEncoder(f.file).Encode(times)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	err = f.file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync data: %w", err)
	}

	return nil
}

func (f *fileFlapsStorage) Close() error {
	return f.file.Close()
}

var _ FlapsStorage = (*fileFlapsStorage)(nil)

// NewFileFlapsStorage creates new FlapsStorage that stores the data inside a gob-encoded file.
func NewFileFlapsStorage(file afero.File) FlapsStorage {
	return &fileFlapsStorage{file}
}

type nopFlapsStorage struct{}

func (r *nopFlapsStorage) Read() ([]time.Time, error) {
	return make([]time.Time, 0), nil
}

func (r nopFlapsStorage) Store([]time.Time) error {
	return nil
}

func (r nopFlapsStorage) Close() error {
	return nil
}

var _ FlapsStorage = (*nopFlapsStorage)(nil)

// NewNopFlapsStorage creates new no-op storage. This "storage" won't help FlapGuard keep track of the application state between
// application restarts, but the FlapGuard will still be able to recognise flaps during the KafkaLocker instance
// existence (it keeps the the same data passed to Store() during runtime).
//
// Use at your own risk: lock instances might get stuck. For more information check FlapGuard documentation.
func NewNopFlapsStorage() FlapsStorage {
	return &nopFlapsStorage{}
}
