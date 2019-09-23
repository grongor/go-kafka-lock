//+build test

package kafkalock

import (
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jonboulle/clockwork"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/grongor/go-kafka-lock/kafkalock/mocks"
)

func TestFileFlapsStorage(t *testing.T) {
	tests := []struct {
		name  string
		times []time.Time
	}{
		{
			"single time",
			[]time.Time{time.Now().Truncate(-1)},
		},
		{
			"multiple times",
			[]time.Time{
				time.Now().Add(time.Hour * -4).Truncate(-1),
				time.Now().Truncate(-1),
				time.Now().Add(time.Hour * 5).Truncate(-1),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			file, _ := fs.OpenFile("some-file", os.O_RDWR|os.O_CREATE, 0600)
			storage := NewFileFlapsStorage(file)

			times, err := storage.Read()
			require.NoError(t, err)
			require.Empty(t, times)

			err = storage.Store(test.times)
			require.NoError(t, err)

			stat, _ := file.Stat()
			size := stat.Size()
			require.NotEmpty(t, size)

			storage.Close()

			file, err = fs.OpenFile("some-file", os.O_RDWR|os.O_EXCL, 0600)
			require.NoError(t, err)

			storage = NewFileFlapsStorage(file)
			times, err = storage.Read()
			require.NoError(t, err)
			require.Equal(t, test.times, times)

			stat, _ = file.Stat()
			require.Equal(t, size, stat.Size())
		})
	}
}

func TestFlapGuardError(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()
	clock = fakeClock

	delay := time.Second
	ctrl := gomock.NewController(t)
	storage := mocks.NewMockFlapsStorage(ctrl)
	guard, err := NewFlapGuard(NewNopLogger(), storage, 2, false)
	require.NoError(t, err)

	time1 := clock.Now()

	// first run, no flaps
	storage.EXPECT().Read().Times(1).Return(make([]time.Time, 0), nil)
	storage.EXPECT().Store([]time.Time{time1}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// second run, single flap
	storage.EXPECT().Store([]time.Time{time1, time1}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	fakeClock.Advance(time.Second * 2)
	time2 := clock.Now()

	// third run, single flap (this run is +2 seconds, so it's not considered as a flap)
	storage.EXPECT().Store([]time.Time{time1, time1, time2}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// forth run, two flaps (we tolerate two, so no error)
	storage.EXPECT().Store([]time.Time{time1, time1, time2, time2}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// fifth run, two flaps (first is gone, last just appeared - same situation as forth run)
	storage.EXPECT().Store([]time.Time{time1, time2, time2, time2}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// sixth run, three flaps: error is emitted as this run just exceeded our defined threshold
	storage.EXPECT().Store([]time.Time{time2, time2, time2, time2}).Times(1)
	err = guard.Check(delay)
	require.EqualError(t, err, ErrLockIsFlapping.Error())

	fakeClock.Advance(time.Second * 2)
	time3 := clock.Now()

	// seventh run, two flaps again (this run is +2 seconds, so not considered as a flap)
	storage.EXPECT().Store([]time.Time{time2, time2, time2, time3}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// eight run, two flaps (first is gone, last just appeared - same situation as seventh run)
	storage.EXPECT().Store([]time.Time{time2, time2, time3, time3}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	storage.EXPECT().Close().Times(1)
	err = guard.Close()
	require.NoError(t, err)
}

func TestFlapGuardWaiting(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()
	clock = fakeClock

	delay := time.Second
	ctrl := gomock.NewController(t)
	storage := mocks.NewMockFlapsStorage(ctrl)
	guard, err := NewFlapGuard(NewNopLogger(), storage, 0, true)
	require.NoError(t, err)

	time1 := clock.Now()

	// first run, no flaps
	storage.EXPECT().Read().Times(1).Return(make([]time.Time, 0), nil)
	storage.EXPECT().Store([]time.Time{time1}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)

	// second run, single flap
	storage.EXPECT().Store([]time.Time{time1, time1}).Times(1)
	var checkFinishedAt time.Time
	ch := async(func() {
		err = guard.Check(delay)
		checkFinishedAt = clock.Now()
	})

	fakeClock.BlockUntil(1)
	fakeClock.Advance(time.Second * 10)

	require.True(t, <-ch, "expected Check() to finish by now")
	require.NotEmpty(t, checkFinishedAt)
	require.True(t, checkFinishedAt.After(time1.Add(time.Second*2)))
	require.NoError(t, err)

	// third run, no flaps (first is gone)
	storage.EXPECT().Store([]time.Time{time1, clock.Now()}).Times(1)
	err = guard.Check(delay)
	require.NoError(t, err)
}
