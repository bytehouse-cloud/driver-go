package conn

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	mock_conn "github.com/bytehouse-cloud/driver-go/conn/mocks"
)

/**
 * this will test different operations of RefreshReader in sequential order
 * ensure that the behaviour is as expected and no goroutine leaks occured
 */
func Test_RefreshReader_SequentialOperations(t *testing.T) {
	type args struct {
		reset time.Duration
	}

	type testCase struct {
		name          string
		args          args
		mockedSetupFn func(reader *mock_conn.MockSetReadDeadlineReader) // set up all mocked behaviours
		flowSetupFn   func(refreshReader *RefreshReader)                // set up the flow execution in order with expected behaviours defined
	}

	var (
		fakedData1 = []byte("test_data1")
		fakedData2 = []byte("test_data2")

		fakedError                     = errors.New("test_error")
		readOnClosedRefreshReaderError = errors.New(readOnCloseRefreshReader)

		errorReadLength   = 0
		successReadLength = 1
		waitTime          = 3 * time.Second

		arguments = args{
			reset: time.Second,
		}
	)

	testCases := []testCase{
		{
			name: "IF RefreshReader is CLOSE THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				closeErr := refreshReader.Close()
				assert.Equal(t, nil, closeErr)

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF RefreshReader READ return error THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(gomock.Any()).Return(errorReadLength, fakedError)
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				readLen, err := refreshReader.Read(fakedData1)
				assert.Equal(t, fakedError, err)
				assert.Equal(t, errorReadLength, readLen)

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF RefreshReader CLOSE THEN no goroutines leak and next READ return readOnCloseChannel error",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				closedRErr := refreshReader.Close()
				assert.Equal(t, nil, closedRErr)

				readLen, readErr := refreshReader.Read(fakedData1)
				assert.Equal(t, errorReadLength, readLen)
				assert.Equal(t, readOnClosedRefreshReaderError, readErr)

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF RefreshReader CLOSE 2 times THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				closedErr := refreshReader.Close()
				assert.Equal(t, nil, closedErr)

				closedErr = refreshReader.Close()
				assert.Equal(t, nil, closedErr)

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF RefreshReader 1st READ return error THEN 2nd READ failed with readOnCloseChannel error",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, fakedError)
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				readLen, readErr := refreshReader.Read(fakedData1)
				assert.Equal(t, errorReadLength, readLen)
				assert.Equal(t, fakedError, readErr)

				readLen, readErr = refreshReader.Read(fakedData2)
				assert.Equal(t, errorReadLength, readLen)
				assert.Equal(t, readOnClosedRefreshReaderError, readErr)

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF RefreshReader is CLOSE after READ success THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(gomock.Any()).Return(successReadLength, nil)
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				readLen, readErr := refreshReader.Read(fakedData1)
				assert.Equal(t, successReadLength, readLen)
				assert.Equal(t, nil, readErr)

				closedRErr := refreshReader.Close()
				assert.Equal(t, nil, closedRErr)

				time.Sleep(waitTime)
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			internalReader := mock_conn.NewMockSetReadDeadlineReader(ctrl)
			tt.mockedSetupFn(internalReader)

			refreshReader := NewRefreshReader(internalReader, tt.args.reset)

			tt.flowSetupFn(refreshReader)
		})
	}
}

/**
 * this will test different operations of RefreshReader in concurrent order
 */
func Test_RefreshReader_ConcurrentOperations(t *testing.T) {
	type args struct {
		reset time.Duration
	}

	type testCase struct {
		name          string
		args          args
		mockedSetupFn func(reader *mock_conn.MockSetReadDeadlineReader) // set up all mocked behaviours
		flowSetupFn   func(refreshReader *RefreshReader)                // set up the flow execution in order with expected behaviours defined
	}

	var (
		fakedData1 = []byte("test_data1")
		fakedData2 = []byte("test_data2")

		fakedError = errors.New("test_error")
		//readOnClosedChannelError = errors.New(readOnCloseChannel)

		errorReadLength   = 0
		successReadLength = 1
		waitTime          = 3 * time.Second

		arguments = args{
			reset: time.Second,
		}
	)

	testCases := []testCase{
		{
			name: "IF 2 concurrent CLOSEs invoke on RefreshReader THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Close()
				go refreshReader.Close()

				time.Sleep(waitTime)
			},
		},
		{
			name: "IF 2 concurrent READs with one READ return error invoke on RefreshReader THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, fakedError).AnyTimes()
				reader.EXPECT().Read(fakedData2).Return(successReadLength, nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Read(fakedData1)
				go refreshReader.Read(fakedData2)

				time.Sleep(waitTime)
			},
		},
		// this test need some intervention in the code by adding time.Sleep() after the close.Load() check under Read() function
		{
			name: "IF concurrent (READ return no error) & CLOSE, CLOSE changed the r.closed to true first THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Read(fakedData1)
				go refreshReader.Close()

				time.Sleep(waitTime)
			},
		},
		// this test need some intervention in the code by adding time.Sleep() after the close.Load() check under Read() function
		{
			name: "IF concurrent (READ return error) & CLOSE, CLOSE changed the r.closed to true first THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, fakedError).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Read(fakedData1)
				go refreshReader.Close()

				time.Sleep(waitTime)
			},
		},
		// this test need some intervention in the code by adding time.Sleep() after the close.Load() check under Close() function
		{
			name: "IF concurrent (READ return no error) & CLOSE, READ changed the r.closed to true first THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Read(fakedData1)
				go refreshReader.Close()

				time.Sleep(waitTime)
			},
		},
		// this test need some intervention in the code by adding time.Sleep() after the close.Load() check under Close() function
		{
			name: "IF concurrent (READ return error) & CLOSE, READ changed the r.closed to true first THEN no goroutines leak",
			args: arguments,
			mockedSetupFn: func(reader *mock_conn.MockSetReadDeadlineReader) {
				reader.EXPECT().SetReadDeadline(gomock.Any()).Return(nil).AnyTimes()
				reader.EXPECT().Read(fakedData1).Return(errorReadLength, nil).AnyTimes()
			},
			flowSetupFn: func(refreshReader *RefreshReader) {
				go refreshReader.Read(fakedData1)
				go refreshReader.Close()

				time.Sleep(waitTime)
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			internalReader := mock_conn.NewMockSetReadDeadlineReader(ctrl)
			tt.mockedSetupFn(internalReader)

			refreshReader := NewRefreshReader(internalReader, tt.args.reset)

			tt.flowSetupFn(refreshReader)
		})
	}
}
