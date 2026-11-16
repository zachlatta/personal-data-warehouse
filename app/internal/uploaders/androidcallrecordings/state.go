package androidcallrecordings

import (
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/recordingupload"
)

type State = recordingupload.State

var OpenState = recordingupload.OpenState

func DefaultStateFile() string {
	return common.DefaultStateFile("android-call-recordings-upload-state.sqlite")
}
