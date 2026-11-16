package googlerecorder

import (
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/recordingupload"
)

type State = recordingupload.State

var OpenState = recordingupload.OpenState

func DefaultStateFile() string { return common.DefaultStateFile("google-recorder-upload-state.sqlite") }
