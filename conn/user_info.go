package conn

import (
	"fmt"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"os"
	"os/user"
)

type UserInfo struct {
	loginName   string
	displayName string
	hostName    string
}

func NewUserInfo() *UserInfo {
	var newUserInfo UserInfo

	if current, err := user.Current(); err != nil {
		newUserInfo.loginName = fmt.Sprintf("unable to get login name: %s", err)
		newUserInfo.displayName = fmt.Sprintf("unable to get display name: %s", err)
	} else {
		newUserInfo.loginName = current.Username
		newUserInfo.displayName = current.Name
	}

	if host, err := os.Hostname(); err != nil {
		newUserInfo.hostName = fmt.Sprintf("unable to get host name: %s", err)
	} else {
		newUserInfo.hostName = host
	}

	return &newUserInfo
}

func WriteUserInfoToEncoder(encoder *ch_encoding.Encoder, info *UserInfo) error {
	var err error
	err = encoder.String(info.loginName)
	if err != nil {
		return err
	}
	err = encoder.String(info.displayName)
	if err != nil {
		return err
	}
	return encoder.String(info.hostName)
}
