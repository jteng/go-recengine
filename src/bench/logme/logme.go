package logme

import (
	"github.com/golang/glog"
	"flag"
)

func Logme(){
	flag.Parse()
	glog.V(1).Infof("hi logs")

}
