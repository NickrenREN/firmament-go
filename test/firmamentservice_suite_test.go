package test_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFirmamentservice(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Firmamentservice Suite")
}
