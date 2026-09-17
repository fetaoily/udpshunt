package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestParseArgsVersionFlag(t *testing.T) {
	o, err := parseArgs([]string{"-v"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.showVersion {
		t.Fatal("-v did not set showVersion")
	}
	if o.cfgPath != "/etc/udpshunt/udpshunt.yaml" {
		t.Fatalf("cfgPath = %q, want default", o.cfgPath)
	}
}

func TestParseArgsConfigFlag(t *testing.T) {
	o, err := parseArgs([]string{"-c", "/tmp/other.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if o.showVersion {
		t.Fatal("showVersion set without -v")
	}
	if o.cfgPath != "/tmp/other.yaml" {
		t.Fatalf("cfgPath = %q, want /tmp/other.yaml", o.cfgPath)
	}
}

func TestParseArgsCombined(t *testing.T) {
	o, err := parseArgs([]string{"-c", "x.yaml", "-v"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.showVersion || o.cfgPath != "x.yaml" {
		t.Fatalf("opts = %+v, want both flags set", o)
	}
}

func TestParseArgsUnknownFlag(t *testing.T) {
	_, err := parseArgs([]string{"-nope"})
	if err == nil {
		t.Fatal("unknown flag accepted")
	}
}

func TestParseArgsHelpIsErrHelp(t *testing.T) {
	_, err := parseArgs([]string{"-h"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
}

func TestPrintVersion(t *testing.T) {
	if version == "" {
		t.Fatal("version is empty")
	}
	var buf bytes.Buffer
	printVersion(&buf)
	if !strings.Contains(buf.String(), version) {
		t.Fatalf("printVersion output %q missing version %q", buf.String(), version)
	}
}
