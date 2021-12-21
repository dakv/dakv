prog :=xnixperms

debug ?=

$(info debug is $(debug))

ifdef debug
  release :=
  target :=debug
  extension :=debug
else
  release :=--release
  target :=release
  extension :=
endif

build:
	cargo build $(release)

install:
	cp target/$(target)/$(prog) ~/bin/$(prog)-$(extension)

all: build install perf coverage

help:
	@echo "usage: make $(prog) [debug=1]"

perf:
	cargo run --example perf --release

coverage:
	docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" -v "/Users/da/.cargo/registry:/usr/local/cargo/registry"  xd009642/tarpaulin