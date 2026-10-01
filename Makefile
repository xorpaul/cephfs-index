# cephfs-dentry-decode is pure Go (static, runs anywhere).
# cephfs-indexd links librados (cgo): build it on a host whose glibc/librados
# match the target (the mon/admin node), with librados-devel installed.
# cephfs-search needs cgo for SQLite, but not librados.
# To build against an extracted RPM tree instead, set SYSROOT=/path/to/root.

GO      ?= go
SYSROOT ?=

ifneq ($(SYSROOT),)
export CGO_CFLAGS  := -I$(SYSROOT)/usr/include
export CGO_LDFLAGS := -L$(SYSROOT)/usr/lib64 -Wl,-rpath-link,$(SYSROOT)/usr/lib64/ceph -Wl,-rpath-link,$(SYSROOT)/usr/lib64
endif

.PHONY: all decode indexd search test vet clean

all: decode indexd search

decode:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w' -o bin/cephfs-dentry-decode ./cmd/cephfs-dentry-decode

indexd:
	$(GO) build -trimpath -o bin/cephfs-indexd ./cmd/cephfs-indexd

search:
	$(GO) build -trimpath -o bin/cephfs-search ./cmd/cephfs-search

test:
	$(GO) test -race ./internal/decode ./internal/scan ./internal/index ./internal/pgindex

vet:
	$(GO) vet ./...

clean:
	rm -rf bin
