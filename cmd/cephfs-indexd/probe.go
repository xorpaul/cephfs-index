package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xorpaul/cephfs-index/internal/scan"
)

type dirRec struct {
	parent uint64
	name   string
}

// matchSink keeps every directory's (parent, name) in memory so matches can
// be printed with full paths. Memory grows with the directory count.
type matchSink struct {
	re       *regexp.Regexp
	typ      byte
	rootIno  uint64
	rootPath string

	mu      sync.Mutex
	dirs    map[uint64]dirRec
	out     *bufio.Writer
	matches int64
}

func (m *matchSink) Emit(batch []scan.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range batch {
		e := &batch[i]
		if e.Type == scan.TypeDir {
			m.dirs[e.Ino] = dirRec{parent: e.Parent, name: e.Name}
		}
		if m.typ != 0 && e.Type != m.typ {
			continue
		}
		if !m.re.MatchString(e.Name) {
			continue
		}
		m.matches++
		uid := strconv.FormatUint(uint64(e.UID), 10)
		if e.Type == scan.TypeHardlink {
			uid = "-"
		}
		fmt.Fprintf(m.out, "%s\t%s\n", uid, m.path(e.Parent, e.Name))
	}
	m.out.Flush()
}

func (m *matchSink) path(parent uint64, name string) string {
	parts := []string{name}
	for ino := parent; ino != m.rootIno; {
		d, ok := m.dirs[ino]
		if !ok {
			parts = append(parts, fmt.Sprintf("<ino 0x%x>", ino))
			break
		}
		parts = append(parts, d.name)
		ino = d.parent
	}
	var b strings.Builder
	b.WriteString(strings.TrimSuffix(m.rootPath, "/"))
	for i := len(parts) - 1; i >= 0; i-- {
		b.WriteByte('/')
		b.WriteString(parts[i])
	}
	return b.String()
}

type countSink struct{}

func (countSink) Emit([]scan.Entry) {}

var entryTypes = map[string]byte{"": 0, "file": scan.TypeFile, "dir": scan.TypeDir, "symlink": scan.TypeSymlink, "hardlink": scan.TypeHardlink}

func probe(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	var o walkOpts
	o.register(fs)
	duration := fs.Duration("duration", 0, "stop after this long (0 = walk the whole tree)")
	startIno := fs.String("start-ino", "1", "directory inode (hex) to start at; 1 = filesystem root")
	prefix := fs.String("prefix", "", "path printed for --start-ino (default / for the root, <ino> otherwise)")
	match := fs.String("match", "", "print uid<TAB>path for names matching this RE2 regex (keeps all dirs in memory)")
	typ := fs.String("type", "", "with --match: only 'file', 'dir', 'symlink' or 'hardlink'")
	_ = fs.Parse(args)

	if o.fsName == "" {
		fmt.Fprintln(os.Stderr, "--fs is required")
		return 2
	}
	start, err := strconv.ParseUint(strings.TrimPrefix(*startIno, "0x"), 16, 64)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad --start-ino:", err)
		return 2
	}

	var sink scan.Sink = countSink{}
	var ms *matchSink
	if *match != "" {
		re, err := regexp.Compile(*match)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad --match:", err)
			return 2
		}
		t, ok := entryTypes[*typ]
		if !ok {
			fmt.Fprintln(os.Stderr, "bad --type:", *typ)
			return 2
		}
		root := *prefix
		if root == "" {
			if start == scan.RootIno {
				root = "/"
			} else {
				root = fmt.Sprintf("<ino 0x%x>", start)
			}
		}
		ms = &matchSink{re: re, typ: t, rootIno: start, rootPath: root,
			dirs: map[uint64]dirRec{}, out: bufio.NewWriter(os.Stdout)}
		sink = ms
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s, err := o.open(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer s.close()
	if err := s.flushJournals(ctx, &o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	sc := s.scanner(&o, sink)
	s.header(&o, fmt.Sprintf(" start=0x%x", start))

	t0 := time.Now()
	report := func(final bool) {
		tag, matches := "", ""
		if final {
			tag = "done  "
		}
		if ms != nil {
			ms.mu.Lock()
			matches = fmt.Sprintf("  matches=%d", ms.matches)
			ms.mu.Unlock()
		}
		el := time.Since(t0)
		fmt.Fprintf(os.Stderr, "%s  %s%s%s  %s\n", ts(), tag, statsLine(sc, el), matches, el.Round(time.Second))
	}
	stop := every(o.progress, func() { report(false) })
	err = sc.Run(ctx, start)
	stop()
	report(true)
	if err != nil && ctx.Err() == nil {
		s.log.Error("scan", "err", err)
		return 1
	}
	if sc.Stats.DecodeErrors.Load() > 0 || sc.Stats.OpErrors.Load() > 0 {
		return 1
	}
	return 0
}
