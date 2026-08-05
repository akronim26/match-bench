// stall-sink — the B4 stalled-peer contestant: accepts connections, drains
// normally until STALL_AFTER_BYTES per connection, then STOPS READING while
// keeping the connection open. The kernel receive buffer fills, the load
// generator's send buffer fills behind it, and every subsequent bot write
// hits EAGAIN forever. This is the wedged-engine failure mode: unlike the
// echo (reads but never replies — exercises only the response-timeout path)
// and unlike drain-sink (always reads — exercises nothing), this forces the
// WRITE path: the drain-deadline write exit, the watchdog last-tick pending
// sweep, and the offered == matched + timed_out accounting invariant.
//
// STALL_AFTER_BYTES=0 never reads at all (accept-and-wedge immediately).
// Never writes a byte in any mode. Listens on 9898 (FIX) and 8080 (HTTP/WS)
// so it can stand in for a contestant on either protocol.
package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
)

const soReusePort = 0xf // SO_REUSEPORT (Linux)

var stallAfter = func() int64 {
	if v := os.Getenv("STALL_AFTER_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			fmt.Fprintf(os.Stderr, "bad STALL_AFTER_BYTES %q\n", v)
			os.Exit(2)
		}
		return n
	}
	return 256 * 1024 // drain a little first so the run is mid-flight when it wedges
}()

func main() {
	fmt.Printf("stall-sink: STALL_AFTER_BYTES=%d, listening on 9898+8080\n", stallAfter)
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	for i := 1; i < n; i++ {
		go reactor()
	}
	reactor()
}

func listen(port int) int {
	lfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		panic(err)
	}
	_ = syscall.SetsockoptInt(lfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	_ = syscall.SetsockoptInt(lfd, syscall.SOL_SOCKET, soReusePort, 1)
	if err := syscall.Bind(lfd, &syscall.SockaddrInet4{Port: port}); err != nil {
		panic(err)
	}
	if err := syscall.Listen(lfd, 1024); err != nil {
		panic(err)
	}
	return lfd
}

func reactor() {
	runtime.LockOSThread()

	listeners := map[int]bool{listen(9898): true, listen(8080): true}
	ep, err := syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	for lfd := range listeners {
		addFD(ep, lfd)
	}

	read := map[int]int64{} // fd -> bytes drained so far; absent = not ours
	events := make([]syscall.EpollEvent, 512)
	buf := make([]byte, 1<<16)

	for {
		nev, err := syscall.EpollWait(ep, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			panic(err)
		}
		for i := 0; i < nev; i++ {
			fd := int(events[i].Fd)
			if listeners[fd] {
				acceptAll(ep, fd, read)
				continue
			}
			got, ours := read[fd]
			if !ours {
				continue
			}
			n, alive := drain(fd, buf, stallAfter-got)
			read[fd] = got + n
			if !alive {
				syscall.EpollCtl(ep, syscall.EPOLL_CTL_DEL, fd, nil)
				syscall.Close(fd)
				delete(read, fd)
				continue
			}
			if read[fd] >= stallAfter {
				// The stall: stop watching the fd but KEEP IT OPEN. From the
				// peer's view the connection is healthy; it just never drains.
				syscall.EpollCtl(ep, syscall.EPOLL_CTL_DEL, fd, nil)
				fmt.Printf("stall-sink: fd %d STALLED after %d bytes\n", fd, read[fd])
			}
		}
	}
}

func addFD(ep, fd int) {
	_ = syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN, Fd: int32(fd),
	})
}

func acceptAll(ep, lfd int, read map[int]int64) {
	for {
		cfd, _, err := syscall.Accept4(lfd, syscall.SOCK_NONBLOCK)
		if err != nil {
			return // EAGAIN
		}
		_ = syscall.SetsockoptInt(cfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		if stallAfter == 0 {
			// Wedge immediately: connected, never registered for reads.
			fmt.Printf("stall-sink: fd %d accepted and immediately stalled\n", cfd)
			read[cfd] = 0
			continue
		}
		addFD(ep, cfd)
		read[cfd] = 0
	}
}

// drain reads and discards up to `budget` bytes. Returns (bytes read, alive).
// alive=false means the peer closed or a hard error occurred.
func drain(fd int, buf []byte, budget int64) (int64, bool) {
	var total int64
	for total < budget {
		want := int64(len(buf))
		if r := budget - total; r < want {
			want = r
		}
		nr, err := syscall.Read(fd, buf[:want])
		if err == syscall.EAGAIN {
			return total, true
		}
		if nr == 0 || (err != nil && err != syscall.EINTR) {
			return total, false
		}
		if nr > 0 {
			total += int64(nr)
		}
	}
	return total, true
}
