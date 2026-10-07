#![cfg(all(unix, feature = "os-poll", feature = "os-ext", feature = "net"))]

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
#[cfg(not(target_os = "emscripten"))]
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[cfg(not(target_os = "emscripten"))]
use mio::unix::pipe::{Receiver, Sender};
use mio::unix::{pipe, SourceFd};
use mio::{Events, Interest, Poll, Token};

mod util;
use util::{assert_would_block, expect_events, expect_no_events, ExpectEvent};

const RECEIVER: Token = Token(0);
const SENDER: Token = Token(1);

const DATA1: &[u8; 11] = b"Hello world";

#[test]
fn source_fd_readable_after_would_block() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);
    let (sender, receiver) = pipe::new().unwrap();
    let mut sender = File::from(OwnedFd::from(sender));
    let mut receiver = File::from(OwnedFd::from(receiver));
    let fd = receiver.as_raw_fd();

    poll.registry()
        .register(&mut SourceFd(&fd), RECEIVER, Interest::READABLE)
        .unwrap();

    for _ in 0..3 {
        sender.write_all(b"x").unwrap();
        expect_events(
            &mut poll,
            &mut events,
            vec![ExpectEvent::new(RECEIVER, Interest::READABLE)],
        );

        let mut buf = [0; 1];
        receiver.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"x");
        assert_would_block(receiver.read(&mut buf));
        expect_no_events(&mut poll, &mut events);
    }

    poll.registry().deregister(&mut SourceFd(&fd)).unwrap();
    sender.write_all(b"x").unwrap();
    expect_no_events(&mut poll, &mut events);
}

#[test]
// Event ports suppress writable-only fallback events when polling with a timeout.
#[cfg(any(not(target_os = "solaris"), mio_unsupported_force_poll_poll))]
fn source_fd_writable_after_would_block() {
    use std::io;

    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);
    let (sender, receiver) = pipe::new().unwrap();
    let mut sender = File::from(OwnedFd::from(sender));
    let mut receiver = File::from(OwnedFd::from(receiver));
    let fd = sender.as_raw_fd();

    poll.registry()
        .register(&mut SourceFd(&fd), RECEIVER, Interest::READABLE)
        .unwrap();
    poll.registry()
        .reregister(&mut SourceFd(&fd), SENDER, Interest::WRITABLE)
        .unwrap();

    for _ in 0..3 {
        expect_events(
            &mut poll,
            &mut events,
            vec![ExpectEvent::new(SENDER, Interest::WRITABLE)],
        );

        let mut buf = [0; 4096];
        loop {
            match sender.write(&buf) {
                Ok(n) => assert!(n > 0),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("unexpected write error: {err}"),
            }
        }
        expect_no_events(&mut poll, &mut events);

        loop {
            match receiver.read(&mut buf) {
                Ok(n) => assert!(n > 0),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("unexpected read error: {err}"),
            }
        }
    }

    poll.registry().deregister(&mut SourceFd(&fd)).unwrap();
    expect_no_events(&mut poll, &mut events);
}

#[test]
fn smoke() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);

    let (mut sender, mut receiver) = pipe::new().unwrap();

    let mut buf = [0; 20];
    assert_would_block(receiver.read(&mut buf));

    poll.registry()
        .register(&mut receiver, RECEIVER, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut sender, SENDER, Interest::WRITABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(SENDER, Interest::WRITABLE)],
    );
    let n = sender.write(DATA1).unwrap();
    assert_eq!(n, DATA1.len());

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(RECEIVER, Interest::READABLE)],
    );
    let n = receiver.read(&mut buf).unwrap();
    assert_eq!(n, DATA1.len());
    assert_eq!(&buf[..n], DATA1);
}

#[test]
#[cfg_attr(
    any(target_os = "hurd", target_os = "nto", target_os = "cygwin"),
    ignore = "Writer fd close events do not trigger POLLHUP on nto and GNU/Hurd targets"
)]
fn event_when_sender_is_dropped() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);

    let (mut sender, mut receiver) = pipe::new().unwrap();
    poll.registry()
        .register(&mut receiver, RECEIVER, Interest::READABLE)
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_barrier = barrier.clone();

    let handle = thread::spawn(move || {
        let n = sender.write(DATA1).unwrap();
        assert_eq!(n, DATA1.len());
        thread_barrier.wait();

        thread_barrier.wait();
        drop(sender);
        thread_barrier.wait();
    });

    barrier.wait(); // Wait for the write to complete.
    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(RECEIVER, Interest::READABLE)],
    );

    barrier.wait(); // Unblock the thread.
    barrier.wait(); // Wait until the sending end is dropped.

    expect_one_closed_event(&mut poll, &mut events, RECEIVER, true);

    handle.join().unwrap();
}

#[test]
#[cfg_attr(
    any(target_os = "hurd", target_os = "nto", target_os = "cygwin"),
    ignore = "Writer fd close events do not trigger POLLHUP on nto and GNU/Hurd targets"
)]
fn event_when_receiver_is_dropped() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);

    let (mut sender, receiver) = pipe::new().unwrap();
    poll.registry()
        .register(&mut sender, SENDER, Interest::WRITABLE)
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let thread_barrier = barrier.clone();

    let handle = thread::spawn(move || {
        thread_barrier.wait();
        drop(receiver);
        thread_barrier.wait();
    });

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(SENDER, Interest::WRITABLE)],
    );

    barrier.wait(); // Unblock the thread.
    barrier.wait(); // Wait until the receiving end is dropped.

    expect_one_closed_event(&mut poll, &mut events, SENDER, false);

    handle.join().unwrap();
}

#[test]
// Emscripten/wasm has no fork/exec, so no subprocesses.
#[cfg(not(target_os = "emscripten"))]
#[cfg_attr(
    any(target_os = "hurd", target_os = "nto", target_os = "cygwin"),
    ignore = "Writer fd close events do not trigger POLLHUP on nto and GNU/Hurd targets"
)]
#[cfg_attr(miri, ignore = "Miri doesn't support process spawning")]
fn from_child_process_io() {
    // `cat` simply echo everything that we write via standard in.
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to start `cat` command");

    let mut sender = Sender::from(child.stdin.take().unwrap());
    let mut receiver = Receiver::from(child.stdout.take().unwrap());

    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(8);

    poll.registry()
        .register(&mut receiver, RECEIVER, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut sender, SENDER, Interest::WRITABLE)
        .unwrap();

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(SENDER, Interest::WRITABLE)],
    );
    let n = sender.write(DATA1).unwrap();
    assert_eq!(n, DATA1.len());

    expect_events(
        &mut poll,
        &mut events,
        vec![ExpectEvent::new(RECEIVER, Interest::READABLE)],
    );
    let mut buf = [0; 20];
    let n = receiver.read(&mut buf).unwrap();
    assert_eq!(n, DATA1.len());
    assert_eq!(&buf[..n], DATA1);

    drop(sender);

    expect_one_closed_event(&mut poll, &mut events, RECEIVER, true);

    child.wait().unwrap();
}

#[test]
#[cfg(not(target_os = "emscripten"))]
#[cfg_attr(miri, ignore = "Miri doesn't support process spawning")]
fn nonblocking_child_process_io() {
    // `cat` simply echo everything that we write via standard in.
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to start `cat` command");

    let sender = Sender::from(child.stdin.take().unwrap());
    let mut receiver = Receiver::from(child.stdout.take().unwrap());

    receiver.set_nonblocking(true).unwrap();

    let mut buf = [0; 20];
    assert_would_block(receiver.read(&mut buf));

    drop(sender);
    child.wait().unwrap();
}

/// Expected a closed event. If `read` is true is checks for `is_read_closed`,
/// otherwise for `is_write_closed`.
pub fn expect_one_closed_event(poll: &mut Poll, events: &mut Events, token: Token, read: bool) {
    poll.poll(events, Some(Duration::from_secs(1))).unwrap();
    let mut iter = events.iter();
    let event = iter.next().unwrap();
    assert_eq!(event.token(), token, "invalid token, event: {event:#?}");
    if read {
        assert!(
            event.is_read_closed(),
            "expected closed or error, event: {event:#?}",
        );
    } else {
        assert!(
            event.is_write_closed(),
            "expected closed or error, event: {event:#?}",
        );
    }
    assert!(iter.next().is_none());
}
