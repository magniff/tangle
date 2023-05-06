# Tangle 🧶
WIP implementation of the [NSQ](https://nsq.io/) message queue in Rust.

# Tests
Integration tests are all written in Python and are using [ansq](https://github.com/list-family/ansq) library. Have a looksy at their [example](https://github.com/list-family/ansq#consumer). That's on purpose, although at this point the server is fairly compatible with any client implementation out there. To run the test suite:

```bash
$ DOCKER_BUILDKIT=1 ./tests/runtests.sh ./tests
```

# Current status
This repo is WIP, at this point in time:
- [x] Implement the core features from the [protocol](https://nsq.io/clients/tcp_protocol_spec.html)
- [x] Implement the in-memory queue
- [ ] Implement the durable (disk backed queue)
- [ ] Implement observability - statsd integration and HTTP API

# Performance
~~Blazingly fast~~ I am working on it, as for right now it performs 10-15% better than the `Go` version, at least in terms of the round trip latency. I am pretty sure `tangle` can do much better than that, stay tuned.
