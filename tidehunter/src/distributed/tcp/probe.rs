//! Explicit isolated framing probe. No WAL, fsync, index, coordinator queue or
//! cluster-performance claim. Kept ignored so ordinary correctness tests do not
//! turn timings into flaky pass/fail assertions.
use super::*;
use std::hint::black_box;

struct CountIo {
    inner: DeadlineIo,
    reads: usize,
    writes: usize,
}
impl Read for CountIo {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reads += 1;
        self.inner.read(buffer)
    }
}
impl Write for CountIo {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        self.inner.write(buffer)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.writes += 1;
        self.inner.write_vectored(buffers)
    }
}

fn payload(bytes: usize, count: usize) -> Response {
    Response::Records(
        (0..count)
            .map(|index| vec![index as u8; bytes].into())
            .collect(),
    )
}

#[test]
#[ignore = "isolated release microprobe; run explicitly without concurrent builds/workloads"]
fn profile_tcp_primitives() {
    let variant =
        std::env::var("TIDEHUNTER_TCP_PROBE_VARIANT").unwrap_or_else(|_| "unspecified".into());
    let temporary = tempdir::TempDir::new("tcp-framing-probe").unwrap();
    let tls = tests::certificates(temporary.path());
    println!(
        "stage,variant,round,value_bytes,records,iterations,total_ns,p50_ns,p95_ns,client_socket_writes_per_op,client_socket_reads_per_op"
    );
    for round in 0..5 {
        for (bytes, count) in [
            (256, 1),
            (1024, 1),
            (8192, 1),
            (256, 64),
            (1024, 64),
            (8192, 64),
        ] {
            let mut response = reactor::Envelope {
                protocol: PROTOCOL,
                id: 1,
                body: payload(bytes, count),
            };
            let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&response).unwrap();
            let iterations = (8 * 1024 * 1024 / (bytes * count)).clamp(32, 10_000);
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(
                    wire_codec(HARD_FRAME_LIMIT)
                        .serialize(black_box(&response))
                        .unwrap(),
                );
            }
            println!(
                "encode,{variant},{round},{bytes},{count},{iterations},{},0,0,0,0",
                start.elapsed().as_nanos()
            );
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(
                    wire_codec(HARD_FRAME_LIMIT)
                        .deserialize::<reactor::Envelope<Response>>(black_box(&encoded))
                        .unwrap(),
                );
            }
            println!(
                "decode,{variant},{round},{bytes},{count},{iterations},{},0,0,0,0",
                start.elapsed().as_nanos()
            );
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap();
            let config = tls.server_config().unwrap();
            let network_iterations = 200;
            let server = thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                let mut io = DeadlineIo::new(socket, Duration::from_secs(60)).unwrap();
                let mut conn = ServerConnection::new(config.config).unwrap();
                conn.set_buffer_limit(Some(HARD_FRAME_LIMIT));
                while conn.is_handshaking() {
                    conn.complete_io(&mut io).unwrap();
                }
                let mut stream = StreamOwned::new(conn, io);
                for _ in 0..network_iterations + 10 {
                    let request: reactor::Envelope<Request> =
                        read_frame(&mut stream, HARD_FRAME_LIMIT).unwrap();
                    assert_eq!(request.protocol, PROTOCOL);
                    assert!(matches!(request.body, Request::Stats));
                    response.id = request.id;
                    write_frame(&mut stream, &response, HARD_FRAME_LIMIT).unwrap();
                }
            });
            let socket = TcpStream::connect(endpoint).unwrap();
            let mut io = CountIo {
                inner: DeadlineIo::new(socket, Duration::from_secs(60)).unwrap(),
                reads: 0,
                writes: 0,
            };
            let mut conn = ClientConnection::new(
                tls.client_config().unwrap(),
                ServerName::try_from("localhost").unwrap(),
            )
            .unwrap();
            conn.set_buffer_limit(Some(HARD_FRAME_LIMIT));
            while conn.is_handshaking() {
                conn.complete_io(&mut io).unwrap();
            }
            let mut stream = StreamOwned::new(conn, io);
            for id in 1..=10 {
                write_frame(
                    &mut stream,
                    &reactor::Envelope {
                        protocol: PROTOCOL,
                        id,
                        body: Request::Stats,
                    },
                    HARD_FRAME_LIMIT,
                )
                .unwrap();
                let response =
                    read_frame::<reactor::Envelope<Response>>(&mut stream, HARD_FRAME_LIMIT)
                        .unwrap();
                assert_eq!(response.id, id);
                black_box(response);
            }
            let (writes, reads) = (stream.sock.writes, stream.sock.reads);
            let start = Instant::now();
            let mut samples = Vec::with_capacity(network_iterations);
            for id in 11..11 + network_iterations as u64 {
                let began = Instant::now();
                write_frame(
                    &mut stream,
                    &reactor::Envelope {
                        protocol: PROTOCOL,
                        id,
                        body: Request::Stats,
                    },
                    HARD_FRAME_LIMIT,
                )
                .unwrap();
                let response =
                    read_frame::<reactor::Envelope<Response>>(&mut stream, HARD_FRAME_LIMIT)
                        .unwrap();
                assert_eq!(response.id, id);
                samples.push(began.elapsed().as_nanos());
                assert!(
                    matches!(black_box(response.body),Response::Records(records) if records.len()==count)
                );
            }
            let elapsed = start.elapsed().as_nanos();
            samples.sort_unstable();
            println!(
                "tls_roundtrip,{variant},{round},{bytes},{count},{network_iterations},{elapsed},{},{},{},{}",
                samples[network_iterations / 2],
                samples[network_iterations * 95 / 100],
                (stream.sock.writes - writes) as f64 / network_iterations as f64,
                (stream.sock.reads - reads) as f64 / network_iterations as f64
            );
            drop(stream);
            server.join().unwrap();
        }
    }
}
