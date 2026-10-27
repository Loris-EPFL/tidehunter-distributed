//! Explicit raw provider FI_READ floor; this is not a Session/RPC or WAL benchmark.
use serde::Serialize;
use std::error::Error;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};
use tidehunter_fabric::{
    Endpoint, FabricBackend, FabricConfig, PeerDrainToken, RemoteReadRegion, RmaAddressMode,
    RmaConfig, RmaReadMode,
};

type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;
const HEADER_BYTES: usize = 24;
const MAX_BOOTSTRAP_BYTES: usize = 4096;
const SESSION_ID: u64 = 0x524d_4150_524f_4245;
const GRANT_GENERATION: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Server,
    Client,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Polling {
    Busy,
    Adaptive,
}

struct Args {
    role: Role,
    listen: Option<String>,
    connect: Option<String>,
    fabric: FabricConfig,
    require_device_read: bool,
    payload_bytes: usize,
    samples: usize,
    warmup: usize,
    deadline: Duration,
    polling: Polling,
    output: PathBuf,
    detailed_steps: bool,
}

impl Args {
    fn parse<I>(tokens: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut args = Self {
            role: Role::Client,
            listen: None,
            connect: None,
            fabric: FabricConfig {
                backend: FabricBackend::SoftwareTcp,
                domain: None,
                source_address: None,
            },
            require_device_read: false,
            payload_bytes: 256,
            samples: 1000,
            warmup: 64,
            deadline: Duration::from_secs(30),
            polling: Polling::Busy,
            output: PathBuf::from("rma-probe.json"),
            detailed_steps: false,
        };
        let mut role_explicit = false;
        let mut fabric_config_explicit = false;
        let mut require_device_read_explicit = false;
        let mut tokens = tokens.into_iter();
        while let Some(flag) = tokens.next() {
            if flag == "--help" {
                return Err(invalid("help requested; use --role server|client --listen ADDR|--connect ADDR --fabric-config JSON_FILE --require-device-read true|false [--bytes N --samples N --warmup N --deadline-ms N --polling busy|adaptive --output FILE --detailed-steps]").into());
            }
            if flag == "--detailed-steps" {
                args.detailed_steps = true;
                continue;
            }
            let value = tokens.next().ok_or_else(|| invalid("missing flag value"))?;
            match flag.as_str() {
                "--role" => {
                    args.role = match value.as_str() {
                        "server" => Role::Server,
                        "client" => Role::Client,
                        _ => return Err(invalid("unknown RMA probe role").into()),
                    };
                    role_explicit = true;
                }
                "--listen" => args.listen = Some(value),
                "--connect" => args.connect = Some(value),
                "--fabric-config" => {
                    args.fabric = serde_json::from_slice(&std::fs::read(value)?)?;
                    fabric_config_explicit = true;
                }
                "--require-device-read" => {
                    args.require_device_read = value.parse()?;
                    require_device_read_explicit = true;
                }
                "--bytes" => args.payload_bytes = value.parse()?,
                "--samples" => args.samples = value.parse()?,
                "--warmup" => args.warmup = value.parse()?,
                "--deadline-ms" => args.deadline = Duration::from_millis(value.parse()?),
                "--polling" => {
                    args.polling = match value.as_str() {
                        "busy" => Polling::Busy,
                        "adaptive" => Polling::Adaptive,
                        _ => return Err(invalid("unknown polling policy").into()),
                    }
                }
                "--output" => args.output = value.into(),
                _ => return Err(invalid("unknown RMA probe option").into()),
            }
        }
        if !role_explicit || !fabric_config_explicit || !require_device_read_explicit {
            return Err(invalid(
                "select an explicit role, --fabric-config and --require-device-read",
            )
            .into());
        }
        args.fabric.validate()?;
        if !(1..=1_048_576).contains(&args.payload_bytes)
            || !(1..=1_000_000).contains(&args.samples)
            || args.warmup > 100_000
            || args.deadline.is_zero()
            || args.deadline > Duration::from_secs(3600)
        {
            return Err(invalid("RMA probe bounds exceeded").into());
        }
        let role_address_ok = match args.role {
            Role::Server => args.listen.is_some() && args.connect.is_none(),
            Role::Client => args.connect.is_some() && args.listen.is_none(),
        };
        let address_valid = match args.role {
            Role::Server => args
                .listen
                .as_deref()
                .and_then(|value| value.parse::<SocketAddr>().ok())
                .is_some(),
            Role::Client => args
                .connect
                .as_deref()
                .and_then(|value| value.parse::<SocketAddr>().ok())
                .is_some(),
        };
        if !role_address_ok || !address_valid {
            return Err(invalid("server requires a numeric --listen socket address; client requires a numeric --connect socket address").into());
        }
        Ok(args)
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| invalid("descriptor offset overflow"))?;
    let field = bytes
        .get(offset..end)
        .ok_or_else(|| invalid("truncated remote read descriptor"))?;
    let field: [u8; 8] = field
        .try_into()
        .map_err(|_| invalid("invalid descriptor field width"))?;
    Ok(u64::from_le_bytes(field))
}

#[derive(Serialize)]
struct Capabilities {
    address_mode: &'static str,
    read_mode: &'static str,
    provider_info: serde_json::Value,
    fabric_config: FabricConfig,
    require_device_read: bool,
    max_read_bytes: usize,
}
#[derive(Serialize)]
struct Sample {
    index: usize,
    phase: &'static str,
    requested_payload_bytes: usize,
    actual_fi_read_bytes: usize,
    post_would_block_retries: usize,
    empty_completion_polls: usize,
    completion_latency_ns: u64,
    api_validation_latency_ns: u64,
    valid: bool,
}
#[derive(Serialize)]
struct Report {
    benchmark: &'static str,
    claim: &'static str,
    role: &'static str,
    topology: &'static str,
    control_security: &'static str,
    provider_info: serde_json::Value,
    fabric_config: FabricConfig,
    fabric: String,
    capabilities: Capabilities,
    payload_bytes: usize,
    accepted_fi_reads: usize,
    samples_requested: usize,
    warmup_requested: usize,
    warmup_completed: usize,
    clean_samples_completed: usize,
    deadline_ms: u128,
    polling: &'static str,
    cpu_affinity: Option<String>,
    registered_region_bytes: usize,
    raw_samples: Vec<Sample>,
    detailed_steps_ns: Option<Vec<(&'static str, u64)>>,
}

fn start_detail(enabled: bool) -> Option<Instant> {
    enabled.then(Instant::now)
}
fn finish_detail(
    steps: &mut Option<Vec<(&'static str, u64)>>,
    name: &'static str,
    started: Option<Instant>,
) {
    if let (Some(steps), Some(started)) = (steps, started) {
        steps.push((name, started.elapsed().as_nanos() as u64));
    }
}

fn open_endpoint(args: &Args) -> io::Result<Endpoint> {
    let config = RmaConfig {
        source_bytes: args.payload_bytes + HEADER_BYTES,
        source_region_slots: 1,
        destination_bytes: args.payload_bytes + HEADER_BYTES,
        destination_slots: 1,
        max_read_bytes: args.payload_bytes + HEADER_BYTES,
        require_device_read: args.require_device_read,
    };
    Endpoint::open(&args.fabric, Some(config))
}

fn write_all_deadline(
    stream: &mut TcpStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn read_exact_deadline(
    stream: &mut TcpStream,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(bytes) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(read) => {
                let (_, rest) = bytes.split_at_mut(read);
                bytes = rest;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn write_frame(stream: &mut TcpStream, bytes: &[u8], deadline: Instant) -> io::Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_BOOTSTRAP_BYTES {
        return Err(invalid("bootstrap frame exceeds bound"));
    }
    write_all_deadline(stream, &(bytes.len() as u32).to_be_bytes(), deadline)?;
    write_all_deadline(stream, bytes, deadline)
}
fn read_frame(stream: &mut TcpStream, deadline: Instant) -> io::Result<Vec<u8>> {
    let mut size = [0; 4];
    read_exact_deadline(stream, &mut size, deadline)?;
    let len = u32::from_be_bytes(size) as usize;
    if len == 0 || len > MAX_BOOTSTRAP_BYTES {
        return Err(invalid("invalid bootstrap frame length"));
    }
    let mut bytes = vec![0; len];
    read_exact_deadline(stream, &mut bytes, deadline)?;
    Ok(bytes)
}
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "RMA probe deadline expired"))
}
fn capabilities(endpoint: &Endpoint, args: &Args) -> io::Result<Capabilities> {
    let caps = endpoint
        .rma_capabilities()
        .ok_or_else(|| io::Error::other("provider did not negotiate FI_READ"))?;
    if args.payload_bytes + HEADER_BYTES > caps.max_read_bytes {
        return Err(io::Error::other(
            "requested read exceeds provider FI_READ cap",
        ));
    }
    Ok(Capabilities {
        address_mode: match caps.address_mode {
            RmaAddressMode::Virtual => "virtual",
            RmaAddressMode::Offset => "offset",
        },
        read_mode: match caps.read_mode {
            RmaReadMode::Native => "native",
            RmaReadMode::Emulated => "emulated",
            RmaReadMode::Unknown => "unknown",
        },
        provider_info: serde_json::to_value(endpoint.provider_info()).map_err(io::Error::other)?,
        fabric_config: args.fabric.clone(),
        require_device_read: args.require_device_read,
        max_read_bytes: caps.max_read_bytes,
    })
}
fn fabric_label(config: &FabricConfig) -> &'static str {
    match &config.backend {
        FabricBackend::Efa => "efa",
        FabricBackend::EfaDirect => "efa-direct",
        FabricBackend::Verbs => "verbs",
        FabricBackend::SoftwareTcp => "software-tcp",
    }
}

fn source_bytes(payload_bytes: usize, region_id: u64, generation: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES + payload_bytes);
    bytes.extend_from_slice(b"RMAP");
    bytes.extend_from_slice(&region_id.to_le_bytes());
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes.extend_from_slice(&(payload_bytes as u32).to_le_bytes());
    bytes.extend((0..payload_bytes).map(|i| (i.wrapping_mul(131).wrapping_add(17) % 251) as u8));
    bytes
}
fn expected_header(region: &RemoteReadRegion, payload_bytes: usize) -> [u8; HEADER_BYTES] {
    let mut header = [0; HEADER_BYTES];
    header[..4].copy_from_slice(b"RMAP");
    header[4..12].copy_from_slice(&region.region_id.to_le_bytes());
    header[12..20].copy_from_slice(&region.generation.to_le_bytes());
    header[20..24].copy_from_slice(&(payload_bytes as u32).to_le_bytes());
    header
}
fn validate(bytes: &[u8], region: &RemoteReadRegion, payload_bytes: usize) -> bool {
    bytes.len() == HEADER_BYTES + payload_bytes
        && bytes[..HEADER_BYTES] == expected_header(region, payload_bytes)
        && bytes[HEADER_BYTES..]
            .iter()
            .enumerate()
            .all(|(i, byte)| *byte == (i.wrapping_mul(131).wrapping_add(17) % 251) as u8)
}
fn pause(policy: Polling, deadline: Instant) -> io::Result<()> {
    remaining(deadline)?;
    if policy == Polling::Adaptive {
        thread::yield_now();
    } else {
        std::hint::spin_loop();
    }
    Ok(())
}
fn cpu_affinity() -> Option<String> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("Cpus_allowed_list:")
                .map(|value| value.trim().to_owned())
        })
}
fn drain_progress(endpoint: &mut Endpoint) -> io::Result<()> {
    match endpoint.try_progress() {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(error),
    }
}

fn server(args: &Args, deadline: Instant) -> Result<()> {
    let mut detailed_steps_ns = args.detailed_steps.then(Vec::new);
    let mut step = start_detail(args.detailed_steps);
    let listen = args
        .listen
        .as_deref()
        .ok_or_else(|| invalid("missing listen address"))?
        .parse::<SocketAddr>()?;
    let listener = TcpListener::bind(listen)?;
    listener.set_nonblocking(true)?;
    let mut endpoint = open_endpoint(args)?;
    let caps = capabilities(&endpoint, args)?;
    let own_addr = endpoint.address()?;
    finish_detail(&mut detailed_steps_ns, "provider_source_setup_ns", step);
    println!("rma_probe_ready listen={listen}");

    step = start_detail(args.detailed_steps);
    let mut stream = loop {
        remaining(deadline)?;
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                pause(args.polling, deadline)?;
            }
            Err(error) => return Err(error.into()),
        }
    };
    stream.set_nodelay(true)?;
    let peer_addr = read_frame(&mut stream, deadline)?;
    endpoint.set_peer(&peer_addr)?;
    write_frame(&mut stream, &own_addr, deadline)?;
    finish_detail(&mut detailed_steps_ns, "control_bootstrap_ns", step);

    step = start_detail(args.detailed_steps);
    let reservation = endpoint.reserve_read_region(SESSION_ID, GRANT_GENERATION)?;
    let payload = source_bytes(
        args.payload_bytes,
        reservation.region_id(),
        reservation.generation(),
    );
    let remote = endpoint.register_read_region(&reservation, payload)?;
    let mut descriptor = Vec::with_capacity(72);
    descriptor.extend_from_slice(&remote.region_id.to_le_bytes());
    descriptor.extend_from_slice(&remote.generation.to_le_bytes());
    descriptor.extend_from_slice(&remote.remote_address.to_le_bytes());
    descriptor.extend_from_slice(&remote.key.to_le_bytes());
    descriptor.extend_from_slice(&remote.len.to_le_bytes());
    descriptor.extend_from_slice(&(args.samples as u64).to_le_bytes());
    descriptor.extend_from_slice(&(args.warmup as u64).to_le_bytes());
    descriptor.extend_from_slice(&(caps.max_read_bytes as u64).to_le_bytes());
    descriptor.push(match caps.read_mode {
        "emulated" => 0,
        "native" => 1,
        "unknown" => 2,
        _ => return Err(invalid("invalid read mode").into()),
    });
    descriptor.push(if caps.address_mode == "virtual" { 1 } else { 0 });
    descriptor.extend_from_slice(&[0; 6]);
    write_frame(&mut stream, &descriptor, deadline)?;
    finish_detail(&mut detailed_steps_ns, "registration_descriptor_ns", step);

    // Keep polling the software provider while waiting for the client's explicit
    // drain acknowledgement. EOF/timeout quarantines by dropping without a token.
    step = start_detail(args.detailed_steps);
    stream.set_nonblocking(true)?;
    let mut ack = [0u8; 32];
    let mut ack_len = 0;
    while ack_len < ack.len() {
        remaining(deadline)?;
        drain_progress(&mut endpoint)?;
        match stream.read(&mut ack[ack_len..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client disconnected before drain acknowledgement",
                )
                .into());
            }
            Ok(read) => ack_len += read,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::Interrupted =>
            {
                pause(args.polling, deadline)?
            }
            Err(error) => return Err(error.into()),
        }
    }
    if ack[..8] != remote.region_id.to_le_bytes()
        || ack[8..16] != remote.generation.to_le_bytes()
        || ack[16..24] != (args.warmup as u64).to_le_bytes()
        || ack[24..32] != (args.samples as u64).to_le_bytes()
    {
        return Err(invalid("drain acknowledgement identity or completion count mismatch").into());
    }
    // SAFETY: only the trusted client sends this exact ack after stopping posts
    // and draining every accepted operation to terminal CQ completion.
    let token = unsafe { PeerDrainToken::after_ack(SESSION_ID, GRANT_GENERATION) };
    endpoint.retire_read_region(reservation, token)?;
    finish_detail(&mut detailed_steps_ns, "drain_ack_retirement_ns", step);
    let report = Report {
        benchmark: "raw_provider_fi_read_floor",
        claim: "provider_FI_READ_completion_and_client_API_validation_not_pure_wire_RTT",
        role: "server",
        topology: "two_process_control_tcp_and_provider_fabric",
        control_security: "bounded_trusted_test_tcp_no_authentication",
        provider_info: caps.provider_info.clone(),
        fabric_config: args.fabric.clone(),
        fabric: fabric_label(&args.fabric).to_owned(),
        capabilities: caps,
        payload_bytes: args.payload_bytes,
        accepted_fi_reads: args.warmup + args.samples,
        samples_requested: args.samples,
        warmup_requested: args.warmup,
        warmup_completed: args.warmup,
        clean_samples_completed: args.samples,
        deadline_ms: args.deadline.as_millis(),
        polling: if args.polling == Polling::Busy {
            "busy_spin"
        } else {
            "adaptive_yield"
        },
        cpu_affinity: cpu_affinity(),
        registered_region_bytes: remote.len as usize,
        raw_samples: Vec::new(),
        detailed_steps_ns,
    };
    write_report(&args.output, &report)?;
    Ok(())
}

fn client(args: &Args, deadline: Instant) -> Result<()> {
    let mut detailed_steps_ns = args.detailed_steps.then(Vec::new);
    let mut step = start_detail(args.detailed_steps);
    let mut endpoint = open_endpoint(args)?;
    let caps = capabilities(&endpoint, args)?;
    let own_addr = endpoint.address()?;
    finish_detail(&mut detailed_steps_ns, "provider_endpoint_setup_ns", step);
    step = start_detail(args.detailed_steps);
    let address = args
        .connect
        .as_deref()
        .ok_or_else(|| invalid("missing connect address"))?
        .parse::<SocketAddr>()?;
    let mut stream = TcpStream::connect_timeout(&address, remaining(deadline)?)?;
    write_frame(&mut stream, &own_addr, deadline)?;
    let peer_addr = read_frame(&mut stream, deadline)?;
    endpoint.set_peer(&peer_addr)?;
    let descriptor = read_frame(&mut stream, deadline)?;
    if descriptor.len() != 72 {
        return Err(invalid("invalid remote read descriptor").into());
    }
    let field = |offset| read_u64(&descriptor, offset);
    let remote = RemoteReadRegion {
        region_id: field(0)?,
        generation: field(8)?,
        remote_address: field(16)?,
        key: field(24)?,
        len: field(32)?,
    };
    let expected_read_mode = match caps.read_mode {
        "emulated" => 0,
        "native" => 1,
        "unknown" => 2,
        _ => return Err(invalid("invalid local read mode").into()),
    };
    if descriptor[64] > 2
        || descriptor[65] > 1
        || descriptor[64] != expected_read_mode
        || (descriptor[65] == 1) != (caps.address_mode == "virtual")
        || field(40)? != args.samples as u64
        || field(48)? != args.warmup as u64
        || field(56)? < remote.len
        || remote.len != (args.payload_bytes + HEADER_BYTES) as u64
    {
        return Err(
            invalid("client/server probe limits, capability, or region size differ").into(),
        );
    }
    finish_detail(
        &mut detailed_steps_ns,
        "control_bootstrap_descriptor_ns",
        step,
    );
    step = start_detail(args.detailed_steps);
    let read_once = |endpoint: &mut Endpoint,
                     phase: &'static str,
                     index: usize,
                     samples: &mut Vec<Sample>|
     -> Result<()> {
        let api_start = Instant::now();
        let mut post_would_block_retries = 0;
        let mut operation = loop {
            remaining(deadline)?;
            match endpoint.try_post_read(&remote, 0, remote.len as usize) {
                Ok(operation) => break operation,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    post_would_block_retries += 1;
                    drain_progress(endpoint)?;
                    pause(args.polling, deadline)?;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let post_time = Instant::now();
        let mut empty_completion_polls = 0;
        loop {
            remaining(deadline)?;
            match endpoint.try_read_completion(&mut operation)? {
                Some(Ok(result)) => {
                    let completion_time = Instant::now();
                    let valid = validate(result.as_ref(), &remote, args.payload_bytes);
                    let done = Instant::now();
                    if !valid {
                        drop(result);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "FI_READ payload failed deterministic validation",
                        )
                        .into());
                    }
                    samples.push(Sample {
                        index,
                        phase,
                        requested_payload_bytes: args.payload_bytes,
                        actual_fi_read_bytes: remote.len as usize,
                        post_would_block_retries,
                        empty_completion_polls,
                        completion_latency_ns: completion_time.duration_since(post_time).as_nanos()
                            as u64,
                        api_validation_latency_ns: done.duration_since(api_start).as_nanos() as u64,
                        valid,
                    });
                    drop(result); // destination slot is reusable only after result release
                    return Ok(());
                }
                Some(Err(error)) => return Err(error.into()),
                None => {
                    empty_completion_polls += 1;
                    drain_progress(endpoint)?;
                    pause(args.polling, deadline)?;
                }
            }
        }
    };
    let mut samples = Vec::with_capacity(args.samples + args.warmup);
    for index in 0..args.warmup {
        read_once(&mut endpoint, "warmup", index, &mut samples)?;
    }
    for index in 0..args.samples {
        read_once(&mut endpoint, "clean", index, &mut samples)?;
    }
    finish_detail(&mut detailed_steps_ns, "warmup_and_clean_reads_ns", step);
    step = start_detail(args.detailed_steps);
    // No further posts are possible; every accepted FI_READ has terminal CQ and its
    // returned buffer was dropped. Only now release the remote source grant.
    let mut acknowledgement = Vec::with_capacity(32);
    acknowledgement.extend_from_slice(&remote.region_id.to_le_bytes());
    acknowledgement.extend_from_slice(&remote.generation.to_le_bytes());
    acknowledgement.extend_from_slice(&(args.warmup as u64).to_le_bytes());
    acknowledgement.extend_from_slice(&(args.samples as u64).to_le_bytes());
    write_all_deadline(&mut stream, &acknowledgement, deadline)?;
    finish_detail(&mut detailed_steps_ns, "drain_ack_write_ns", step);
    let report = Report {
        benchmark: "raw_provider_fi_read_floor",
        claim: "provider_FI_READ_completion_and_client_API_validation_not_pure_wire_RTT",
        role: "client",
        topology: "two_process_control_tcp_and_provider_fabric",
        control_security: "bounded_trusted_test_tcp_no_authentication",
        provider_info: caps.provider_info.clone(),
        fabric_config: args.fabric.clone(),
        fabric: fabric_label(&args.fabric).to_owned(),
        capabilities: caps,
        payload_bytes: args.payload_bytes,
        accepted_fi_reads: args.warmup + args.samples,
        samples_requested: args.samples,
        warmup_requested: args.warmup,
        warmup_completed: args.warmup,
        clean_samples_completed: args.samples,
        deadline_ms: args.deadline.as_millis(),
        polling: if args.polling == Polling::Busy {
            "busy_spin"
        } else {
            "adaptive_yield"
        },
        cpu_affinity: cpu_affinity(),
        registered_region_bytes: remote.len as usize,
        raw_samples: samples,
        detailed_steps_ns,
    };
    write_report(&args.output, &report)?;
    Ok(())
}
fn write_report(path: &PathBuf, report: &Report) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let output = serde_json::to_vec_pretty(report)?;
    std::fs::write(path, output)?;
    Ok(())
}
pub fn run<I>(tokens: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let tokens: Vec<String> = tokens.into_iter().collect();
    if tokens.len() == 1 && tokens[0] == "--help" {
        println!(
            "--rma-probe --role server --listen ADDR | --rma-probe --role client --connect ADDR"
        );
        println!(
            "Required: --fabric-config JSON_FILE --require-device-read true|false. FabricConfig selects efa, efa-direct, verbs or software-tcp explicitly."
        );
        println!(
            "Optional: --bytes 1..1048576 --samples 1..1000000 --warmup 0..100000 --deadline-ms 1..3600000 --polling busy|adaptive --output FILE --detailed-steps."
        );
        println!(
            "Trusted-test TCP carries bootstrap and drain acknowledgement; no TLS or peer authentication. Device-read policy rejects unverified hardware; software reads are explicitly emulated."
        );
        return Ok(());
    }
    let args = Args::parse(tokens)?;
    let started = Instant::now();
    let deadline = started
        .checked_add(args.deadline)
        .ok_or_else(|| invalid("deadline overflow"))?;
    match args.role {
        Role::Server => server(&args, deadline),
        Role::Client => client(&args, deadline),
    }
}

#[cfg(test)]
mod tests {
    use super::{Args, FabricBackend, Role};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_CONFIG: AtomicU64 = AtomicU64::new(0);

    fn parse(tokens: &[&str]) -> super::Result<Args> {
        Args::parse(tokens.iter().map(|token| (*token).to_owned()))
    }

    fn with_config(backend: &str, require_device_read: &str) -> super::Result<Args> {
        let path = std::env::temp_dir().join(format!(
            "tidehunter-rma-probe-{}-{}.json",
            std::process::id(),
            NEXT_CONFIG.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(
            &path,
            format!(r#"{{"backend":"{backend}","domain":"lo","source_address":null}}"#),
        )?;
        let path_string = path.to_string_lossy().into_owned();
        let result = Args::parse(
            [
                "--role",
                "client",
                "--connect",
                "127.0.0.1:9",
                "--fabric-config",
                path_string.as_str(),
                "--require-device-read",
                require_device_read,
            ]
            .into_iter()
            .map(str::to_owned),
        );
        std::fs::remove_file(path)?;
        result
    }

    #[test]
    fn parser_requires_explicit_config_policy_and_role_address() {
        assert!(parse(&["--role", "client", "--connect", "127.0.0.1:9"]).is_err());
        assert!(with_config("efa-direct", "false").is_ok_and(|args| {
            args.role == Role::Client && args.fabric.backend == FabricBackend::EfaDirect
        }));
        assert!(with_config("verbs", "false").is_ok_and(|args| {
            args.fabric.backend == FabricBackend::Verbs && !args.require_device_read
        }));
        assert!(
            with_config("software-tcp", "false")
                .is_ok_and(|args| { args.fabric.backend == FabricBackend::SoftwareTcp })
        );
        assert!(with_config("efa", "true").is_ok_and(|args| {
            args.fabric.backend == FabricBackend::Efa && args.require_device_read
        }));
    }

    #[test]
    fn parser_rejects_legacy_provider_selectors() {
        assert!(parse(&["--role", "client", "--provider", "efa", "--efa-direct"]).is_err());
        assert!(with_config("tcp", "false").is_err());
    }
}
