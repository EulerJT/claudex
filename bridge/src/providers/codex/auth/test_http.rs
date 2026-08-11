use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub(crate) struct MockServer {
    pub(crate) url: String,
    shutdown: Arc<AtomicBool>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

pub(crate) fn spawn_mock_server<F>(ready_message: &'static str, handler: F) -> MockServer
where
    F: Fn(&str) -> String + Send + Sync + 'static,
{
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handler = Arc::new(handler);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    listener.set_nonblocking(true).expect("set nonblocking");
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let _ = ready_tx.send(());
        loop {
            if sd.load(Ordering::Relaxed) {
                return;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let Some(request) = read_http_request(&mut stream) else {
                        continue;
                    };
                    let response = handler(&request);
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });
    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect(ready_message);

    MockServer { url, shutdown }
}

pub(crate) fn json_response(status: u16, body: &str) -> String {
    response(status, "application/json", body)
}

pub(crate) fn response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    )
}

pub(crate) fn send_get(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    let _ = stream.write_all(request.as_bytes());
    let _ = stream.flush();

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).to_string()
}

pub(crate) fn read_http_request(stream: &mut TcpStream) -> Option<String> {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut request = Vec::new();
    let mut chunk = [0; 4096];

    loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..n]);

        if let Some(header_end) = find_header_end(&request) {
            let content_length = content_length(&request[..header_end]).unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }

    if request.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&request).to_string())
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(headers);
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse().ok()
        } else {
            None
        }
    })
}
