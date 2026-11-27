use discordia_bot::protocol::Id;
use std::{
    fs::File,
    io,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub struct TestSfu {
    pub url: String,
    pub key: String,
    pub secret: String,
    child: Option<Child>,
    directory: Option<tempfile::TempDir>,
}

impl TestSfu {
    pub fn configured() -> Self {
        if let Ok(url) = std::env::var("MUSIC_TEST_SFU_URL") {
            assert_eq!(url::Url::parse(&url).unwrap().host_str(), Some("127.0.0.1"));
            return Self {
                url,
                key: std::env::var("MUSIC_TEST_API_KEY").unwrap(),
                secret: std::env::var("MUSIC_TEST_API_SECRET").unwrap(),
                child: None,
                directory: None,
            };
        }
        let binary = PathBuf::from(
            std::env::var_os("MUSIC_TEST_SFU_BINARY")
                .expect("explicit test SFU binary or URL required"),
        )
        .canonicalize()
        .unwrap();
        assert!(binary.is_file());
        let directory = tempfile::tempdir().unwrap();
        let signal = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = signal.local_addr().unwrap().port();
        let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp_port = tcp.local_addr().unwrap().port();
        let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        let udp_port = udp.local_addr().unwrap().port();
        let key = "music-e2e".to_string();
        let secret = format!("{}{}", Id::new_v4().simple(), Id::new_v4().simple());
        let config = directory.path().join("sfu.yaml");
        std::fs::write(&config,format!("port: {port}\nbind_addresses:\n  - '127.0.0.1'\nrtc:\n  tcp_port: {tcp_port}\n  udp_port: {udp_port}\n  node_ip: '127.0.0.1'\n  use_external_ip: false\nkeys:\n  {key}: '{secret}'\nlogging:\n  level: warn\n")).unwrap();
        drop((signal, tcp, udp));
        let mut command = Command::new(binary);
        hidden(&mut command);
        command
            .args(["--config"])
            .arg(&config)
            .stdout(Stdio::from(
                File::create(directory.path().join("sfu.stdout.log")).unwrap(),
            ))
            .stderr(Stdio::from(
                File::create(directory.path().join("sfu.stderr.log")).unwrap(),
            ));
        let child = command.spawn().expect("spawn isolated test SFU");
        let mut server = Self {
            url: format!("ws://127.0.0.1:{port}"),
            key,
            secret,
            child: Some(child),
            directory: Some(directory),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                server.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "test SFU exited during startup"
            );
            if std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "isolated test SFU did not open its signal port"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        server
    }

    pub fn close(&mut self) -> io::Result<()> {
        if let Some(child) = self.child.as_mut() {
            if child.try_wait()?.is_none() {
                let pid = child.id().to_string();
                #[cfg(windows)]
                let mut graceful = {
                    let mut command = Command::new("taskkill");
                    command.args(["/PID", &pid]);
                    command
                };
                #[cfg(not(windows))]
                let mut graceful = {
                    let mut command = Command::new("kill");
                    command.args(["-TERM", &pid]);
                    command
                };
                hidden(&mut graceful);
                match graceful
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                {
                    Ok(status) if status.success() => {
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while child.try_wait()?.is_none() && Instant::now() < deadline {
                            std::thread::sleep(Duration::from_millis(25));
                        }
                    }
                    _ => {}
                }
                if child.try_wait()?.is_none() {
                    child.kill()?;
                }
            }
            child.wait()?;
        }
        self.child = None;
        if let Some(directory) = self.directory.take() {
            directory.close()?;
        }
        Ok(())
    }
}

impl Drop for TestSfu {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("temporary test SFU cleanup failed: {error}");
        }
    }
}

fn hidden(command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    #[cfg(not(windows))]
    let _ = command;
}
