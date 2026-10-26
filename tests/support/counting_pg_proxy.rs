//! Test-owned opaque PostgreSQL TLS relay that counts server response bytes.
#![allow(dead_code)]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

pub struct CountingPgProxy {
    port: u16,
    server_bytes: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl CountingPgProxy {
    pub fn start(target_port: u16) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let listener = TcpListener::from_std(listener).unwrap();
        let server_bytes = Arc::new(AtomicU64::new(0));
        let count = server_bytes.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((downstream, _)) = listener.accept().await {
                let count = count.clone();
                connections.spawn(async move {
                    let upstream = TcpStream::connect(("127.0.0.1", target_port))
                        .await
                        .unwrap();
                    let (mut down_read, mut down_write) = downstream.into_split();
                    let (mut up_read, mut up_write) = upstream.into_split();
                    tokio::join!(
                        async {
                            let _ = tokio::io::copy(&mut down_read, &mut up_write).await;
                            let _ = up_write.shutdown().await;
                        },
                        async {
                            let mut buffer = [0u8; 16 * 1024];
                            while let Ok(n @ 1..) = up_read.read(&mut buffer).await {
                                count.fetch_add(n as u64, Ordering::Relaxed);
                                if down_write.write_all(&buffer[..n]).await.is_err() {
                                    break;
                                }
                            }
                            let _ = down_write.shutdown().await;
                        }
                    );
                });
            }
        });
        Self {
            port,
            server_bytes,
            task,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn server_bytes(&self) -> u64 {
        self.server_bytes.load(Ordering::Relaxed)
    }
}

impl Drop for CountingPgProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
