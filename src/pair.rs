//! Simulación de un par Wallet+Extension: abre las dos conexiones reales,
//! hace una negociación SDP/ICE real al conectar, se mantiene activo con
//! mensajes periódicos, y simula desconexión/reconexión según
//! `--reconnect-probability`. Nunca abre sockets vacíos (requisito de
//! PROYECTO.md sección 5, Componente B).

use crate::config::Args;
use crate::metrics::Metrics;
use crate::protocol::{info_of, make_message};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// `requestId` -> instante de envío. Compartido entre las dos tareas del
/// mismo par: quien manda inserta, quien recibe el `remoteData`
/// correspondiente (en la OTRA conexión) calcula la latencia E2E real y
/// remueve la entrada.
type PendingMap = Arc<Mutex<HashMap<String, Instant>>>;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>;

/// Cuenta cuántos errores de connect ya se imprimieron a stderr, para no
/// inundar la terminal con miles de líneas si la tabla de conexiones del
/// lado de red (conntrack, NAT del router/ISP, etc.) se satura -- con las
/// primeras ~30 muestras (mezclando "rechazo activo" vs "timeout") ya
/// alcanza para diagnosticar el patrón.
static CONNECT_ERRORS_LOGGED: AtomicU64 = AtomicU64::new(0);

fn log_connect_error(pair_index: u32, own_role: &str, msg: &str) {
    if CONNECT_ERRORS_LOGGED.fetch_add(1, Ordering::Relaxed) < 30 {
        eprintln!("[connect-error] par={pair_index} rol={own_role}: {msg}");
    }
}

pub fn random_connection_id() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn jitter_duration(base_secs: u64) -> Duration {
    if base_secs == 0 {
        return Duration::from_millis(50);
    }
    let mut rng = rand::thread_rng();
    // +/- 30% de jitter para no sincronizar a todos los pares entre sí.
    let factor = rng.gen_range(0.7..1.3);
    Duration::from_secs_f64(base_secs as f64 * factor)
}

/// Lanza las dos tareas (wallet + extension) de un par. No bloquea: el
/// llamador sigue abriendo el resto de los pares mientras estas corren.
/// Devuelve los dos `JoinHandle` para poder esperar un cierre ordenado al
/// final de la prueba.
pub fn spawn_pair(
    pair_index: u32,
    base_url: &str,
    args: Arc<Args>,
    metrics: Arc<Metrics>,
    csv_tx: Option<mpsc::UnboundedSender<String>>,
    shutdown_rx: watch::Receiver<bool>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let connection_id = random_connection_id();
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

    let h1 = tokio::spawn(run_side(
        pair_index,
        "wallet",
        connection_id.clone(),
        base_url.to_string(),
        args.clone(),
        metrics.clone(),
        csv_tx.clone(),
        shutdown_rx.clone(),
        pending.clone(),
    ));
    let h2 = tokio::spawn(run_side(
        pair_index,
        "extension",
        connection_id,
        base_url.to_string(),
        args,
        metrics,
        csv_tx,
        shutdown_rx,
        pending,
    ));
    (h1, h2)
}

async fn run_side(
    pair_index: u32,
    own_role: &'static str,
    connection_id: String,
    base_url: String,
    args: Arc<Args>,
    metrics: Arc<Metrics>,
    csv_tx: Option<mpsc::UnboundedSender<String>>,
    mut shutdown_rx: watch::Receiver<bool>,
    pending: PendingMap,
) {
    let other_role = if own_role == "wallet" { "extension" } else { "wallet" };

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        let url = format!("{base_url}/{connection_id}?target={other_role}&source={own_role}");
        let ws_stream = match tokio::time::timeout(Duration::from_secs(8), connect_async(&url)).await {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                // rechazo activo (RST, 4xx del handshake, DNS, etc.) --
                // respondió algo, aunque sea un error.
                log_connect_error(pair_index, own_role, &format!("rechazado: {e}"));
                metrics.errors_connect.fetch_add(1, Ordering::Relaxed);
                metrics.connections_failed_to_open.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(jitter_duration(1)).await;
                continue;
            }
            Err(_) => {
                // nadie respondió en 8s -- típico de un paquete SYN
                // descartado en silencio (tabla NAT/conntrack llena sin
                // enviar RST), distinto de un rechazo activo.
                log_connect_error(pair_index, own_role, "timeout (8s) -- sin respuesta, probable descarte silencioso");
                metrics.errors_connect.fetch_add(1, Ordering::Relaxed);
                metrics.connections_failed_to_open.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(jitter_duration(1)).await;
                continue;
            }
        };
        metrics.connections_opened.fetch_add(1, Ordering::Relaxed);

        let (mut sink, mut stream) = ws_stream.split();
        let mut known_remote_id: Option<String> = None;

        'connection: loop {
            let keepalive_sleep = tokio::time::sleep(jitter_duration(args.keepalive_interval_secs));
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        let _ = sink.send(WsMessage::Close(None)).await;
                        return;
                    }
                }
                incoming = stream.next() => {
                    match incoming {
                        Some(Ok(WsMessage::Text(text))) => {
                            handle_incoming(
                                &text,
                                &mut sink,
                                &mut known_remote_id,
                                pair_index,
                                own_role,
                                &metrics,
                                &pending,
                                &csv_tx,
                            )
                            .await;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => {
                            break 'connection;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) => {
                            metrics.errors_unexpected_close.fetch_add(1, Ordering::Relaxed);
                            break 'connection;
                        }
                    }
                }
                _ = keepalive_sleep => {
                    let Some(remote_id) = known_remote_id.clone() else {
                        continue;
                    };

                    if rand::random::<f64>() < args.reconnect_probability {
                        metrics.reconnects_performed.fetch_add(1, Ordering::Relaxed);
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break 'connection;
                    }

                    send_one_message(&mut sink, pair_index, own_role, &remote_id, "iceCandidate", &metrics, &pending).await;
                }
            }
        }

        if *shutdown_rx.borrow() {
            return;
        }
        tokio::time::sleep(jitter_duration(1)).await;
    }
}

/// Procesa un mensaje entrante. Si es una notificación de apertura
/// (`remoteClientJustConnected` / `remoteClientIsAlreadyConnected`),
/// además de guardar el id remoto dispara inmediatamente la negociación
/// inicial real (offer si fuimos nosotros los que ya estábamos --
/// `remoteClientJustConnected` nos llega vía Redis cuando el otro se
/// conecta después; answer si nosotros llegamos después y el otro ya
/// estaba -- `remoteClientIsAlreadyConnected`), seguida de 3 iceCandidate,
/// igual que una negociación SDP real -- requisito de PROYECTO.md
/// sección 5: "no debe limitarse a abrir conexiones WebSocket vacías".
async fn handle_incoming(
    text: &str,
    sink: &mut Sink,
    known_remote_id: &mut Option<String>,
    pair_index: u32,
    own_role: &'static str,
    metrics: &Arc<Metrics>,
    pending: &PendingMap,
    csv_tx: &Option<mpsc::UnboundedSender<String>>,
) {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return, // no debería pasar nunca contra nuestro propio server
    };
    let info = info_of(&value).unwrap_or("");

    match info {
        "remoteClientJustConnected" => {
            if let Some(id) = value.get("remoteClientId").and_then(|v| v.as_str()) {
                *known_remote_id = Some(id.to_string());
                send_negotiation_round(sink, pair_index, own_role, id, "offer", metrics, pending).await;
            }
        }
        "remoteClientIsAlreadyConnected" => {
            if let Some(id) = value.get("remoteClientId").and_then(|v| v.as_str()) {
                *known_remote_id = Some(id.to_string());
                send_negotiation_round(sink, pair_index, own_role, id, "answer", metrics, pending).await;
            }
        }
        "remoteClientDisconnected" => {
            *known_remote_id = None;
        }
        "confirmation" => {
            metrics.confirmations_received.fetch_add(1, Ordering::Relaxed);
        }
        "remoteData" => {
            metrics.remote_data_received.fetch_add(1, Ordering::Relaxed);
            if let Some(request_id) = value.get("requestId").and_then(|v| v.as_str()) {
                let sent_at = pending.lock().unwrap().remove(request_id);
                if let Some(sent_at) = sent_at {
                    let latency_ms = sent_at.elapsed().as_secs_f64() * 1000.0;
                    metrics.record_latency(latency_ms);
                    if let Some(tx) = csv_tx {
                        let method = value
                            .get("data")
                            .and_then(|d| d.get("method"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("unknown");
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        let _ = tx.send(format!("{now_ms},{pair_index},{method},{latency_ms:.3}"));
                    }
                }
            }
        }
        "missingRemoteClientError" => {
            metrics.errors_missing_remote_client.fetch_add(1, Ordering::Relaxed);
        }
        "validationError" => {
            metrics.errors_validation.fetch_add(1, Ordering::Relaxed);
        }
        "invalidMessageError" => {
            metrics.errors_invalid_message.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

async fn send_negotiation_round(
    sink: &mut Sink,
    pair_index: u32,
    own_role: &'static str,
    remote_id: &str,
    first_method: &str,
    metrics: &Arc<Metrics>,
    pending: &PendingMap,
) {
    send_one_message(sink, pair_index, own_role, remote_id, first_method, metrics, pending).await;
    for _ in 0..3 {
        send_one_message(sink, pair_index, own_role, remote_id, "iceCandidate", metrics, pending).await;
    }
}

async fn send_one_message(
    sink: &mut Sink,
    pair_index: u32,
    own_role: &'static str,
    target_client_id: &str,
    method: &str,
    metrics: &Arc<Metrics>,
    pending: &PendingMap,
) {
    let _ = pair_index;
    let _ = own_role;
    let request_id = uuid::Uuid::new_v4().to_string();
    let msg = make_message(&request_id, method, target_client_id);

    pending.lock().unwrap().insert(request_id.clone(), Instant::now());
    if sink.send(WsMessage::Text(msg.to_string())).await.is_ok() {
        metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
    } else {
        // el envío falló (conexión ya caída) -- no vale la pena medir
        // una latencia que nunca va a resolverse, sacamos la entrada.
        pending.lock().unwrap().remove(&request_id);
    }
}
