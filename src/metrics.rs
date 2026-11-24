use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Contadores globales del proceso. Atómicos porque cada par corre en sus
/// propias tareas tokio, todas incrementando en paralelo.
#[derive(Default)]
pub struct Metrics {
    /// Conteo a nivel de conexión WS individual (cada lado de cada par
    /// abre su propia conexión real) -- más directo de comparar contra
    /// la columna "Conexiones" del plan de pruebas de PROYECTO.md
    /// (500/1000/2000/4000/6000) que un conteo a nivel de par.
    pub connections_opened: AtomicU64,
    pub connections_failed_to_open: AtomicU64,
    pub messages_sent: AtomicU64,
    pub confirmations_received: AtomicU64,
    /// Entregas end-to-end reales confirmadas (el otro lado recibió
    /// `remoteData`) -- distinto de `confirmations_received`, que solo
    /// certifica que el PUBLISH a Redis no lanzó excepción (sección 2.3
    /// de la especificación de compatibilidad: la confirmation no
    /// garantiza que el destinatario lo recibió).
    pub remote_data_received: AtomicU64,
    pub reconnects_performed: AtomicU64,
    pub errors_validation: AtomicU64,
    pub errors_invalid_message: AtomicU64,
    pub errors_missing_remote_client: AtomicU64,
    pub errors_connect: AtomicU64,
    pub errors_unexpected_close: AtomicU64,
    /// Muestras de latencia E2E real en milisegundos (desde que se envió
    /// un mensaje hasta que la OTRA conexión del par recibió su
    /// `remoteData` correspondiente). Protegidas por Mutex porque se
    /// escriben con poca frecuencia comparado con los contadores de
    /// arriba -- no justifica una estructura lock-free.
    pub latencies_ms: Mutex<Vec<f64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_latency(&self, ms: f64) {
        self.latencies_ms.lock().unwrap().push(ms);
    }

    pub fn snapshot_summary(&self) -> Summary {
        let mut latencies = self.latencies_ms.lock().unwrap().clone();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let percentile = |p: f64| -> f64 {
            if latencies.is_empty() {
                return 0.0;
            }
            let idx = ((p / 100.0) * (latencies.len() as f64 - 1.0)).round() as usize;
            latencies[idx.min(latencies.len() - 1)]
        };

        Summary {
            connections_opened: self.connections_opened.load(Ordering::Relaxed),
            connections_failed_to_open: self.connections_failed_to_open.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            confirmations_received: self.confirmations_received.load(Ordering::Relaxed),
            remote_data_received: self.remote_data_received.load(Ordering::Relaxed),
            reconnects_performed: self.reconnects_performed.load(Ordering::Relaxed),
            errors_validation: self.errors_validation.load(Ordering::Relaxed),
            errors_invalid_message: self.errors_invalid_message.load(Ordering::Relaxed),
            errors_missing_remote_client: self.errors_missing_remote_client.load(Ordering::Relaxed),
            errors_connect: self.errors_connect.load(Ordering::Relaxed),
            errors_unexpected_close: self.errors_unexpected_close.load(Ordering::Relaxed),
            latency_count: latencies.len(),
            latency_min_ms: latencies.first().copied().unwrap_or(0.0),
            latency_max_ms: latencies.last().copied().unwrap_or(0.0),
            latency_avg_ms: if latencies.is_empty() {
                0.0
            } else {
                latencies.iter().sum::<f64>() / latencies.len() as f64
            },
            latency_p50_ms: percentile(50.0),
            latency_p95_ms: percentile(95.0),
            latency_p99_ms: percentile(99.0),
        }
    }
}

#[derive(Debug)]
pub struct Summary {
    pub connections_opened: u64,
    pub connections_failed_to_open: u64,
    pub messages_sent: u64,
    pub confirmations_received: u64,
    pub remote_data_received: u64,
    pub reconnects_performed: u64,
    pub errors_validation: u64,
    pub errors_invalid_message: u64,
    pub errors_missing_remote_client: u64,
    pub errors_connect: u64,
    pub errors_unexpected_close: u64,
    pub latency_count: usize,
    pub latency_min_ms: f64,
    pub latency_max_ms: f64,
    pub latency_avg_ms: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== resumen signaling-loadtest ===")?;
        writeln!(
            f,
            "conexiones WS abiertas:  {}  (fallidas al abrir: {})",
            self.connections_opened, self.connections_failed_to_open
        )?;
        writeln!(
            f,
            "pares (wallet+extension): ~{}",
            self.connections_opened / 2
        )?;
        writeln!(f, "mensajes enviados:       {}", self.messages_sent)?;
        writeln!(f, "confirmations recibidas: {}", self.confirmations_received)?;
        writeln!(
            f,
            "remoteData recibidos:    {}  (entregas E2E reales confirmadas)",
            self.remote_data_received
        )?;
        writeln!(f, "reconexiones simuladas:  {}", self.reconnects_performed)?;
        writeln!(f, "--- errores ---")?;
        writeln!(f, "validationError:          {}", self.errors_validation)?;
        writeln!(f, "invalidMessageError:      {}", self.errors_invalid_message)?;
        writeln!(f, "missingRemoteClientError: {}", self.errors_missing_remote_client)?;
        writeln!(f, "fallos de connect:        {}", self.errors_connect)?;
        writeln!(f, "cierres inesperados:      {}", self.errors_unexpected_close)?;
        writeln!(f, "--- latencia E2E (ms), n={} ---", self.latency_count)?;
        writeln!(
            f,
            "min={:.1}  avg={:.1}  p50={:.1}  p95={:.1}  p99={:.1}  max={:.1}",
            self.latency_min_ms,
            self.latency_avg_ms,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms,
            self.latency_max_ms,
        )
    }
}
