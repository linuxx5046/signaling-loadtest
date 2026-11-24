use clap::Parser;

/// `signaling-loadtest` -- generador de carga real para `signaling-rust`
/// (Componente B, sección 5 de PROYECTO.md). Simula pares Wallet+Extension
/// que abren conexión, negocian SDP/ICE real y se mantienen activos, con
/// desconexiones/reconexiones periódicas -- no abre sockets vacíos.
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Args {
    /// URL base del servidor WebSocket, sin path ni query.
    /// Ejemplo: ws://127.0.0.1:18080  (o el puerto que mapee el túnel SSH
    /// hacia la máquina local durante las pruebas).
    #[arg(long)]
    pub url: String,

    /// Número de PARES wallet+extension a simular. Cada par abre 2
    /// conexiones WS reales (una por rol). Para la etapa de "6000
    /// conexiones" del plan de pruebas, esto es --pairs 3000.
    #[arg(long)]
    pub pairs: u32,

    /// Segundos durante los cuales se reparte la apertura de todos los
    /// pares (evita abrir todo de golpe en el instante 0).
    #[arg(long, default_value_t = 30)]
    pub ramp_up_secs: u64,

    /// Segundos que dura la fase de "mantenerse activo" tras completar el
    /// ramp-up, antes de cerrar todo y mostrar el resumen. 0 = indefinido
    /// (correr hasta Ctrl-C).
    #[arg(long, default_value_t = 120)]
    pub duration_secs: u64,

    /// Cada cuántos segundos (aprox., con jitter) un par envía un mensaje
    /// de mantenimiento (iceCandidate) durante la fase activa, además de
    /// la negociación inicial offer/answer/ICE.
    #[arg(long, default_value_t = 20)]
    pub keepalive_interval_secs: u64,

    /// Probabilidad (0.0..1.0) de que, en cada tick de keepalive, un par
    /// simule una desconexión+reconexión en vez de mandar un mensaje
    /// normal (sección 5 de PROYECTO.md: "Simular desconexiones. Simular
    /// reconexiones.").
    #[arg(long, default_value_t = 0.05)]
    pub reconnect_probability: f64,

    /// Archivo CSV de salida con una fila por mensaje con entrega E2E
    /// confirmada (timestamp_ms,pair_id,method,latency_ms). Insumo para
    /// `signaling-bench` (Componente C). Si se omite, no se escribe nada.
    #[arg(long)]
    pub output_csv: Option<String>,

    /// Imprime una línea de progreso cada N pares abiertos durante el
    /// ramp-up, y cada N segundos durante la fase activa. 0 desactiva el
    /// progreso (deja solo el resumen final).
    #[arg(long, default_value_t = 200)]
    pub progress_every: u32,
}
