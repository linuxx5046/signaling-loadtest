mod config;
mod metrics;
mod pair;
mod protocol;

use clap::Parser;
use config::Args;
use metrics::Metrics;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    let metrics = Arc::new(Metrics::new());

    eprintln!(
        "signaling-loadtest: {} pares ({} conexiones WS) contra {}, ramp-up={}s, duración={}s",
        args.pairs,
        args.pairs * 2,
        args.url,
        args.ramp_up_secs,
        args.duration_secs
    );

    // Escritor de CSV en background -- una tarea dedicada en vez de abrir
    // el archivo desde cada par, para no pelear por el lock del
    // filesystem a miles de escrituras concurrentes.
    let csv_tx = args.output_csv.as_ref().map(|path| {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        spawn_csv_writer(path.clone(), rx);
        tx
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // --- ramp-up: abrir todos los pares repartidos en ramp_up_secs ---
    let mut handles = Vec::with_capacity((args.pairs as usize) * 2);
    let delay_between_pairs = if args.pairs > 0 {
        std::time::Duration::from_secs_f64(args.ramp_up_secs as f64 / args.pairs as f64)
    } else {
        std::time::Duration::ZERO
    };

    for i in 0..args.pairs {
        let (h1, h2) = pair::spawn_pair(
            i,
            &args.url,
            args.clone(),
            metrics.clone(),
            csv_tx.clone(),
            shutdown_rx.clone(),
        );
        handles.push(h1);
        handles.push(h2);

        if args.progress_every > 0 && (i + 1) % args.progress_every == 0 {
            eprintln!("ramp-up: {}/{} pares lanzados", i + 1, args.pairs);
        }

        if !delay_between_pairs.is_zero() {
            tokio::time::sleep(delay_between_pairs).await;
        }
    }
    eprintln!("ramp-up completo: {} pares lanzados", args.pairs);

    // --- fase activa: esperar duration_secs, mostrando progreso, con
    //     salida anticipada limpia por Ctrl-C ---
    let duration = if args.duration_secs == 0 {
        None
    } else {
        Some(std::time::Duration::from_secs(args.duration_secs))
    };
    run_active_phase(duration, &args, &metrics).await;

    eprintln!("cerrando todas las conexiones...");
    let _ = shutdown_tx.send(true);

    // esperar un cierre ordenado, con un tope -- si algo se cuelga no
    // vale la pena bloquear el reporte final por eso.
    let join_all = futures_util::future::join_all(handles);
    if tokio::time::timeout(std::time::Duration::from_secs(15), join_all)
        .await
        .is_err()
    {
        eprintln!("advertencia: no todas las conexiones cerraron limpio dentro de 15s");
    }

    let summary = metrics.snapshot_summary();
    println!("{summary}");
}

async fn run_active_phase(duration: Option<std::time::Duration>, args: &Args, metrics: &Metrics) {
    let progress_interval = std::time::Duration::from_secs(if args.progress_every > 0 { 10 } else { u64::MAX / 2 });
    let start = std::time::Instant::now();

    loop {
        let tick = tokio::time::sleep(progress_interval);
        tokio::select! {
            _ = tick => {
                if args.progress_every > 0 {
                    let s = metrics.snapshot_summary();
                    eprintln!(
                        "[{:>4}s] conexiones={} msgs={} confirmations={} remoteData={} errores_conn={} rechazos_remoto={}",
                        start.elapsed().as_secs(),
                        s.connections_opened,
                        s.messages_sent,
                        s.confirmations_received,
                        s.remote_data_received,
                        s.errors_connect + s.errors_unexpected_close,
                        s.errors_missing_remote_client,
                    );
                }
                if let Some(d) = duration {
                    if start.elapsed() >= d {
                        return;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Ctrl-C recibido, cerrando...");
                return;
            }
        }
    }
}

fn spawn_csv_writer(path: String, mut rx: mpsc::UnboundedReceiver<String>) {
    tokio::spawn(async move {
        let file = match std::fs::File::create(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("no se pudo crear {path}: {e}");
                return;
            }
        };
        let mut writer = std::io::BufWriter::new(file);
        let _ = writeln!(writer, "timestamp_ms,pair_id,method,latency_ms");
        let mut since_flush = 0u32;
        while let Some(line) = rx.recv().await {
            let _ = writeln!(writer, "{line}");
            since_flush += 1;
            if since_flush >= 500 {
                let _ = writer.flush();
                since_flush = 0;
            }
        }
        let _ = writer.flush();
    });
}
