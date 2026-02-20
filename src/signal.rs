/// Waits for shutdown signals and returns on first receipt.
///
/// Pin this future once before a loop and poll `&mut shutdown` inside `tokio::select!`
/// rather than calling this function on every iteration — re-calling recreates the signal
/// registration and can miss signals delivered between iterations.
pub(crate) async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use std::future::pending;
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).ok();
        let mut sigquit = signal(SignalKind::quit()).ok();
        let mut sigtstp = signal(SignalKind::from_raw(libc::SIGTSTP)).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                if let Some(s) = &mut sigterm { let _ = s.recv().await; }
                else { pending::<()>().await; }
            } => {}
            _ = async {
                if let Some(s) = &mut sigquit { let _ = s.recv().await; }
                else { pending::<()>().await; }
            } => {}
            _ = async {
                if let Some(s) = &mut sigtstp { let _ = s.recv().await; }
                else { pending::<()>().await; }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
